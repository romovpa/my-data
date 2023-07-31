import sys
import mailbox
import warnings
from pathlib import Path
import hashlib
import traceback

import rdflib
from bs4 import BeautifulSoup
from rdflib import Graph, Literal, RDF, URIRef, BNode
from rdflib.namespace import FOAF, XSD, Namespace, RDF
from tqdm import tqdm

from mydata.parsers.mbox.email_data import Message

warnings.filterwarnings("ignore", category=UserWarning, module='bs4')

SCHEMA = Namespace('https://schema.org/')
MYDATA = Namespace('mydata://')


def get_message_ref(message_id):
    message_id_hash = hashlib.sha1(message_id.strip().encode()).hexdigest()
    return MYDATA[f'mbox/message/{message_id_hash}']


def get_thread_ref(thread_id):
    thread_id_hash = hashlib.sha1(thread_id.strip().encode()).hexdigest()
    return MYDATA[f'mbox/thread/{thread_id_hash}']


def message_to_rdf(mbox_msg):
    msg = Message(mbox_msg)

    # Extracting links and text from the message contents
    text = None
    links = []
    if msg.content_plain is not None:
        text = msg.content_plain
    if msg.content_html is not None:
        soup = BeautifulSoup(msg.content_html, 'html.parser')
        if text is None:
            text = soup.get_text()
        links = [
            {
                'url': link.attrs['href'],
                'text': link.text,
            }
            for link in soup.find_all('a')
            if 'href' in link
        ]

    if msg.message_id is None:
        return []

    message_ref = get_message_ref(msg.message_id)

    triples = [
        (message_ref, RDF.type, SCHEMA.EmailMessage),
        (message_ref, SCHEMA.messageId, Literal(msg.message_id)),
        (message_ref, SCHEMA.subject, Literal(msg.subject)),
        (message_ref, SCHEMA.text, Literal(text)),
    ]

    if msg.datetime is not None:
        triples.append((message_ref, SCHEMA.dateSent, Literal(msg.datetime, datatype=XSD.dateTime)))

    if msg.in_reply_to is not None:
        triples.append((message_ref, SCHEMA.inReplyTo, get_message_ref(msg.in_reply_to)))
    if msg.thread_id is not None:
        triples.append((message_ref, SCHEMA.thread, get_thread_ref(msg.thread_id)))

    # Addresses
    if msg.addr_from is not None:
        triples.append((message_ref, SCHEMA.sender, URIRef(f'mailto:{msg.addr_from.normalized}')))
        triples.append((message_ref, SCHEMA.senderEmail, Literal(msg.addr_from.email)))
        if msg.addr_from.name:
            triples.append((message_ref, SCHEMA.senderName, Literal(msg.addr_from.name)))

    if msg.addrs_to is not None:
        for addr in msg.addrs_to:
            triples.append((message_ref, SCHEMA.recipient, URIRef(f'mailto:{addr.normalized}')))
    if msg.addrs_cc is not None:
        for addr in msg.addrs_cc:
            triples.append((message_ref, SCHEMA.ccRecipient, URIRef(f'mailto:{addr.normalized}')))
    if msg.addrs_bcc is not None:
        for addr in msg.addrs_bcc:
            triples.append((message_ref, SCHEMA.bccRecipient, URIRef(f'mailto:{addr.normalized}')))
    if msg.addr_reply_to is not None:
        triples.append((message_ref, SCHEMA.replyTo, URIRef(f'mailto:{msg.addr_reply_to.normalized}')))

    # Attachments
    for attachment in msg.attachments:
        attachment_id = hashlib.sha1(
            (hashlib.sha1(attachment.get_content()).hexdigest() + '-' + attachment.get_filename()).encode()
        ).hexdigest()
        attachment_ref = message_ref + f'/attachment/{attachment_id}'

        triples.append((message_ref, SCHEMA.attachment, attachment_ref))
        triples.append((attachment_ref, RDF.type, SCHEMA.DataDownload))
        triples.append((attachment_ref, SCHEMA.name, Literal(attachment.get_filename())))
        triples.append((attachment_ref, SCHEMA.contentSize, Literal(len(attachment.get_content()), datatype=XSD.nonNegativeInteger)))
        triples.append((attachment_ref, SCHEMA.encodingFormat, Literal(attachment.get_content_type())))

    # Additional info
    if msg['List-Id']:
        triples.append((message_ref, SCHEMA.listId, Literal(msg['List-Id'])))

    return triples


def parse_mbox(exports_dir='exports', cache_dir='cache'):
    graph = Graph()
    graph.bind('rdf', RDF)
    graph.bind('schema', SCHEMA)
    graph.bind('xsd', XSD)

    mbox_files = list(Path(exports_dir).glob('**/*.mbox'))
    for mbox_file_idx, mbox_file in enumerate(mbox_files):
        mbox = mailbox.mbox(mbox_file)

        mbox_size = len(mbox)

        desc = f'[{mbox_file_idx + 1}/{len(mbox_files)}] {mbox_file.relative_to(exports_dir)}'
        for msg_index, mbox_msg in tqdm(enumerate(mbox), total=mbox_size, desc=desc):
            try:
                msg_triples = message_to_rdf(mbox_msg)
                for triple in msg_triples:
                    graph.add(triple)
            except KeyboardInterrupt:
                #sys.exit()
                print('Serializing graph...')
                graph.serialize(Path(cache_dir) / 'mbox.ttl', format='turtle')
            except Exception as e:
                print(f'Error parsing message {mbox_msg["message_id"]}: {e}')
                # Print full stack trace
                traceback.print_exc()
                continue

    graph.serialize(Path(cache_dir) / 'mbox.ttl', format='turtle')


if __name__ == '__main__':
    parse_mbox()
