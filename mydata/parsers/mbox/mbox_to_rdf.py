import hashlib
import mailbox
import traceback
import warnings
from pathlib import Path

from bs4 import BeautifulSoup
from rdflib import Graph, Literal, URIRef
from rdflib.namespace import RDF, XSD, Namespace
from rdflib.term import _is_valid_uri
from tqdm import tqdm

from mydata.parsers.mbox.email_data import Message

warnings.filterwarnings("ignore", category=UserWarning, module="bs4")

MBOX_TYPE = Namespace("https://ownld.org/service/mbox/")
MBOX_DATA = Namespace("mydata://db/service/mbox/")


def get_message_ref(message_id):
    message_id_hash = hashlib.sha1(message_id.strip().encode()).hexdigest()
    return MBOX_DATA[f"message/{message_id_hash}"]


def get_thread_ref(thread_id):
    thread_id_hash = hashlib.sha1(thread_id.strip().encode()).hexdigest()
    return MBOX_DATA[f"thread/{thread_id_hash}"]


def get_attachment_id(attachment):
    content = attachment.get_content()
    if content is None:
        content = b""
    if isinstance(content, str):
        content = content.encode()
    content_hash = hashlib.sha1(content).hexdigest()

    filename = attachment.get_filename()
    if filename is None:
        filename = ""

    return hashlib.sha1((content_hash + "$" + filename).encode()).hexdigest()


def message_to_rdf(mbox_msg):
    msg = Message(mbox_msg)

    # Extracting links and text from the message contents
    text = None
    if msg.content_plain is not None:
        text = msg.content_plain
    if msg.content_html is not None:
        soup = BeautifulSoup(msg.content_html, "html.parser")
        if text is None:
            text = soup.get_text()

    if msg.message_id is None:
        return []

    message_ref = get_message_ref(msg.message_id)

    triples = [
        (message_ref, RDF.type, MBOX_TYPE.Message),
        (message_ref, MBOX_TYPE.messageId, Literal(msg.message_id)),
        (message_ref, MBOX_TYPE.subject, Literal(msg.subject)),
        (message_ref, MBOX_TYPE.text, Literal(text)),
    ]

    if msg.datetime is not None:
        triples.append((message_ref, MBOX_TYPE.dateSent, Literal(msg.datetime, datatype=XSD.dateTime)))

    if msg.in_reply_to is not None:
        triples.append((message_ref, MBOX_TYPE.inReplyTo, get_message_ref(msg.in_reply_to)))
    if msg.thread_id is not None:
        triples.append((message_ref, MBOX_TYPE.thread, get_thread_ref(msg.thread_id)))

    # Addresses
    if msg.addr_from is not None:
        uri = f"mailto:{msg.addr_from.normalized}"
        if _is_valid_uri(uri):
            triples.append((message_ref, MBOX_TYPE.sender, URIRef(uri)))
        triples.append((message_ref, MBOX_TYPE.senderEmail, Literal(msg.addr_from.email)))
        if msg.addr_from.name:
            triples.append((message_ref, MBOX_TYPE.senderName, Literal(msg.addr_from.name)))

    if msg.addrs_to is not None:
        for addr in msg.addrs_to:
            uri = f"mailto:{addr.normalized}"
            if _is_valid_uri(uri):
                triples.append((message_ref, MBOX_TYPE.recipient, URIRef(uri)))
    if msg.addrs_cc is not None:
        for addr in msg.addrs_cc:
            uri = f"mailto:{addr.normalized}"
            if _is_valid_uri(uri):
                triples.append((message_ref, MBOX_TYPE.ccRecipient, URIRef(uri)))
    if msg.addrs_bcc is not None:
        for addr in msg.addrs_bcc:
            uri = f"mailto:{addr.normalized}"
            if _is_valid_uri(uri):
                triples.append((message_ref, MBOX_TYPE.bccRecipient, URIRef(uri)))
    if msg.addr_reply_to is not None:
        uri = f"mailto:{msg.addr_reply_to.normalized}"
        if _is_valid_uri(uri):
            triples.append((message_ref, MBOX_TYPE.replyTo, URIRef(uri)))

    # Attachments
    for attachment in msg.attachments:
        attachment_id = get_attachment_id(attachment)
        attachment_ref = message_ref + f"/attachment/{attachment_id}"

        triples.append((message_ref, MBOX_TYPE.attachment, attachment_ref))
        triples.append((attachment_ref, RDF.type, MBOX_TYPE.Attachment))
        triples.append((attachment_ref, MBOX_TYPE.name, Literal(attachment.get_filename())))
        triples.append(
            (
                attachment_ref,
                MBOX_TYPE.contentSize,
                Literal(len(attachment.get_content()), datatype=XSD.nonNegativeInteger),
            )
        )
        triples.append((attachment_ref, MBOX_TYPE.encodingFormat, Literal(attachment.get_content_type())))

    # Additional info
    if msg["List-Id"]:
        triples.append((message_ref, MBOX_TYPE.listId, Literal(msg["List-Id"])))

    return triples


def parse_mbox(graph, exports_dir="exports", cache_dir="cache"):
    mbox_files = list(Path(exports_dir).glob("**/*.mbox"))
    for mbox_file_idx, mbox_file in enumerate(mbox_files):
        mbox = mailbox.mbox(mbox_file)

        mbox_size = len(mbox)

        desc = f"[{mbox_file_idx + 1}/{len(mbox_files)}] {mbox_file.relative_to(exports_dir)}"
        for msg_index, mbox_msg in tqdm(enumerate(mbox), total=mbox_size, desc=desc):
            try:
                msg_triples = message_to_rdf(mbox_msg)
                for triple in msg_triples:
                    graph.add(triple)
            except KeyboardInterrupt:
                # sys.exit()
                print("Serializing graph...")
                graph.serialize(Path(cache_dir) / "mbox.ttl", format="turtle")
            except Exception as e:
                print(f'Error parsing message {mbox_msg["message_id"]}: {e}')
                # Print full stack trace
                traceback.print_exc()
                continue


def discover_and_parse(graph):
    parse_mbox(graph)


def main(exports_dir="exports", cache_dir="cache"):
    graph = Graph()
    graph.bind("rdf", RDF)
    graph.bind("xsd", XSD)
    graph.bind("own_mbox", MBOX_TYPE)

    parse_mbox(graph, exports_dir=exports_dir, cache_dir=cache_dir)

    graph.serialize(Path(cache_dir) / "mbox.nt", format="nt", encoding="utf-8")


if __name__ == "__main__":
    main()
