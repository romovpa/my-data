import datetime
import hashlib
import json
import mailbox
import multiprocessing
import os
import shutil
import traceback
import warnings
import zipfile
from pathlib import Path
from tempfile import TemporaryDirectory
from zipfile import ZipFile

from bs4 import BeautifulSoup
from rdflib import Graph, Literal, URIRef
from rdflib.namespace import RDF, XSD, Namespace
from rdflib.term import _is_valid_uri
from tqdm import tqdm

from mydata.parsers.mbox.email_data import MboxChunk, Message, mbox_chunks

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


def message_to_rdf(msg):
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
    parsed_ids = set()
    exceptions = []

    def save_introspection():
        with open(cache_dir + "/mbox_introspection.json", "w") as fout:
            json.dump(
                {
                    "parsed_ids": list(parsed_ids),
                    "exceptions": exceptions,
                },
                fout,
            )

    def process_mbox_file(filename, file_obj):
        print(filename)
        mbox = mailbox.mbox(file_obj)

        mbox_size = len(mbox)

        desc = f"{filename}"
        for msg_index, mbox_msg in tqdm(enumerate(mbox), total=mbox_size, desc=desc):
            try:
                msg = Message(mbox_msg)

                if msg.message_id is None or msg.message_id in parsed_ids:
                    continue

                msg_triples = message_to_rdf(msg)
                for triple in msg_triples:
                    graph.add(triple)
                parsed_ids.add(msg.message_id)

            except KeyboardInterrupt:
                save_introspection()

            except Exception as e:
                exceptions.append(
                    {
                        "mbox_file": filename,
                        "msg_index": msg_index,
                        "exception": str(e),
                        "traceback": traceback.format_exc(),
                    }
                )

    zip_files = list(Path(exports_dir).glob("**/*.zip"))
    for zip_file_idx, zip_file in enumerate(zip_files):
        print(zip_file)
        zip = ZipFile(zip_file)
        mbox_files = [f for f in zip.namelist() if f.lower().endswith(".mbox")]
        for mbox_file_idx, mbox_file in enumerate(mbox_files):
            with TemporaryDirectory() as tmpdir:
                try:
                    zip.extract(mbox_file, tmpdir)
                except zipfile.BadZipFile:
                    continue

                process_mbox_file(f"{zip_file}:{mbox_file}", Path(tmpdir) / mbox_file)

                graph.serialize(Path(cache_dir) / "mbox.nt", format="nt", encoding="utf-8")

                shutil.rmtree(tmpdir)

    mbox_files = Path(exports_dir).glob("**/*.mbox")
    for mbox_file_idx, mbox_file in enumerate(mbox_files):
        process_mbox_file(mbox_file, mbox_file)

    save_introspection()


def process_mbox_chunk(filename, begin, end, output_file):
    graph = Graph()
    graph.bind("rdf", RDF)
    graph.bind("xsd", XSD)
    graph.bind("own_mbox", MBOX_TYPE)

    mbox_chunk = MboxChunk(filename, begin, end)
    for i, mbox_msg in enumerate(mbox_chunk):
        try:
            msg = Message(mbox_msg)

            if msg.message_id is None:
                continue

            msg_triples = message_to_rdf(msg)
            for triple in msg_triples:
                graph.add(triple)

        except Exception:
            pass

    graph.serialize(output_file, format="nt", encoding="utf-8")


def parse_mbox_parallel(exports_dir="exports", cache_dir="cache", chunk_size=100 * 1024 * 1024):
    pool = multiprocessing.Pool(16)

    output_dir = Path(cache_dir) / f"mbox_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
    os.makedirs(output_dir, exist_ok=True)

    print(output_dir)

    def process_mbox_file(filename, desc):
        print(filename)
        for chunk in mbox_chunks(filename, chunk_size):
            output_file = output_dir / f"{abs(hash(desc))}_{chunk.begin}_{chunk.end}.nt"
            pool.apply_async(process_mbox_chunk, args=(chunk.filename, chunk.begin, chunk.end, output_file))

    mbox_files = Path(exports_dir).glob("**/*.mbox")
    for mbox_file_idx, mbox_file in enumerate(mbox_files):
        process_mbox_file(mbox_file, mbox_file)

    zip_files = list(Path(exports_dir).glob("**/*.zip"))
    for zip_file_idx, zip_file in enumerate(zip_files):
        print(zip_file)
        zip = ZipFile(zip_file)
        mbox_files = [f for f in zip.namelist() if f.lower().endswith(".mbox")]
        for mbox_file_idx, mbox_file in enumerate(mbox_files):
            with TemporaryDirectory() as tmpdir:
                try:
                    zip.extract(mbox_file, tmpdir)
                except zipfile.BadZipFile:
                    continue

                process_mbox_file(Path(tmpdir) / mbox_file, f"{zip_file}:{mbox_file}")

                shutil.rmtree(tmpdir)

    pool.join()


def discover_and_parse(graph):
    parse_mbox(graph)


def main(exports_dir="exports", cache_dir="cache"):
    # graph = Graph()
    # graph.bind("rdf", RDF)
    # graph.bind("xsd", XSD)
    # graph.bind("own_mbox", MBOX_TYPE)
    #
    # parse_mbox(graph, exports_dir=exports_dir, cache_dir=cache_dir)
    #
    # graph.serialize(Path(cache_dir) / "mbox.nt", format="nt", encoding="utf-8")

    parse_mbox_parallel()


if __name__ == "__main__":
    main()
