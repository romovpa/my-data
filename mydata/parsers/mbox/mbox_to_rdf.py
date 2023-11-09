import hashlib
import json
import mailbox
import shutil
import traceback
import warnings
import zipfile
from pathlib import Path
from tempfile import TemporaryDirectory
from zipfile import ZipFile

from bs4 import BeautifulSoup
from rdflib import Graph
from rdflib.namespace import XSD, Namespace
from rdflib.term import _is_valid_uri
from tqdm import tqdm

from mydata.parsers.mbox.email_data import Message
from mydata.utils import add_records_to_graph

warnings.filterwarnings("ignore", category=UserWarning, module="bs4")

MBOX_TYPE = Namespace("https://ownld.org/service/mbox/")
MBOX_DATA = Namespace("mydata://db/service/mbox/")

context_jsonld = {
    "@vocab": MBOX_TYPE,
    "dateSent": {"@type": XSD["dateTime"]},
}


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


def addr_to_record(addr):
    uri = f"mailto:{addr.normalized}"
    return {
        "address": {"@id": uri} if _is_valid_uri(uri) else None,
        "rawAddress": addr.email,
        "name": addr.name,
    }


def extract_text(msg):
    text = None
    if msg.content_plain is not None:
        text = msg.content_plain
    if msg.content_html is not None:
        soup = BeautifulSoup(msg.content_html, "html.parser")
        if text is None:
            text = soup.get_text()
    return text


def parse_message(msg):
    if msg.message_id is None:
        return {}

    message_ref = get_message_ref(msg.message_id)

    return {
        "@id": message_ref,
        "@type": "Message",
        "messageId": msg.message_id,
        "subject": msg.subject,
        "text": extract_text(msg),
        "dateSent": msg.datetime,
        "inReplyTo": {
            "@id": get_message_ref(msg.in_reply_to),
        }
        if msg.in_reply_to is not None
        else None,
        "thread": {
            "@id": get_thread_ref(msg.thread_id),
            "@type": "Thread",
        }
        if msg.thread_id is not None
        else None,
        "from": addr_to_record(msg.addr_from) if msg.addr_from is not None else None,
        "to": [addr_to_record(addr) for addr in (msg.addrs_to or [])],
        "cc": [addr_to_record(addr) for addr in (msg.addrs_cc or [])],
        "bcc": [addr_to_record(addr) for addr in (msg.addrs_bcc or [])],
        "replyTo": addr_to_record(msg.addr_reply_to) if msg.addr_reply_to is not None else None,
        "attachment": [
            {
                "@id": message_ref + f"/attachment/{get_attachment_id(attachment)}",
                "@type": "Attachment",
                "name": attachment.get_filename(),
                "contentSize": len(attachment.get_content()),
                "encodingFormat": attachment.get_content_type(),
            }
            for attachment in msg.attachments
        ],
        "listId": msg["List-Id"],
    }


def discover_and_parse(graph, exports_dir="exports", cache_dir="cache", parse_all=False):
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

                yield parse_message(msg)
                parsed_ids.add(msg.message_id)

            except Exception as e:
                exceptions.append(
                    {
                        "mbox_file": filename,
                        "msg_index": msg_index,
                        "exception": str(e),
                        "traceback": traceback.format_exc(),
                    }
                )
                save_introspection()

    if parse_all:
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

                    records = process_mbox_file(f"{zip_file}:{mbox_file}", Path(tmpdir) / mbox_file)
                    add_records_to_graph(graph, context_jsonld, records)

                    graph.serialize(Path(cache_dir) / "mbox_intermediate.nt", format="nt", encoding="utf-8")

                    shutil.rmtree(tmpdir)

        mbox_files = Path(exports_dir).glob("**/*.mbox")
        for mbox_file_idx, mbox_file in enumerate(mbox_files):
            records = process_mbox_file(mbox_file, mbox_file)
            add_records_to_graph(graph, context_jsonld, records)

    else:
        mbox_file = "exports/Takeout/Mail/All mail Including Spam and Trash.mbox"
        records = process_mbox_file(mbox_file, mbox_file)
        add_records_to_graph(graph, context_jsonld, records)

    save_introspection()


def main():
    graph = Graph()
    discover_and_parse(graph)
    print("Triples:", len(graph))
    graph.serialize("cache/mbox_jsonld.nt", format="nt", encoding="utf-8")


if __name__ == "__main__":
    main()
