"""
This script looks for .mbox files in `exports` and converts
all mailboxes into a simplified representation in `cache`.

Usage:
PYTHONIOENCODING=utf-8 python mailbox_analyzer.py
"""

import collections
import io
import json
import mailbox
import re
import sys
import warnings
import zipfile
from pathlib import Path

import pandas
import requests
import tldextract
from bs4 import BeautifulSoup
from tqdm import tqdm

from mydata.email_data import Message

warnings.filterwarnings("ignore", category=UserWarning, module="bs4")


def parse_mbox_message(mbox_msg):
    msg = Message(mbox_msg)

    text = None
    links = []
    if msg.content_plain is not None:
        text = msg.content_plain
    if msg.content_html is not None:
        soup = BeautifulSoup(msg.content_html, "html.parser")
        if text is None:
            text = soup.get_text()
        links = [
            {
                "url": link.attrs["href"],
                "text": link.text,
            }
            for link in soup.find_all("a")
            if "href" in link
        ]

    return {
        # Basic information
        "unixtime": msg.unixtime,
        "datetime": msg.datetime.strftime("%Y-%m-%d %H:%M:%S") if msg.datetime else None,
        "message_id": msg.message_id.strip() if msg.message_id is not None else None,
        "in_reply_to": msg.in_reply_to.strip() if msg.in_reply_to is not None else None,
        "thread_id": msg.thread_id.strip() if msg.thread_id is not None else None,
        "from": msg.addr_from.normalized if msg.addr_from else None,
        "from_original": msg.addr_from.email if msg.addr_from else None,
        "from_name": msg.addr_from.name if msg.addr_from else None,
        "delivered_to": msg["Delivered-To"],
        "to": [addr.normalized for addr in msg.addrs_to],
        "cc": [addr.normalized for addr in msg.addrs_cc],
        "bcc": [addr.normalized for addr in msg.addrs_bcc],
        "reply_to": msg.addr_reply_to.normalized if msg.addr_reply_to else None,
        "attachments": [
            {
                "filename": attachment.get_filename(),
                "type": attachment.get_content_type(),
                "size": len(attachment.get_content()),
            }
            for attachment in msg.attachments
        ],
        "subject": msg.subject,
        # Header features
        "headers": list(set(list(msg.message))),  # list of unique headers present in the msg
        "labels": msg.labels,
        "auto_submitted": msg["Auto-Submitted"],  # https://www.rfc-editor.org/rfc/rfc5436.html#section-2.7.1
        "feedback_id": msg["Feedback-ID"] or msg["X-Feedback-ID"],  # Google Feedback Loop
        "auto_response_suppress": msg["X-Auto-Response-Suppress"],
        # https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-oxcmail/ced68690-498a-4567-9d14-5c01f974d8b1?redirectedfrom=MSDN
        "list_id": msg["List-Id"],  # https://www.rfc-editor.org/rfc/rfc2919
        "list_unsubscribe": msg["List-Unsubscribe"],
        "precedence": msg["Precedence"],  # http://www.faqs.org/rfcs/rfc2076.html
        "x_msfbl": msg["X-MSFBL"],  # "Other obscure headers" in https://www.arp242.net/autoreply.html
        "x_loop": msg["X-Loop"],
        "x_autoreply": msg["X-Autoreply"],
        "x_mailer": msg["X-Mailer"],
        "x_library": msg["X-Library"],
        "x_forwarded_to": msg["X-Forwarded-To"],
        "x_forwarded_for": msg["X-Forwarded-For"],
        # Content features
        "has_plain": msg.content_plain is not None,
        "has_html": msg.content_html is not None,
        "text": text,
        "links": links,
    }


def label_threads(messages):
    """
    Assigns to each message `first_id`, the pointer to the first known message in the thread.
    This ID can be used to group messages in threads.
    """
    for message_id, msg in messages.items():
        first_id = message_id
        earliest_id = first_id
        earliest_time = msg["unixtime"]

        visited_ids = {first_id}
        while first_id in messages and messages[first_id]["in_reply_to"] is not None:
            parent_id = messages[first_id]["in_reply_to"]
            if parent_id == first_id:
                # Self-reference
                break
            if parent_id in visited_ids:
                # Cyclic reference: set the earliest message as the first
                first_id = earliest_id
                break

            first_id = parent_id
            visited_ids.add(first_id)
            if first_id in messages and messages[first_id]["unixtime"] is not None:
                if earliest_time > messages[first_id]["unixtime"]:
                    earliest_id = first_id
                    earliest_time = messages[first_id]["unixtime"]

        msg["first_id"] = first_id


def group_threads(messages):
    thread_messages = collections.defaultdict(list)
    threads = []

    for msg in messages.values():
        thread_id = msg["first_id"]
        if thread_id:
            thread_messages[thread_id].append(msg)

    for thread_id in list(thread_messages.keys()):
        thread_messages[thread_id].sort(key=lambda msg: msg["unixtime"] or 0)

        main_is_first = False
        main_message = thread_messages[thread_id][0]
        for msg in thread_messages[thread_id]:
            if msg["first_id"] == msg["message_id"]:
                main_message = msg
                if msg["in_reply_to"] is None or msg["in_reply_to"] == msg["message_id"]:
                    main_is_first = True

        thread = {
            "thread_id": thread_id,
            "main_is_first": main_is_first,
            "main": main_message,
            "messages": thread_messages[thread_id],
        }

        threads.append(thread)

    return threads


def find_my_addrs(messages, min_coverage=0.99, max_addrs=100):
    """
    Automatically detects the list of my email addresses.
    """
    message_addrs = []

    listing_headers = {"List-Unsubscribe", "List-Id"}
    for message_id, message in messages.items():
        if not listing_headers.intersection(message["headers"]):
            addrs = message["to"] + message["cc"] + message["bcc"]
            if message["x_forwarded_to"]:
                addrs += message["x_forwarded_to"].replace(",", " ").split()
            if message["x_forwarded_for"]:
                addrs += message["x_forwarded_for"].replace(",", " ").split()
            addrs = list(set(addrs))
            if "" in addrs:
                addrs.remove("")
            message_addrs.append((message_id, (message["from"], addrs)))

    if min_coverage < 1:
        min_coverage = max(1, len(message_addrs) * min_coverage)

    my_addrs = set()
    my_addrs_list = []

    for trial in range(max_addrs):
        addr_occurrences = collections.Counter()
        num_covered = 0
        for _, (from_, addrs) in message_addrs:
            if not set(addrs).intersection(my_addrs) and from_ not in my_addrs:
                for addr in addrs:
                    addr_occurrences[addr] += 1
            else:
                num_covered += 1

        if num_covered >= min_coverage:
            break

        if addr_occurrences:
            new_addr, new_cnt = addr_occurrences.most_common(1)[0]
            my_addrs.add(new_addr)
            my_addrs_list.append((new_addr, new_cnt))

    return my_addrs_list


PATTERN_NOREPLY = re.compile(r"(info)|(notifications?)|((.+)?not?.?reply(.+)?)")
GENERATED_HEADERS = set(
    map(
        lambda s: s.lower(),
        [
            "Feedback-Id",
            "X-Feedback-Id",
        ],
    )
)


def extract_thread_features(thread, my_addrs=[]):
    main = thread["main"]
    messages = thread["messages"]

    is_generated = False
    is_generated |= main["message_id"].find(".JavaMail.") >= 0
    if main["from"]:
        from_name = main["from"].split("@", 1)[0]
        is_generated |= any(
            (
                PATTERN_NOREPLY.match(from_name) is not None,
                len(GENERATED_HEADERS.intersection(map(lambda s: s.lower(), main["headers"]))) > 0,
            )
        )

    from_domain = None
    if main["from"]:
        parts = tldextract.extract(main["from"].lower())
        from_domain = parts.domain + "." + parts.suffix

    to_me = None
    for addr in main["to"]:
        if addr in my_addrs:
            to_me = addr
            break

    features = {
        "thread_id": thread["thread_id"],
        "unixtime": main["unixtime"],
        "datetime": main["datetime"],
        "subject": main["subject"],
        "main_is_first": thread["main_is_first"],
        "from": main["from"],
        "from_domain": from_domain,
        "to_me": to_me,
        "recipients_to": len([addr for addr in main["to"] if addr.strip()]),
        "recipients_all": len([addr for addr in (main["to"] + main["cc"] + main["bcc"]) if addr.strip()]),
        "is_generated": is_generated,
        "num_messages": len(messages),
    }

    account = None
    if features["datetime"] is not None and main["from"] not in my_addrs and to_me is not None:
        service_id = from_domain
        account = (service_id, to_me)

    return features, account


def detect_accounts(threads, my_addrs, domain_rank=None):
    account_threads = collections.defaultdict(list)
    for thread in threads:
        features, account = extract_thread_features(
            thread,
            my_addrs=my_addrs,
        )
        if account is not None:
            account_threads[account].append(features)

    accounts = []
    for account, acc_threads in account_threads.items():
        service_id, my_addr = account
        acc_threads = sorted(acc_threads, key=lambda thread: thread["datetime"])
        first_thread = acc_threads[0]

        record = {
            "service_id": service_id,
            "my_addr": my_addr,
            "joined": first_thread["datetime"],
            "first_subject": first_thread["subject"],
            "last": acc_threads[-1]["datetime"],
            "threads": len(acc_threads),
            "generated": sum([int(thread["is_generated"]) for thread in acc_threads]),
            "domain_rank": domain_rank.get(service_id) if domain_rank is not None else None,
        }
        accounts.append(record)

    return accounts


def parse_mbox(exports_dir="exports"):
    n_failed_to_parse = 0
    messages = {}

    mbox_files = list(Path(exports_dir).glob("**/*.mbox"))
    for mbox_file_idx, mbox_file in enumerate(mbox_files):
        mbox = mailbox.mbox(mbox_file)

        mbox_size = len(mbox)

        desc = f"[{mbox_file_idx + 1}/{len(mbox_files)}] {mbox_file.relative_to(exports_dir)}"
        for msg_index, mbox_msg in tqdm(enumerate(mbox), total=mbox_size, desc=desc):
            try:
                message = parse_mbox_message(mbox_msg)
            except KeyboardInterrupt:
                sys.exit()
            except Exception:
                n_failed_to_parse += 1
                continue

            message_id = message["message_id"]
            if message_id is not None:
                if message_id in messages:
                    # Duplicate: leave the earliest message
                    previous_message = messages[message_id]
                    if previous_message["unixtime"] is None or message["unixtime"] < previous_message["unixtime"]:
                        messages[message_id] = message
                else:
                    messages[message_id] = message

    print(f"Failed to parse: {n_failed_to_parse}")
    return messages


def read_alexa_ranks(url="http://s3.amazonaws.com/alexa-static/top-1m.csv.zip", filename="top-1m.csv"):
    """Downloads Alexa Top-1M domains. See https://gist.github.com/chilts/7229605"""
    response = requests.get(url, stream=True)
    archive = zipfile.ZipFile(io.BytesIO(response.content))
    with archive.open(filename) as fin:
        df_alexa_ranks = pandas.read_csv(
            fin,
            names=["rank", "domain"],
            index_col="domain",
        )
    alexa_domain_rank = df_alexa_ranks.to_dict()["rank"]
    return alexa_domain_rank


def discover_and_parse_mbox(exports_dir="exports", cache_dir="cache"):
    messages_json = Path(cache_dir) / "email.json"
    if messages_json.exists():
        with open(messages_json, "r") as fin:
            messages = json.load(fin)
    else:
        messages = parse_mbox(exports_dir)

    label_threads(messages)

    with open(messages_json, "w") as fout:
        json.dump(messages, fout, ensure_ascii=False)

    my_addrs_list = find_my_addrs(messages)
    my_addrs = set([addr for addr, cnt in my_addrs_list])

    print("My Email Addresses:")
    for addr, cnt in my_addrs_list:
        print(f"  - {addr} ({cnt} messages)")
    with open(Path(cache_dir) / "my_email_addrs.json", "w") as fout:
        json.dump([addr for addr, cnt in my_addrs_list], fout)

    # Alexa rank. It is outdated, yet better than nothing.
    alexa_domain_rank = read_alexa_ranks()

    threads = group_threads(messages)
    accounts = detect_accounts(threads, my_addrs, domain_rank=alexa_domain_rank)

    with open(Path(cache_dir) / "accounts.json", "w") as fout:
        json.dump(accounts, fout)

    return accounts
