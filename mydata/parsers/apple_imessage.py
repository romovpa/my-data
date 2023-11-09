"""
Import Apple iMessage data (sms, mms, imessage) from ~/Library/Messages/chat.db.
"""
import glob
from pathlib import Path
from urllib.parse import quote

import phonenumbers
from rdflib import XSD, Graph, Namespace, URIRef

from mydata.utils import SQLiteConnection, parse_datetime

APPLE_TYPE = Namespace("https://ownld.org/service/apple/")
APPLE_DATA = Namespace("mydata://db/service/apple/")

context_jsonld = {
    "@vocab": APPLE_TYPE,
    "contacts": {"@id": "contact", "@type": "@id"},
    "chat": {"@type": "@id"},
}

MESSAGE_QUERY = """
    SELECT
        guid,
        account_guid,
        associated_message_guid,
        reply_to_guid,
        thread_originator_guid,

        CASE (date > 0)
            WHEN TRUE THEN
                DATETIME(CAST(date AS FLOAT)/1000000000+978307200, 'UNIXEPOCH')
            ELSE NULL
        END AS "time",

        CASE (date_read > 0)
            WHEN TRUE THEN
                DATETIME(CAST(date_read AS FLOAT)/1000000000+978307200,'UNIXEPOCH')
            ELSE NULL
        END AS "time_read",

        CASE (date_delivered > 0)
            WHEN TRUE THEN
                DATETIME(CAST(date_delivered AS FLOAT)/1000000000+978307200,'UNIXEPOCH')
            ELSE NULL
        END AS "time_delivered",

        CASE (date_retracted > 0)
            WHEN TRUE THEN
                DATETIME(CAST(date_retracted AS FLOAT)/1000000000+978307200,'UNIXEPOCH')
            ELSE NULL
        END AS "time_retracted",

        CASE (date_edited > 0)
            WHEN TRUE THEN
                DATETIME(CAST(date_edited AS FLOAT)/1000000000+978307200,'UNIXEPOCH')
            ELSE NULL
        END AS "time_edited",

        text,
        subject,
        service,
        destination_caller_id,

        is_archive,
        is_audio_message,
        is_auto_reply,
        is_corrupt,
        is_delayed,
        is_delivered,
        is_emote,
        is_empty,
        is_expirable,
        is_finished,
        is_forward,
        is_from_me,
        is_played,
        is_prepared,
        is_read,
        is_sent,
        is_service_message,
        is_spam,
        is_system_message,
        was_data_detected,
        was_deduplicated,
        was_delivered_quietly,
        was_detonated,
        was_downgraded

    FROM message
"""

CHAT_QUERY = """
    SELECT
        guid,
        account_id,
        chat_identifier,
        service_name,
        display_name,

        is_archived,
        is_filtered,
        is_blackholed
    FROM chat
"""

LINK_QUERY = """
    SELECT
        chat.guid AS 'chat_guid',
        message.guid AS 'message_guid'
    FROM chat_message_join
    JOIN chat ON chat.ROWID = chat_message_join.chat_id
    JOIN message ON message.ROWID = chat_message_join.message_id
"""


def guid_ref(guid):
    return APPLE_DATA[quote(f"{guid}")]


def guid_to_contacts(guid):
    contacts = []
    if guid.startswith("SMS;-;"):
        sms_id = guid[len("SMS;-;") :]
        try:
            number = phonenumbers.parse(sms_id)
            number_norm = phonenumbers.format_number(number, phonenumbers.PhoneNumberFormat.E164)
            contacts.append(URIRef(f"tel:{number_norm}"))
        except phonenumbers.NumberParseException:
            contacts.append(URIRef(f"sms:{quote(sms_id)}"))
    if guid.startswith("iMessage;-;"):
        imessage_id = guid[len("iMessage;-;") :]
        contacts.append(URIRef(f"imessage:{imessage_id}"))
        try:
            number = phonenumbers.parse(imessage_id)
            number_norm = phonenumbers.format_number(number, phonenumbers.PhoneNumberFormat.E164)
            contacts.append(URIRef(f"tel:{number_norm}"))
        except phonenumbers.NumberParseException:
            pass
    return contacts


def parse_imessage(db_file):
    with SQLiteConnection(db_file) as db:
        chats = db.sql(CHAT_QUERY)
        for chat in chats:
            yield {
                "@id": guid_ref(chat["guid"]),
                "@type": "Chat",
                "guid": chat["guid"],
                "contacts": guid_to_contacts(chat["guid"]),
                "account": {
                    "@id": guid_ref(chat["account_id"]),
                    "@type": "Account",
                    "guid": chat["account_id"],
                    "contacts": guid_to_contacts(chat["account_id"]),
                },
                "identifier": chat["chat_identifier"],
                "service": chat["service_name"],
                "display_name": chat["display_name"] if chat["display_name"] != "" else None,
                "is_archived": bool(chat["is_archived"]) if chat["is_archived"] is not None else None,
                "is_filtered": bool(chat["is_filtered"]) if chat["is_filtered"] is not None else None,
                "is_blackholed": bool(chat["is_blackholed"]) if chat["is_blackholed"] is not None else None,
            }

        links = db.sql(LINK_QUERY)
        for link in links:
            yield {
                "@id": guid_ref(link["message_guid"]),
                "chat": guid_ref(link["chat_guid"]),
            }

        messages = db.sql(MESSAGE_QUERY)
        for message in messages:
            record = {
                "@id": guid_ref(message["guid"]),
                "@type": "Message",
            }

            for guid_col in ("account_guid", "associated_message_guid", "reply_to_guid", "thread_originator_guid"):
                if message[guid_col] is not None:
                    type_name = guid_col[: -len("_guid")]
                    record[type_name] = guid_ref(message[guid_col])

            for time_col in ("time", "time_read", "time_delivered", "time_retracted", "time_edited"):
                record[time_col] = {
                    "@value": parse_datetime(message[time_col], "%Y-%m-%d %H:%M:%S"),
                    "@type": XSD.dateTime,
                }

            for text_col in ("text", "subject", "service", "destination_caller_id"):
                record[text_col] = message[text_col]

            for binary_col in (
                "is_archive",
                "is_audio_message",
                "is_auto_reply",
                "is_corrupt",
                "is_delayed",
                "is_delivered",
                "is_emote",
                "is_empty",
                "is_expirable",
                "is_finished",
                "is_forward",
                "is_from_me",
                "is_played",
                "is_prepared",
                "is_read",
                "is_sent",
                "is_service_message",
                "is_spam",
                "is_system_message",
                "was_data_detected",
                "was_deduplicated",
                "was_delivered_quietly",
                "was_detonated",
                "was_downgraded",
            ):
                record[binary_col] = bool(message[binary_col]) if message[binary_col] is not None else None

            yield record


def discover_and_parse_imessage(graph):
    default_chat_path = Path.home() / "Library/Messages/chat.db"
    if default_chat_path.exists():
        for record in parse_imessage(default_chat_path):
            graph.parse(data=record, format="json-ld", context=context_jsonld)
    for db_filename in glob.glob("exports/**/chat*.db", recursive=True):
        for record in parse_imessage(db_filename):
            graph.parse(data=record, format="json-ld", context=context_jsonld)


def main():
    graph = Graph()
    discover_and_parse_imessage(graph)
    print(f"Triples: {len(graph)}")
    graph.serialize("cache/apple_imessage_jsonld.ttl", format="turtle")


if __name__ == "__main__":
    main()
