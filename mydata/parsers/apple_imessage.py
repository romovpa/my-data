"""
Import Apple iMessage data (sms, mms, imessage) from ~/Library/Messages/chat.db.
"""
import glob
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

import phonenumbers
from rdflib import Namespace, RDF, Literal, XSD, Graph, URIRef

from mydata.utils import SQLiteConnection

APPLE_TYPE = Namespace('https://ownld.org/service/apple/')
APPLE_DATA = Namespace('mydata://db/service/apple/')

MESSAGE_QUERY = '''
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
'''

CHAT_QUERY = '''
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
'''

LINK_QUERY = '''
    SELECT 
        chat.guid AS 'chat_guid',
        message.guid AS 'message_guid'
    FROM chat_message_join
    JOIN chat ON chat.ROWID = chat_message_join.chat_id
    JOIN message ON message.ROWID = chat_message_join.message_id
'''


def guid_ref(guid):
    return APPLE_DATA[quote(f"{guid}")]


def parse_imessage(graph, db_file):
    with SQLiteConnection(db_file) as db:

        chats = db.sql(CHAT_QUERY)
        for chat in chats:
            chat_ref = guid_ref(chat['guid'])

            graph.add((chat_ref, RDF.type, APPLE_TYPE['Chat']))
            graph.add((chat_ref, APPLE_TYPE['guid'], Literal(chat['guid'])))
            graph.add((chat_ref, APPLE_TYPE['account'], guid_ref(chat['account_id'])))

            graph.add((chat_ref, APPLE_TYPE['identifier'], Literal(chat['chat_identifier'])))
            graph.add((chat_ref, APPLE_TYPE['service'], Literal(chat['service_name'])))
            if chat['display_name']:
                graph.add((chat_ref, APPLE_TYPE['display_name'], Literal(chat['display_name'])))

            for binary_col in ('is_archived', 'is_filtered', 'is_blackholed'):
                if chat[binary_col] is not None:
                    graph.add((chat_ref, APPLE_TYPE[binary_col], Literal(bool(chat[binary_col]), datatype=XSD.boolean)))

            if chat['guid'].startswith('SMS;-;'):
                sms_id = chat['guid'][len('SMS;-;'):]
                try:
                    number = phonenumbers.parse(sms_id)
                    number_norm = phonenumbers.format_number(number, phonenumbers.PhoneNumberFormat.E164)
                    graph.add((chat_ref, APPLE_TYPE['contact'], URIRef(f'tel:{number_norm}')))
                except phonenumbers.NumberParseException:
                    graph.add((chat_ref, APPLE_TYPE['contact'], URIRef(f'sms:{quote(sms_id)}')))

            if chat['guid'].startswith('iMessage;-;'):
                imessage_id = chat['guid'][len('iMessage;-;'):]
                graph.add((chat_ref, APPLE_TYPE['contact'], URIRef(f'imessage:{imessage_id}')))
                try:
                    number = phonenumbers.parse(imessage_id)
                    number_norm = phonenumbers.format_number(number, phonenumbers.PhoneNumberFormat.E164)
                    graph.add((chat_ref, APPLE_TYPE['contact'], URIRef(f'tel:{number_norm}')))
                except phonenumbers.NumberParseException:
                    pass

        messages = db.sql(MESSAGE_QUERY)
        for message in messages:
            message_ref = guid_ref(message['guid'])

            graph.add((message_ref, RDF.type, APPLE_TYPE['Message']))

            for guid_col in ('account_guid', 'associated_message_guid', 'reply_to_guid', 'thread_originator_guid'):
                if message[guid_col] is not None:
                    type_name = guid_col[:-len('_guid')]
                    graph.add((message_ref, APPLE_TYPE[type_name], guid_ref(message[guid_col])))

            for time_col in ('time', 'time_read', 'time_delivered', 'time_retracted', 'time_edited'):
                if message[time_col] is not None:
                    dt = datetime.strptime(message[time_col], '%Y-%m-%d %H:%M:%S')
                    graph.add((message_ref, APPLE_TYPE[time_col], Literal(dt, datatype=XSD.dateTime)))

            for text_col in ('text', 'subject', 'service', 'destination_caller_id'):
                if message[text_col] is not None:
                    graph.add((message_ref, APPLE_TYPE[text_col], Literal(message[text_col])))

            for binary_col in (
                    'is_archive', 'is_audio_message', 'is_auto_reply', 'is_corrupt', 'is_delayed', 'is_delivered',
                    'is_emote', 'is_empty', 'is_expirable', 'is_finished', 'is_forward', 'is_from_me', 'is_played',
                    'is_prepared', 'is_read', 'is_sent', 'is_service_message', 'is_spam', 'is_system_message',
                    'was_data_detected', 'was_deduplicated', 'was_delivered_quietly', 'was_detonated',
                    'was_downgraded'):
                if message[binary_col] is not None:
                    graph.add((message_ref, APPLE_TYPE[binary_col], Literal(bool(message[binary_col]), datatype=XSD.boolean)))

        links = db.sql(LINK_QUERY)
        for link in links:
            graph.add((guid_ref(link['message_guid']), APPLE_TYPE['chat'], guid_ref(link['chat_guid'])))


def discover_and_parse_imessage(graph):
    default_chat_path = Path.home() / 'Library/Messages/chat.db'
    if default_chat_path.exists():
        parse_imessage(graph, default_chat_path)
    for db_filename in glob.glob('exports/**/chat*.db', recursive=True):
        parse_imessage(graph, db_filename)


def main():
    graph = Graph()
    graph.bind('rdf', RDF)
    graph.bind('xsd', XSD)
    graph.bind('own_apple', APPLE_TYPE)

    discover_and_parse_imessage(graph)

    print(f'Triples: {len(graph)}')

    graph.serialize('cache/apple_imessage.ttl', format='turtle')


if __name__ == '__main__':
    main()
