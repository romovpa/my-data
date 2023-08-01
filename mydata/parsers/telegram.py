import json
from pathlib import Path
from datetime import datetime
from tqdm import tqdm
from rdflib import Graph, Literal
from rdflib.namespace import XSD, Namespace, RDF


SCHEMA = Namespace('https://schema.org/')
TG = Namespace('https://mydata-schema.org/telegram/')
MYDATA = Namespace('mydata://')

TG_CHAT_TYPES = {
    'personal_chat': TG.PersonalChat,
    'private_group': TG.PrivateGroup,
    'bot_chat': TG.BotChat,
    'private_supergroup': TG.PrivateSupergroup,
    'public_channel': TG.PublicChannel,
    'private_channel': TG.PrivateChannel,
    'public_supergroup': TG.PublicSupergroup,
    'saved_messages': TG.SavedMessages,
}


def parse_telegram(graph, dump_dir):

    # results.json
    with open(dump_dir / 'result.json') as fin:
        results = json.load(fin)

    for chat in tqdm(results['chats']['list']):
        chat_id = chat['id']
        chat_ref = MYDATA[f'telegram/chat/{chat_id}']

        graph.add((chat_ref, RDF.type, TG.Chat))
        graph.add((chat_ref, RDF.type, TG_CHAT_TYPES[chat['type']]))
        graph.add((chat_ref, TG.id, Literal(chat_id)))

        if 'name' in chat:
            graph.add((chat_ref, TG.name, Literal(chat['name'])))
        else:
            print(json.dumps(chat, indent=2))

        for message in chat['messages']:
            message_id = message['id']
            message_ref = MYDATA[f'telegram/chat/{chat_id}/message/{message_id}']

            time = datetime.fromtimestamp(int(message['date_unixtime']))

            graph.add((message_ref, RDF.type, TG.Message))
            graph.add((message_ref, TG.chat, chat_ref))
            graph.add((message_ref, TG.messageType, Literal(message['type'])))
            graph.add((message_ref, TG.time, Literal(time, datatype=XSD.dateTime)))

            if message['type'] != 'message':
                continue

            if 'text' in message:
                graph.add((message_ref, TG.text, Literal(message['text'])))

            if 'reply_to_message_id' in message:
                reply_to_message_ref = MYDATA[f'telegram/chat/{chat_id}/message/{message["reply_to_message_id"]}']
                graph.add((message_ref, TG.replyTo, reply_to_message_ref))

            if 'mime_type' in message:
                graph.add((message_ref, TG.mimeType, Literal(message['mime_type'])))

            sender_id = message['from_id']
            sender_ref = MYDATA[f'telegram/user/{sender_id}']

            graph.add((message_ref, TG.sender, sender_ref))

            graph.add((sender_ref, RDF.type, TG.User))
            graph.add((sender_ref, TG.name, Literal(message['from'])))


def main(dump_dir, graph_path):
    graph = Graph()
    graph.bind('rdf', RDF)
    graph.bind('schema', SCHEMA)
    graph.bind('xsd', XSD)

    parse_telegram(graph, dump_dir)

    graph.serialize(graph_path, format='turtle')


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--dump-dir', type=Path, required=True)
    parser.add_argument('--graph', type=Path, required=True)
    args = parser.parse_args()

    main(args.dump_dir, args.graph)