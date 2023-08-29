import glob
import json
from datetime import datetime
from pathlib import Path

from rdflib import Graph, Literal
from rdflib.namespace import RDF, XSD, Namespace
from tqdm import tqdm

TG_TYPE = Namespace("https://ownld.org/service/telegram/")
TG_DATA = Namespace("mydata://db/service/telegram/")


TG_CHAT_TYPES = {
    "personal_chat": TG_TYPE.PersonalChat,
    "private_group": TG_TYPE.PrivateGroup,
    "bot_chat": TG_TYPE.BotChat,
    "private_supergroup": TG_TYPE.PrivateSupergroup,
    "public_channel": TG_TYPE.PublicChannel,
    "private_channel": TG_TYPE.PrivateChannel,
    "public_supergroup": TG_TYPE.PublicSupergroup,
    "saved_messages": TG_TYPE.SavedMessages,
}


def parse_telegram(graph, dump_dir):
    # results.json
    with open(dump_dir / "result.json") as fin:
        results = json.load(fin)

    for chat in tqdm(results["chats"]["list"], position=0, desc="Chats"):
        chat_id = chat["id"]
        chat_ref = TG_DATA[f"chat/{chat_id}"]

        graph.add((chat_ref, RDF.type, TG_TYPE.Chat))
        graph.add((chat_ref, RDF.type, TG_CHAT_TYPES[chat["type"]]))
        graph.add((chat_ref, TG_TYPE.id, Literal(chat_id)))

        if "name" in chat:
            graph.add((chat_ref, TG_TYPE.name, Literal(chat["name"])))
        else:
            print(json.dumps(chat, indent=2))

        for message in tqdm(chat["messages"], position=1, leave=False, desc="Messages"):
            message_id = message["id"]
            message_ref = TG_DATA[f"chat/{chat_id}/message/{message_id}"]

            time = datetime.fromtimestamp(int(message["date_unixtime"]))

            graph.add((message_ref, RDF.type, TG_TYPE.Message))
            graph.add((message_ref, TG_TYPE.chat, chat_ref))
            graph.add((message_ref, TG_TYPE.messageType, Literal(message["type"])))
            graph.add((message_ref, TG_TYPE.time, Literal(time, datatype=XSD.dateTime)))

            if message["type"] != "message":
                continue

            if "text" in message:
                graph.add((message_ref, TG_TYPE.text, Literal(message["text"])))

            if "reply_to_message_id" in message:
                reply_to_message_ref = TG_DATA[f'chat/{chat_id}/message/{message["reply_to_message_id"]}']
                graph.add((message_ref, TG_TYPE.replyTo, reply_to_message_ref))

            if "mime_type" in message:
                graph.add((message_ref, TG_TYPE.mimeType, Literal(message["mime_type"])))

            sender_id = message["from_id"]
            sender_ref = TG_DATA[f"user/{sender_id}"]

            graph.add((message_ref, TG_TYPE.sender, sender_ref))

            graph.add((sender_ref, RDF.type, TG_TYPE.User))
            graph.add((sender_ref, TG_TYPE.name, Literal(message["from"])))


def main():
    graph = Graph()
    graph.bind("rdf", RDF)
    graph.bind("xsd", XSD)
    graph.bind("own_tg", TG_TYPE)

    discover_and_parse(graph)

    graph.serialize("cache/telegram.nt", format="nt", encoding="utf-8")


def discover_and_parse(graph):
    for results_file in glob.glob("exports/telegram/**/result.json", recursive=True):
        dump_dir = Path(results_file).parent
        parse_telegram(graph, dump_dir)


if __name__ == "__main__":
    main()
