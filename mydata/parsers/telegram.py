import glob
import json
from datetime import datetime
from pathlib import Path

from rdflib import Graph
from rdflib.namespace import XSD, Namespace
from tqdm import tqdm

from mydata.utils import add_records_to_graph

TG_TYPE = Namespace("https://ownld.org/service/telegram/")
TG_DATA = Namespace("mydata://db/service/telegram/")


context_jsonld = {
    "@vocab": TG_TYPE,
    "chat": {"@type": "@id"},
    "sender": {"@type": "@id"},
    "time": {"@type": XSD["dateTime"]},
}


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


def parse_telegram(dump_dir):
    with open(dump_dir / "result.json") as fin:
        results = json.load(fin)

    for chat in tqdm(results["chats"]["list"], position=0, desc="Chats"):
        chat_id = chat["id"]
        chat_ref = TG_DATA[f"chat/{chat_id}"]

        yield {
            "@id": chat_ref,
            "@type": ["Chat", TG_CHAT_TYPES[chat["type"]]],
            "id": chat_id,
            "name": chat.get("name"),
        }

        for message in tqdm(chat["messages"], position=1, leave=False, desc="Messages"):
            message_id = message["id"]
            message_ref = TG_DATA[f"chat/{chat_id}/message/{message_id}"]

            time = datetime.fromtimestamp(int(message["date_unixtime"]))

            yield {
                "@id": message_ref,
                "@type": "Message",
                "chat": chat_ref,
                "messageType": message["type"],
                "time": time,
                "text": message.get("text"),
                "replyTo": TG_DATA[f"chat/{chat_id}/message/{message['reply_to_message_id']}"]
                if "reply_to_message_id" in message
                else None,
                "mimeType": message.get("mime_type"),
                "sender": {
                    "@id": TG_DATA[f"user/{message['from_id']}"],
                    "@type": "User",
                    "name": message.get("from_name"),
                }
                if "from_id" in message
                else None,
            }


def discover_and_parse(graph):
    for results_file in glob.glob("exports/telegram/**/result.json", recursive=True):
        dump_dir = Path(results_file).parent
        add_records_to_graph(graph, context_jsonld, parse_telegram(dump_dir))


def main():
    graph = Graph()
    discover_and_parse(graph)
    print(f"Triples: {len(graph)}")
    graph.serialize("cache/telegram_jsonld.nt", format="nt", encoding="utf-8")


if __name__ == "__main__":
    main()
