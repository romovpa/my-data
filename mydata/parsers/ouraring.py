"""
Parser for Oura ring data.
"""

import glob
import json

from rdflib import Graph
from rdflib.namespace import XSD, Namespace

from mydata.utils import add_records_to_graph, parse_datetime

OURA_TYPE = Namespace("https://ownld.org/service/ouraring/")
OURA_DATA = Namespace("mydata://db/service/ouraring/")

context_jsonld = {
    "@vocab": OURA_TYPE,
    "start": {"@type": XSD["dateTime"]},
    "end": {"@type": XSD["dateTime"]},
}


def parse_ouraring(data_file):
    with open(data_file) as fin:
        sleep_data = json.load(fin)

    for record in sleep_data["sleep"]:
        start_time = parse_datetime(record["bedtime_start"])
        end_time = parse_datetime(record["bedtime_end"])

        event_ref = OURA_DATA[f"bedtime/{start_time.isoformat()}"]

        yield {
            "@id": event_ref,
            "@type": OURA_TYPE.Bedtime,
            "start": start_time,
            "end": end_time,
        }


def discover_and_parse(graph):
    for oura_filename in glob.glob("exports/**/oura_data_*.json", recursive=True):
        print(f"Parsing {oura_filename}")
        add_records_to_graph(graph, context_jsonld, parse_ouraring(oura_filename))


def main():
    graph = Graph()
    discover_and_parse(graph)
    graph.serialize("cache/ouraring_jsonld.ttl", format="turtle")


if __name__ == "__main__":
    main()
