"""
CLI for importing data into the triplestore.

Scans the `exports` dir and your local data folders with all parsers, structures the data into a graph,
serializes it to `cache` for uploading to a SPARQL triplestore like Apache Fuseki.

Usage:
$ python -m mydata.importer
"""

from datetime import datetime

from rdflib import Graph

PARSERS = {
    module_name: getattr(
        __import__(f"mydata.parsers.{module_name}", fromlist=["discover_and_parse"]),
        "discover_and_parse",
    )
    for module_name in [
        # 'mbox.mbox_to_rdf',
        "apple_knowledge",
        "apple_photos",
        "browser",
        "google_activity",
        "ouraring",
        "password_managers",
        "telegram",
    ]
}


def main():
    # TODO Split the output graph into smaller parts
    # TODO Parallelize with multiprocessing?

    graph = Graph()

    for parser_name, discover_and_parse in PARSERS.items():
        print(f"Parsing {parser_name}")
        discover_and_parse(graph)

    output_filename = f'cache/parsed_{datetime.now().strftime("%Y-%m-%d-%H-%M-%S")}.nt'

    graph.serialize(output_filename, format="nt", encoding="utf-8")


if __name__ == "__main__":
    main()
