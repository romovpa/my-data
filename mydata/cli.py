import requests

from mydata.parsers.apple_knowledge import AppleKnowledgeCParser


def main():
    """
    CLI implementation

    Input:
        - data loader + its config
        - debug output files
        - upload
            - is_replace
            - update_endpoint
    """
    # read config

    # import data
    # TODO add a choice of parser
    parser = AppleKnowledgeCParser()
    graph, graph_uri = parser.run()

    # checks and auto-generated schema
    # TODO add check_schema(graph)

    # save to file
    print(graph_uri)
    print(f"Triples: {len(graph)}")
    graph.serialize("cache/apple_knowledgeC_jsonld.ttl", format="turtle")

    # upload graph
    is_replace = False
    update_endpoint = "http://localhost:3030/test/data"

    print("Uploading graph")
    resp = requests.request(
        "POST" if is_replace else "PUT",
        update_endpoint,
        params={"graph": graph_uri},
        data=graph.serialize(format="n3"),
        headers={"Content-Type": "text/n3"},
    )
    print(resp)
    print(resp.json())

    # upload required schemas
    # TODO add schema retrieval and upload


if __name__ == "__main__":
    main()
