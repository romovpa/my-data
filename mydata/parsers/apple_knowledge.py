"""
Apple devices keep track of various usage events in knowledgeC.db.

Information about knowldgeC.db:
- http://www.mac4n6.com/blog/2018/8/5/knowledge-is-power-using-the-knowledgecdb-database-on-macos-and-ios-to-determine-precise-user-and-application-usage
- https://github.com/mac4n6/APOLLO/

"""

import glob
import hashlib
import os.path
from datetime import datetime

import requests
from rdflib import Graph
from rdflib.namespace import Namespace

from mydata.utils import SQLiteConnection, parse_datetime

OWNLD_TYPE = Namespace("https://ownld.org/core/")
APPLE_TYPE = Namespace("https://ownld.org/service/apple/")
MY_DATA = Namespace("mydata://db/")
APPLE_DATA = Namespace("mydata://db/service/apple/")


def parse_knowldegeC(db_file):
    with SQLiteConnection(db_file) as db:
        cur = db.sql(
            """
            SELECT
                ZOBJECT.Z_ENT as ent,
                ZSTREAMNAME as "type",
                ZOBJECT.ZVALUESTRING AS "bundle_id",
                ZBUNDLEID as "bundle_id_2",
                ZDEVICEID as "device_id",

                DATETIME(ZOBJECT.ZSTARTDATE+978307200,'UNIXEPOCH') AS "start_time",
                DATETIME(ZOBJECT.ZENDDATE+978307200,'UNIXEPOCH') AS "end_time",
                (ZOBJECT.ZENDDATE-ZOBJECT.ZSTARTDATE) AS "usage_sec",

                --ZSTRUCTUREDMETADATA .Z_DKAPPLICATIONMETADATAKEY__LAUNCHREASON AS "launch_reason",
                --ZSTRUCTUREDMETADATA .Z_DKAPPLICATIONMETADATAKEY__EXTENSIONCONTAININGBUNDLEIDENTIFIER AS "ext_containing_bundle_id",
                --ZSTRUCTUREDMETADATA .Z_DKAPPLICATIONMETADATAKEY__EXTENSIONHOSTIDENTIFIER AS "ext_host_id",
                --ZSTRUCTUREDMETADATA.Z_DKAPPLICATIONACTIVITYMETADATAKEY__ACTIVITYTYPE AS "activity_type",
                --ZSTRUCTUREDMETADATA.Z_DKAPPLICATIONACTIVITYMETADATAKEY__TITLE as "title",
                --ZSTRUCTUREDMETADATA.Z_DKAPPLICATIONACTIVITYMETADATAKEY__USERACTIVITYREQUIREDSTRING as "activity_string",
                --datetime(ZSTRUCTUREDMETADATA.Z_DKAPPLICATIONACTIVITYMETADATAKEY__EXPIRATIONDATE+978307200,'UNIXEPOCH', 'LOCALTIME') as "expiration_date",

                ZOBJECT.ZSECONDSFROMGMT/3600 AS "gmt_offset",
                DATETIME(ZOBJECT.ZCREATIONDATE+978307200,'UNIXEPOCH') AS "created_time",
                ZOBJECT.ZUUID AS "uuid",
                ZSTRUCTUREDMETADATA.ZMETADATAHASH AS "hash"
                --ZOBJECT.Z_PK AS "table_id"
            FROM ZOBJECT
                  LEFT JOIN
                 ZSTRUCTUREDMETADATA
                 ON ZOBJECT.ZSTRUCTUREDMETADATA = ZSTRUCTUREDMETADATA.Z_PK
              LEFT JOIN
                 ZSOURCE
                 ON ZOBJECT.ZSOURCE = ZSOURCE.Z_PK
        """
        )

        for row in cur:
            event_type = row["type"].lstrip("/").split("/", 1)[0]

            yield {
                "@context": {"@vocab": APPLE_TYPE},
                #
                "@id": APPLE_DATA[f'{row["uuid"]}'],
                "@type": APPLE_TYPE[f"event/{event_type}"],
                #
                "uuid": row["uuid"],
                "startDate": parse_datetime(row["start_time"], "%Y-%m-%d %H:%M:%S"),
                "endDate": parse_datetime(row["end_time"], "%Y-%m-%d %H:%M:%S"),
                "createdTime": parse_datetime(row["created_time"], "%Y-%m-%d %H:%M:%S"),
                #
                "bundle": {
                    "@id": APPLE_DATA[f"bundle/{row['bundle_id']}"],
                    "@type": APPLE_TYPE["Bundle"],
                }
                if row["bundle_id"] is not None
                else None,
                "device": {
                    "@id": APPLE_DATA[f"device/{row['device_id']}"],
                    "@type": APPLE_TYPE["Device"],
                }
                if row["device_id"] is not None
                else None,
            }


def file_hash(filepath, hash_function="sha256"):
    """
    Compute hash of a file using a specified hash function (default is SHA256).

    :param filepath: path to the file
    :param hash_function: name of the hash function (e.g., 'sha256', 'md5')
    :return: hexadecimal hash string of the file
    """
    hash_func = getattr(hashlib, hash_function)()
    with open(filepath, "rb") as f:
        # Read and update hash string value in blocks of 4K
        for byte_block in iter(lambda: f.read(4096), b""):
            hash_func.update(byte_block)
    return f"{hash_function}:{hash_func.hexdigest()}"


def load_data():
    """
    Common customizable part of all parsers, can be wrapped in a class or a decorator.
    discover_files_and_load_data
    """

    graph = Graph()

    start_time = datetime.now()

    # discover and parse files
    glob_pattern = [
        "exports/knowledgeC.db",
        # os.path.expanduser("~/Library/Application Support/Knowledge/knowledgeC.db"),
        # "exports/**/knowledgeC*.db",
    ]
    dependencies = []
    for pattern in glob_pattern:
        for file in glob.glob(pattern, recursive=True):
            print(f"Parsing {file}")
            file_h = file_hash(file)

            records = parse_knowldegeC(file)

            n_records = 0
            for record in records:
                graph.parse(data=record, format="json-ld")
                n_records += 1

            if n_records > 0:
                file_dep = {
                    "@id": MY_DATA[f"file/{file_h}"],
                    "@type": OWNLD_TYPE["File"],
                    "hash": file_h,
                    "absolutePath": os.path.abspath(file),
                    "size": os.path.getsize(file),
                    "createdAt": datetime.fromtimestamp(os.path.getctime(file)),
                    "modifiedAt": datetime.fromtimestamp(os.path.getmtime(file)),
                }
                dependencies.append(file_dep)

    # add graph and source metadata
    graph_uri = APPLE_DATA["graph/apple_knowledge"]
    graph_metadata = {
        "@context": {"@vocab": OWNLD_TYPE},
        "@id": graph_uri,
        "@type": [
            OWNLD_TYPE["Graph"],  # core:Graph — this is a general graph
            APPLE_TYPE["KnowledgeGraph"],  # apple:Graph — Apple-specific data
        ],
        "source": {
            "@type": OWNLD_TYPE["Source"],
            "@id": MY_DATA[f"source/apple_knowledgeC/{datetime.now().isoformat()}"],
            "script": {  # link to a specific script (e.g. commit)
                "@id": APPLE_TYPE["KnowledgeCParser/1.0.0"],
                "@type": OWNLD_TYPE["Parser"],
                "commit": "1234567890abcdef",
            },
            "dependsOn": dependencies,
            "createdAt": start_time,
            "parsedTriples": len(graph),
            "parsingTime": datetime.now() - start_time,
        },
    }
    graph.parse(data=graph_metadata, format="json-ld")

    return graph, graph_uri


def main():
    # read config

    # import data
    graph, graph_uri = load_data()

    # checks and auto-generated schema
    # check_schema(graph)

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


"""

Parser / importer:
- discover_and_parse(config, optional[dataset]) -> (record...)

CLI:
my

----

Discover and parse
1. discover_and_parse(config, dataset) -> (record...)

Produce graphs
1. (records) -> data_graph
2. schema -> schema_graph

Upload or save graphs
1. Update existing graph
2. Upload new graph

----

my generate-schema <script>

Automatic generation of schema and json-ld context
  - test run -> RDFS schema -> knowledge/parser.ttl
  - filling descriptions with LLMs
  - values stats, sanity checks, size estimates
  - try to download the schema from the web; try to find it in knowledge folder

In this script:
    CONTEXT = {
        "@vocab": APPLE_TYPE,
        "ex": "http://example.com/",
    }

When parsing JSON-LD records: make sure that URIRef is used for @id, e.g. by wrapping it with {@id: ...}


"""


"""
graph_metadata_jsonld = {
    "@context": {
        "@vocab": OWNLD,
    },

    "@id": OWNLD_DATA["graph/apple_knowledge"],  # graph:apple_knowledge
    "@type": [
        OWNLD["Graph"],        # core:Graph — this is a general graph
        APPLE_TYPE["Graph"],   # apple:Graph — Apple-specific data
    ],

    # should be inferred from the type
    "name": "Apple KnowledgeC",
    "description": "Apple KnowledgeC database",

    # who owns the data behind the graph
    # maybe instead: creator
    "owner": OWNLD_DATA["me"],

    # additive pieces of the same graph parsed incrementally
    "source": {
        "@id": OWNLD_DATA["source/apple_knowledgeC/<hash>"],
        "@type": OWNLD["Source"],

        # unique identifier of the parser
        # this resource should be created together with the schema
        "script": APPLE_TYPE["KnowledgeCParser/1.0.0"],

        "createdAt": "2020-01-01T00:00:00Z",
        "timeElapsed": 12345,
        "numTriples": 12345,

        "dependsOn": [
            # files that were parsed
            {
                "@type": OWNLD["File"],
                "absolutePath": "/Users/peter/Library/Application Support/Knowledge/knowledgeC.db",
                "hash": "sha256:...",
                "size": 12345,
                "createdAt": "2020-01-01T00:00:00Z",
                "modifiedAt": "2020-01-01T00:00:00Z",
            },
            {
                "@type": OWNLD["File"],
                "absolutePath": "/Users/peter/Downloads/knowledgeC.db",
                "hash": "sha256:...",
                "size": 12345,
                "createdAt": "2020-01-01T00:00:00Z",
                "modifiedAt": "2020-01-01T00:00:00Z",
            },

            # graphs that were used
            {"@id": OWNLD_DATA["graph/apple_health/version_<datetime>_<hash>"]},
            {"@id": OWNLD_DATA["graph/other_graph/version_<datetime>_<hash>"]},
        ],

    }
}
"""


if __name__ == "__main__":
    main()
