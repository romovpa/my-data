"""
Apple devices keep track of various usage events in knowledgeC.db.

Information about knowldgeC.db:
- http://www.mac4n6.com/blog/2018/8/5/knowledge-is-power-using-the-knowledgecdb-database-on-macos-and-ios-to-determine-precise-user-and-application-usage
- https://github.com/mac4n6/APOLLO/

"""

import glob
import os.path
from datetime import datetime

import requests
from rdflib import Graph
from rdflib.namespace import Namespace

from mydata.utils import SQLiteConnection, get_file_hash, parse_datetime

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
            file_hash = get_file_hash(file)

            records = parse_knowldegeC(file)

            n_records = 0
            for record in records:
                graph.parse(data=record, format="json-ld")
                n_records += 1

            if n_records > 0:
                file_dep = {
                    "@id": MY_DATA[f"file/{file_hash}"],
                    "@type": OWNLD_TYPE["File"],
                    "hash": file_hash,
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
    """
    CLI implementation
    """
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


if __name__ == "__main__":
    main()
