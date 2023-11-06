"""
Apple devices keep track of various usage events in knowledgeC.db.

Information about knowldgeC.db:
- http://www.mac4n6.com/blog/2018/8/5/knowledge-is-power-using-the-knowledgecdb-database-on-macos-and-ios-to-determine-precise-user-and-application-usage
- https://github.com/mac4n6/APOLLO/

"""

import glob
from pathlib import Path

from rdflib import Graph
from rdflib.namespace import Namespace

from mydata.utils import SQLiteConnection, parse_datetime

APPLE_TYPE = Namespace("https://ownld.org/service/apple/")
APPLE_DATA = Namespace("mydata://db/service/apple/")


context_jsonld = {
    "@vocab": APPLE_TYPE,
    "bundle": {"@type": "@id"},
    "device": {"@type": "@id"},
    "startDate": {"@type": "xsd:dateTime"},
    "endDate": {"@type": "xsd:dateTime"},
    "createdTime": {"@type": "xsd:dateTime"},
}


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

            record = {
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

            yield record


def discover_and_parse(graph):
    default_knowledgeC_path = Path.home() / "Library/Application Support/Knowledge/knowledgeC.db"
    if default_knowledgeC_path.exists():
        print(f"Parsing {default_knowledgeC_path}")
        for record in parse_knowldegeC(default_knowledgeC_path):
            graph.parse(data=record, format="json-ld", context=context_jsonld)

    for db_filename in glob.glob("exports/**/knowledgeC*.db", recursive=True):
        print(f"Parsing {db_filename}")
        for record in parse_knowldegeC(db_filename):
            graph.parse(data=record, format="json-ld", context=context_jsonld)


def main():
    graph = Graph()
    discover_and_parse(graph)
    print(f"Triples: {len(graph)}")
    graph.serialize("cache/apple_knowledgeC_jsonld.ttl", format="turtle")


if __name__ == "__main__":
    main()
