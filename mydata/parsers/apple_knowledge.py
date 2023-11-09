"""
Apple devices keep track of various usage events in knowledgeC.db.

Information about knowldgeC.db:
- http://www.mac4n6.com/blog/2018/8/5/knowledge-is-power-using-the-knowledgecdb-database-on-macos-and-ios-to-determine-precise-user-and-application-usage
- https://github.com/mac4n6/APOLLO/

"""

from mydata.api import DiscoverAndParse
from mydata.namespace import MY, OWNLD
from mydata.utils import SQLiteConnection, parse_datetime


class AppleKnowledgeCParser(DiscoverAndParse):
    # discovery
    glob_pattern = [
        "exports/knowledgeC.db",
        # os.path.expanduser("~/Library/Application Support/Knowledge/knowledgeC.db"),
        # "exports/**/knowledgeC*.db",
    ]

    # graph node
    graph_uri = MY["apple/knowledge"]
    graph_types = [
        OWNLD["apple#KnowledgeGraph"],
    ]

    # script metadata
    script_record = {
        "@id": OWNLD["apple/knowledge/parser/v1"],
        "@type": OWNLD["core#Parser"],
        "commit": "1234567890abcdef",
    }

    def parse_file(self, file):
        with SQLiteConnection(file) as db:
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
                    "@context": {"@vocab": OWNLD["apple#"]},
                    #
                    "@id": MY[f"apple/knowledge/{row['uuid']}"],
                    "@type": OWNLD[f"apple/{event_type}"],
                    #
                    "uuid": row["uuid"],
                    "startDate": parse_datetime(row["start_time"], "%Y-%m-%d %H:%M:%S"),
                    "endDate": parse_datetime(row["end_time"], "%Y-%m-%d %H:%M:%S"),
                    "createdTime": parse_datetime(row["created_time"], "%Y-%m-%d %H:%M:%S"),
                    #
                    "bundle": {
                        "@id": OWNLD[f"apple/bundle/{row['bundle_id']}"],
                        "@type": OWNLD["apple#Bundle"],
                    }
                    if row["bundle_id"] is not None
                    else None,
                    "device": {
                        "@id": MY[f"apple/device/{row['device_id']}"],
                        "@type": OWNLD["apple#Device"],
                    }
                    if row["device_id"] is not None
                    else None,
                }
