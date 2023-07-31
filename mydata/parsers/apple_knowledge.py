"""
Apple devices keep track of various usage events in knowledgeC.db.

Information about knowldgeC.db:
- http://www.mac4n6.com/blog/2018/8/5/knowledge-is-power-using-the-knowledgecdb-database-on-macos-and-ios-to-determine-precise-user-and-application-usage
- https://github.com/mac4n6/APOLLO/

"""

import pandas
import sqlite3


if __name__ == '__main__':
    con = sqlite3.connect('exports/knowledgeC.db')

    df = pandas.read_sql('''
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
    ''', con)

    df.to_csv('cache/knowledgeC_events.csv', index=False)
