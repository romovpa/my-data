"""
Apple devices keep track of various usage events in knowledgeC.db.

Information about knowldgeC.db:
- http://www.mac4n6.com/blog/2018/8/5/knowledge-is-power-using-the-knowledgecdb-database-on-macos-and-ios-to-determine-precise-user-and-application-usage
- https://github.com/mac4n6/APOLLO/

"""

import glob
from datetime import datetime
from pathlib import Path

from rdflib import Graph, Literal
from rdflib.namespace import XSD, Namespace, RDF

from mydata.utils import SQLiteConnection


APPLE_TYPE = Namespace('https://ownld.org/service/apple/')
APPLE_DATA = Namespace('mydata://db/service/apple/')


def parse_date(date_str):
    try:
        return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        return None


def parse_knowldegeC(graph, db_file):
    with SQLiteConnection(db_file) as db:
        cur = db.sql("""
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
        """)

        for row in cur:
            event_ref = APPLE_DATA[f'{row["uuid"]}']
            event_type = row["type"].lstrip("/").split('/', 1)[0]

            graph.add((event_ref, RDF.type, APPLE_TYPE[f'event/{event_type}']))
            graph.add((event_ref, APPLE_TYPE.uuid, Literal(row['uuid'], datatype=XSD.string)))
            graph.add((event_ref, APPLE_TYPE.bundleId, APPLE_DATA[f"bundle/{row['bundle_id']}"]))
            graph.add((event_ref, APPLE_TYPE.deviceId, APPLE_DATA[f"device/{row['device_id']}"]))

            start_date = parse_date(row['start_time'])
            if start_date:
                graph.add((event_ref, APPLE_TYPE.startDate, Literal(start_date, datatype=XSD.dateTime)))

            end_date = parse_date(row['end_time'])
            if end_date:
                graph.add((event_ref, APPLE_TYPE.endDate, Literal(end_date, datatype=XSD.dateTime)))

            created_date = parse_date(row['created_time'])
            if created_date:
                graph.add((event_ref, APPLE_TYPE.createdTime, Literal(created_date, datatype=XSD.dateTime)))


def discover_and_parse(graph):
    default_knowledgeC_path = Path.home() / 'Library/Application Support/Knowledge/knowledgeC.db'
    if default_knowledgeC_path.exists():
        parse_knowldegeC(graph, default_knowledgeC_path)
    for db_filename in glob.glob('exports/**/knowledgeC*.db', recursive=True):
        parse_knowldegeC(graph, db_filename)


def main():
    graph = Graph()
    graph.bind('rdf', RDF)
    graph.bind('xsd', XSD)
    graph.bind('own_apple', APPLE_TYPE)

    default_knowledgeC_path = Path.home() / 'Library/Application Support/Knowledge/knowledgeC.db'
    if default_knowledgeC_path.exists():
        print(f'Parsing {default_knowledgeC_path}')
        parse_knowldegeC(graph, default_knowledgeC_path)

    for db_filename in glob.glob('exports/**/knowledgeC*.db', recursive=True):
        print(f'Parsing {db_filename}')
        parse_knowldegeC(graph, db_filename)

    print(f'Triples: {len(graph)}')

    graph.serialize('cache/apple_knowledgeC.ttl', format='turtle')


if __name__ == '__main__':
    main()
