"""
Parse web browser events from Chrome and Safari history databases.
"""
import glob
import shutil
from datetime import datetime
import hashlib
import sqlite3
from pathlib import Path
from tempfile import TemporaryDirectory

from rdflib import Graph, Literal
from rdflib.namespace import XSD, Namespace, RDF


SCHEMA = Namespace('https://schema.org/')
BROWSER = Namespace('https://mydata-schema.org/browser/')
MYDATA = Namespace('mydata://')


def parse_web_history(graph, db_file, sql, browser=None, has_duration=False):
    print(f'Parsing {db_file}')

    with TemporaryDirectory() as tmpdir:
        shutil.copyfile(db_file, Path(tmpdir) / 'db')

        conn = sqlite3.connect(Path(tmpdir) / 'db')
        conn.row_factory = sqlite3.Row

        try:
            cur = conn.execute(sql)
        except sqlite3.OperationalError:
            print(f'Failed to parse {db_file}')
            shutil.rmtree(tmpdir)
            return

        for row in cur:
            time = datetime.strptime(row['time'], '%Y-%m-%d %H:%M:%S')
            unixtime = (time - datetime(1970, 1, 1)).total_seconds()

            url = row['url']
            url_hash = hashlib.sha1(url.encode()).hexdigest()

            visit_ref = MYDATA[f'browser/visit/{unixtime}/{url_hash}']

            # TODO add who is actor (who is using the browser)
            graph.add((visit_ref, RDF.type, MYDATA['WebVisit']))
            graph.add((visit_ref, MYDATA['url'], Literal(url, datatype=XSD['anyURI'])))
            graph.add((visit_ref, MYDATA['time'], Literal(time, datatype=XSD['dateTime'])))
            if row['title'] is not None:
                graph.add((visit_ref, MYDATA['title'], Literal(row['title'])))
            if browser is not None:
                graph.add((visit_ref, MYDATA['browser'], Literal(browser)))
            if has_duration and row['duration'] is not None:
                graph.add((visit_ref, MYDATA['duration'], Literal(row['duration'], datatype=XSD['float'])))

        shutil.rmtree(tmpdir)


def parse_chrome_history(graph, db_file):
    parse_web_history(
        graph,
        db_file,
        '''
            SELECT 
                DATETIME((visit_time/1000000)-11644473600, 'unixepoch', 'localtime') AS time,
                CAST(visit_duration AS FLOAT)/1000000 AS duration,
                urls.url AS url,
                urls.title AS title
            FROM visits
            LEFT JOIN urls ON visits.url = urls.id
        ''',
        browser='Chrome',
        has_duration=True
    )


def parse_safari_history(graph, db_file):
    parse_web_history(
        graph,
        db_file,
        '''
            SELECT 
                DATETIME(visit_time + 978307200, 'unixepoch', 'localtime') AS time, 
                url AS url,
                title AS title
            FROM history_items 
            LEFT JOIN history_visits ON history_items.id = history_visits.history_item
        ''',
        browser='Safari',
    )


def prepare_web_events():
    graph = Graph()
    graph.bind('rdf', RDF)
    graph.bind('schema', SCHEMA)
    graph.bind('xsd', XSD)

    # Scan all db files in the exports directory and in the Mac OS default locations

    chrome_history_path = Path.home() / 'Library/Application Support/Google/Chrome/Default/History'
    if chrome_history_path.exists():
        parse_chrome_history(graph, Path.home() / 'Library/Application Support/Google/Chrome/Default/History')
    for path in glob.glob('exports/**/Chrome_History*.db', recursive=True):
        parse_chrome_history(graph, path)

    safari_history_path = Path.home() / 'Library/Safari/History.db'
    if safari_history_path.exists():
        parse_safari_history(graph, Path.home() / 'Library/Safari/History.db')
    for path in glob.glob('exports/**/Safari_History*.db', recursive=True):
        parse_safari_history(graph, path)

    print(f'Triples: {len(graph)}')

    # Using N-Triples, because Turtle is too slow (likely due to the large number of unique URIs)
    graph.serialize('cache/web_events.nt', format='nt', encoding='utf-8')


if __name__ == '__main__':
    prepare_web_events()