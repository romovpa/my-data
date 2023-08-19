"""
Parse web browser events from Chrome and Safari history databases.
"""
import glob
import hashlib
from datetime import datetime
from pathlib import Path
from sqlite3 import OperationalError
from urllib.parse import quote

from rdflib import Graph, Literal
from rdflib.namespace import RDF, XSD, Namespace

from mydata.utils import SQLiteConnection

BROWSER_TYPE = Namespace("https://ownld.org/service/browser/")
BROWSER_DATA = Namespace("mydata://db/service/browser/")


def parse_web_history(graph, db_file, sql, browser=None, has_duration=False):
    print(f"Parsing {db_file}")

    profile = Path(db_file).parts[-2]
    profile_ref = BROWSER_DATA[f"profile/{quote(profile)}"]

    with SQLiteConnection(db_file) as db:
        try:
            cur = db.sql(sql)
        except OperationalError:
            print(f"Error parsing {db_file}")
            return

        for row in cur:
            time = datetime.strptime(row["time"], "%Y-%m-%d %H:%M:%S")
            unixtime = (time - datetime(1970, 1, 1)).total_seconds()

            url = row["url"]
            url_hash = hashlib.sha1(url.encode()).hexdigest()

            visit_ref = BROWSER_DATA[f"visit/{unixtime}/{url_hash}"]

            # TODO add who is actor (who is using the browser)
            graph.add((visit_ref, RDF.type, BROWSER_TYPE["WebVisit"]))
            graph.add((visit_ref, BROWSER_TYPE["url"], Literal(url, datatype=XSD["anyURI"])))
            graph.add((visit_ref, BROWSER_TYPE["time"], Literal(time, datatype=XSD["dateTime"])))
            if row["title"] is not None:
                graph.add((visit_ref, BROWSER_TYPE["title"], Literal(row["title"])))
            if browser is not None:
                graph.add((visit_ref, BROWSER_TYPE["browser"], Literal(browser)))
            if has_duration and row["duration"] is not None:
                graph.add((visit_ref, BROWSER_TYPE["duration"], Literal(row["duration"], datatype=XSD["float"])))
            graph.add((visit_ref, BROWSER_TYPE["profile"], profile_ref))


def parse_chrome_history(graph, db_file):
    parse_web_history(
        graph,
        db_file,
        """
            SELECT
                DATETIME((visit_time/1000000)-11644473600, 'unixepoch', 'localtime') AS time,
                CAST(visit_duration AS FLOAT)/1000000 AS duration,
                urls.url AS url,
                urls.title AS title
            FROM visits
            LEFT JOIN urls ON visits.url = urls.id
        """,
        browser="Chrome",
        has_duration=True,
    )


def parse_safari_history(graph, db_file):
    parse_web_history(
        graph,
        db_file,
        """
            SELECT
                DATETIME(visit_time + 978307200, 'unixepoch', 'localtime') AS time,
                url AS url,
                title AS title
            FROM history_items
            LEFT JOIN history_visits ON history_items.id = history_visits.history_item
        """,
        browser="Safari",
    )


def prepare_web_events():
    # TODO add hardware device identifiers to know from which device it was collected
    graph = Graph()
    graph.bind("rdf", RDF)
    graph.bind("xsd", XSD)
    graph.bind("own_browser", BROWSER_TYPE)

    # Scan all db files in the exports directory and in the macOS default locations
    chrome_dir_path = Path.home() / "Library/Application Support/Google/Chrome"
    for path in glob.glob(str(chrome_dir_path / "*/History")):
        parse_chrome_history(graph, path)
    for path in glob.glob("exports/**/Chrome_History*.db", recursive=True):
        parse_chrome_history(graph, path)

    safari_history_path = Path.home() / "Library/Safari/History.db"
    if safari_history_path.exists():
        parse_safari_history(graph, Path.home() / "Library/Safari/History.db")
    for path in glob.glob("exports/**/Safari_History*.db", recursive=True):
        parse_safari_history(graph, path)

    print(f"Triples: {len(graph)}")

    # Using N-Triples, because Turtle is too slow (likely due to the large number of unique URIs)
    graph.serialize("cache/web_events.nt", format="nt", encoding="utf-8")


def discover_and_parse(graph):
    chrome_history_path = Path.home() / "Library/Application Support/Google/Chrome/Default/History"
    if chrome_history_path.exists():
        parse_chrome_history(graph, Path.home() / "Library/Application Support/Google/Chrome/Default/History")
    for path in glob.glob("exports/**/Chrome_History*.db", recursive=True):
        parse_chrome_history(graph, path)

    safari_history_path = Path.home() / "Library/Safari/History.db"
    if safari_history_path.exists():
        parse_safari_history(graph, Path.home() / "Library/Safari/History.db")
    for path in glob.glob("exports/**/Safari_History*.db", recursive=True):
        parse_safari_history(graph, path)


if __name__ == "__main__":
    prepare_web_events()
