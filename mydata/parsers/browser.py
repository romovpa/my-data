"""
Parse web browser events from Chrome and Safari history databases.
"""
import glob
import hashlib
from datetime import datetime
from pathlib import Path
from sqlite3 import OperationalError
from urllib.parse import quote

from rdflib import Graph
from rdflib.namespace import XSD, Namespace

from mydata.utils import SQLiteConnection, add_records_to_graph, parse_datetime

BROWSER_TYPE = Namespace("https://ownld.org/service/browser/")
BROWSER_DATA = Namespace("mydata://db/service/browser/")


context_jsonld = {
    "@vocab": BROWSER_TYPE,
    "url": {"@type": XSD["anyURI"]},
    "time": {"@type": XSD["dateTime"]},
    "duration": {"@type": XSD["float"]},
}


def parse_web_history(db_file, sql, browser=None, has_duration=False):
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
            time = parse_datetime(row["time"], "%Y-%m-%d %H:%M:%S")
            unixtime = int((time - datetime(1970, 1, 1)).total_seconds())
            url = row["url"]
            url_hash = hashlib.sha1(url.encode()).hexdigest()
            visit_ref = BROWSER_DATA[f"visit/{unixtime}/{url_hash}"]

            # generate JSON-LD record
            yield {
                "@id": visit_ref,
                "@type": "WebVisit",
                "url": url,
                "time": time,
                "title": row["title"],
                "browser": browser,
                "duration": row["duration"] if has_duration else None,
                "profile": {
                    "@id": profile_ref,
                    "@type": "Profile",
                },
            }


def parse_chrome_history(db_file):
    return parse_web_history(
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


def parse_safari_history(db_file):
    return parse_web_history(
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


def discover_and_parse(graph):
    # Scan all db files in the exports directory and in the macOS default locations
    chrome_dir_path = Path.home() / "Library/Application Support/Google/Chrome"
    for path in glob.glob(str(chrome_dir_path / "*/History")):
        add_records_to_graph(graph, context_jsonld, parse_chrome_history(path))
    for path in glob.glob("exports/**/Chrome_History*.db", recursive=True):
        add_records_to_graph(graph, context_jsonld, parse_chrome_history(path))

    safari_history_path = Path.home() / "Library/Safari/History.db"
    if safari_history_path.exists():
        add_records_to_graph(graph, context_jsonld, parse_safari_history(Path.home() / "Library/Safari/History.db"))
    for path in glob.glob("exports/**/Safari_History*.db", recursive=True):
        add_records_to_graph(graph, context_jsonld, parse_safari_history(path))


def main():
    # TODO add hardware device identifiers to know from which device it was collected
    graph = Graph()
    discover_and_parse(graph)
    print(f"Triples: {len(graph)}")

    # Using N-Triples, because Turtle is too slow (likely due to the large number of unique URIs)
    graph.serialize("cache/web_events_jsonld.nt", format="nt", encoding="utf-8")


if __name__ == "__main__":
    main()
