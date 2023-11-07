"""
Password managers may help to identify accounts that you have forgotten about.
"""
import hashlib
import re
import urllib.parse
from pathlib import Path

import tldextract
from rdflib import XSD, Graph, Namespace, URIRef

from mydata.utils import SQLiteConnection, add_records_to_graph, parse_datetime

BROWSER_TYPE = Namespace("https://ownld.org/service/browser/")
BROWSER_DATA = Namespace("mydata://db/service/browser/")

context_jsonld = {
    "@vocab": BROWSER_TYPE,
    "domain": {"@type": "@id"},
    "url": {"@type": XSD["anyURI"]},
    "timeLastUsed": {"@type": XSD["dateTime"]},
    "timeLastModified": {"@type": XSD["dateTime"]},
    "email": {"@type": "@id"},
}


def url_to_domain(url):
    parts = urllib.parse.urlparse(url)
    if parts.scheme in ("http", "https"):
        domain = tldextract.extract(url).registered_domain
        if domain:
            return domain


def is_valid_email(email):
    pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    return re.match(pattern, email) is not None


def parse_chrome_logins(graph, chrome_db_filename):
    with SQLiteConnection(chrome_db_filename) as db:
        rows = db.sql(
            """
            SELECT
                id,
                CASE
                    WHEN date_last_used > 86400000000 THEN
                        datetime(date_last_used / 1000000 + strftime('%s', '1601-01-01'), 'unixepoch', 'localtime')
                    ELSE
                        NULL
                END AS time_last_used,
                CASE
                    WHEN date_password_modified > 86400000000 THEN
                        datetime(date_password_modified / 1000000 + strftime('%s', '1601-01-01'), 'unixepoch', 'localtime')
                    ELSE
                        NULL
                END AS time_last_modified,
                username_value AS username,
                origin_url AS url
            FROM logins
            """,
        )

        for row in rows:
            url = row["url"]
            domain = url_to_domain(url)

            login_hash = hashlib.sha1(f'{url}${row["username"]}'.encode()).hexdigest()
            login_ref = BROWSER_DATA[f"login/{domain}/{login_hash}"]

            username = row["username"]

            if domain is not None and username:
                yield {
                    "@id": login_ref,
                    "@type": "WebLogin",
                    "domain": URIRef(f"domain:{domain}"),
                    "url": url,
                    "username": username,
                    "timeLastUsed": parse_datetime(row["time_last_used"], "%Y-%m-%d %H:%M:%S"),
                    "timeLastModified": parse_datetime(row["time_last_modified"], "%Y-%m-%d %H:%M:%S"),
                    "email": URIRef(f"mailto:{username}") if is_valid_email(username) else None,
                }


def discover_and_parse(graph):
    chrome_db_filename = Path.home() / "Library/Application Support/Google/Chrome/Default/Login Data"
    add_records_to_graph(graph, context_jsonld, parse_chrome_logins(graph, chrome_db_filename))


def prepare_browser_logins():
    graph = Graph()
    discover_and_parse(graph)
    print(f"Triples: {len(graph)}")
    graph.serialize("cache/browser_logins_jsonld.ttl", format="turtle")


if __name__ == "__main__":
    prepare_browser_logins()
