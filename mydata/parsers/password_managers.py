"""
Password managers may help to identify accounts that you have forgotten about.
"""
import hashlib
import re
from datetime import datetime

import tldextract
from pathlib import Path
import urllib.parse

from rdflib import Graph, RDF, Namespace, XSD, Literal, URIRef

from mydata.utils import SQLiteConnection


SCHEMA = Namespace('https://schema.org/')
BROWSER = Namespace('https://mydata-schema.org/browser/')
MYDATA = Namespace('mydata://')


def url_to_domain(url):
    parts = urllib.parse.urlparse(url)
    if parts.scheme in ('http', 'https'):
        domain = tldextract.extract(url).registered_domain
        if domain:
            return domain


def is_valid_email(email):
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
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

            url = row['url']
            domain = url_to_domain(url)

            login_hash = hashlib.sha1(f'{url}${row["username"]}'.encode()).hexdigest()
            login_ref = BROWSER[f'login/{domain}/{login_hash}']

            username = row['username']

            if domain is not None and username:
                graph.add((login_ref, RDF.type, BROWSER['WebLogin']))
                graph.add((login_ref, BROWSER['domain'], MYDATA[f'domain/{domain}']))
                graph.add((login_ref, BROWSER['url'], Literal(url, datatype=XSD['anyURI'])))
                graph.add((login_ref, BROWSER['username'], Literal(username)))

                if row['time_last_used'] is not None:
                    graph.add((login_ref, BROWSER['timeLastUsed'], Literal(
                        datetime.strptime(row['time_last_used'], '%Y-%m-%d %H:%M:%S'), datatype=XSD['dateTime'])))
                if row['time_last_modified'] is not None:
                    graph.add((login_ref, BROWSER['timeLastModified'], Literal(
                        datetime.strptime(row['time_last_modified'], '%Y-%m-%d %H:%M:%S'), datatype=XSD['dateTime'])))

                if is_valid_email(username):
                    graph.add((login_ref, BROWSER['user'], URIRef(f'mailto:{username}')))


def prepare_browser_logins():
    graph = Graph()
    graph.bind('rdf', RDF)
    graph.bind('schema', SCHEMA)
    graph.bind('xsd', XSD)

    chrome_db_filename = Path.home() / 'Library/Application Support/Google/Chrome/Default/Login Data'
    parse_chrome_logins(graph, chrome_db_filename)

    graph.serialize('cache/browser_logins.ttl', format='turtle')



if __name__ == '__main__':
    prepare_browser_logins()
