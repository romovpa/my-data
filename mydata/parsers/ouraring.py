"""
Parser for Oura ring data.
"""

import json
import glob
from pathlib import Path
from datetime import datetime
from tqdm import tqdm
from rdflib import Graph, Literal
from rdflib.namespace import XSD, Namespace, RDF
from dateutil.parser import parse as parse_date


SCHEMA = Namespace('https://schema.org/')
OURA = Namespace('https://mydata-schema.org/ouraring/')
MYDATA = Namespace('mydata://')


def parse_ouraring(graph, data_file):
    with open(data_file) as fin:
        sleep_data = json.load(fin)

    events = []

    for record in sleep_data['sleep']:
        start_time = parse_date(record['bedtime_start'])
        end_time = parse_date(record['bedtime_end'])

        event_ref = MYDATA[f'ouraring/bedtime/{start_time.isoformat()}']

        graph.add((event_ref, RDF.type, OURA.Bedtime))
        graph.add((event_ref, OURA.start, Literal(start_time, datatype=XSD.dateTime)))
        graph.add((event_ref, OURA.end, Literal(end_time, datatype=XSD.dateTime)))


def main():
    graph = Graph()
    graph.bind('rdf', RDF)
    graph.bind('schema', SCHEMA)
    graph.bind('xsd', XSD)

    for oura_filename in glob.glob('exports/**/oura_data_*.json', recursive=True):
        print(f'Parsing {oura_filename}')
        parse_ouraring(graph, oura_filename)

    graph.serialize('cache/ouraring.ttl', format='turtle')


if __name__ == '__main__':
    main()
