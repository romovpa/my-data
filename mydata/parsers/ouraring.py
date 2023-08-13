"""
Parser for Oura ring data.
"""

import json
import glob
from rdflib import Graph, Literal
from rdflib.namespace import XSD, Namespace, RDF
from dateutil.parser import parse as parse_date


OURA_TYPE = Namespace('https://ownld.org/service/ouraring/')
OURA_DATA = Namespace('mydata://db/service/ouraring/')


def parse_ouraring(graph, data_file):
    with open(data_file) as fin:
        sleep_data = json.load(fin)

    for record in sleep_data['sleep']:
        start_time = parse_date(record['bedtime_start'])
        end_time = parse_date(record['bedtime_end'])

        event_ref = OURA_DATA[f'bedtime/{start_time.isoformat()}']

        graph.add((event_ref, RDF.type, OURA_TYPE.Bedtime))
        graph.add((event_ref, OURA_TYPE.start, Literal(start_time, datatype=XSD.dateTime)))
        graph.add((event_ref, OURA_TYPE.end, Literal(end_time, datatype=XSD.dateTime)))


def main():
    graph = Graph()
    graph.bind('rdf', RDF)
    graph.bind('xsd', XSD)
    graph.bind('own_oura', OURA_TYPE)

    for oura_filename in glob.glob('exports/**/oura_data_*.json', recursive=True):
        print(f'Parsing {oura_filename}')
        parse_ouraring(graph, oura_filename)

    graph.serialize('cache/ouraring.ttl', format='turtle')


def discover_and_parse(graph):
    for oura_filename in glob.glob('exports/**/oura_data_*.json', recursive=True):
        print(f'Parsing {oura_filename}')
        parse_ouraring(graph, oura_filename)


if __name__ == '__main__':
    main()
