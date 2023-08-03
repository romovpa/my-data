"""
Parser for Apple Photos
"""

import sys
from datetime import datetime
import urllib.parse

from rdflib import Graph, Literal
from rdflib.namespace import XSD, Namespace, RDF

try:
    import osxphotos
except ImportError:
    print('osxphotos not installed. Please install it with "pip install osxphotos"')
    sys.exit(1)


SCHEMA = Namespace('https://schema.org/')
APPLE = Namespace('https://mydata-schema.org/apple/')
MYDATA = Namespace('mydata://')


def parse_date(date_str):
    try:
        return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        return None


def prepare_photos(graph):
    photosdb = osxphotos.PhotosDB()

    for photo in photosdb.photos():
        photo_ref = MYDATA[f'apple/photos/{photo.uuid}']
        graph.add((photo_ref, RDF.type, APPLE.Photo))
        graph.add((photo_ref, RDF.type, SCHEMA.ImageObject))
        graph.add((photo_ref, APPLE.uuid, Literal(photo.uuid)))
        graph.add((photo_ref, SCHEMA.name, Literal(photo.original_filename)))
        if photo.date is not None:
            graph.add((photo_ref, SCHEMA.dateCreated, Literal(photo.date, datatype=XSD.dateTime)))
        if photo.date_modified is not None:
            graph.add((photo_ref, SCHEMA.dateModified, Literal(photo.date_modified, datatype=XSD.dateTime)))
        if photo.date_added is not None:
            graph.add((photo_ref, SCHEMA.dateAdded, Literal(photo.date_added, datatype=XSD.dateTime)))
        if photo.title is not None:
            graph.add((photo_ref, SCHEMA.title, Literal(photo.title)))
        if photo.description is not None:
            graph.add((photo_ref, SCHEMA.description, Literal(photo.description)))
        graph.add((photo_ref, SCHEMA.width, Literal(photo.width, datatype=XSD.integer)))
        graph.add((photo_ref, SCHEMA.height, Literal(photo.height, datatype=XSD.integer)))
        graph.add((photo_ref, APPLE.isScreenshot, Literal(photo.screenshot, datatype=XSD.boolean)))
        graph.add((photo_ref, APPLE.isPortrait, Literal(photo.portrait, datatype=XSD.boolean)))
        for keyword in photo.keywords:
            graph.add((photo_ref, SCHEMA.keywords, MYDATA[f'apple/photos/keyword/{urllib.parse.quote(keyword)}']))
        for album in photo.albums:
            graph.add((photo_ref, SCHEMA.album, MYDATA[f'apple/photos/album/{urllib.parse.quote(album)}']))
        for person in photo.persons:
            graph.add((photo_ref, SCHEMA.person, MYDATA[f'apple/photos/person/{urllib.parse.quote(person)}']))


def main():
    graph = Graph()
    graph.bind('rdf', RDF)
    graph.bind('schema', SCHEMA)
    graph.bind('xsd', XSD)
    graph.bind('apple', APPLE)

    prepare_photos(graph)

    print(f'Triples: {len(graph)}')

    graph.serialize('cache/apple_photos.ttl', format='turtle')


if __name__ == '__main__':
    main()
