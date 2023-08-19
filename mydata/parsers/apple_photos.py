"""
Parser for Apple Photos
"""

import sys
import urllib.parse
from datetime import datetime

from rdflib import Graph, Literal
from rdflib.namespace import RDF, XSD, Namespace

try:
    import osxphotos
except ImportError:
    print('osxphotos not installed. Please install it with "pip install osxphotos"')
    sys.exit(1)


APPLE_TYPE = Namespace("https://ownld.org/service/apple/")
APPLE_DATA = Namespace("mydata://db/service/apple/")


def parse_date(date_str):
    try:
        return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def prepare_photos(graph):
    photosdb = osxphotos.PhotosDB()

    for photo in photosdb.photos():
        photo_ref = APPLE_DATA[f"photos/photo/{photo.uuid}"]
        graph.add((photo_ref, RDF.type, APPLE_TYPE.Photo))
        graph.add((photo_ref, APPLE_TYPE.uuid, Literal(photo.uuid)))
        graph.add((photo_ref, APPLE_TYPE.name, Literal(photo.original_filename)))
        if photo.date is not None:
            graph.add((photo_ref, APPLE_TYPE.dateCreated, Literal(photo.date, datatype=XSD.dateTime)))
        if photo.date_modified is not None:
            graph.add((photo_ref, APPLE_TYPE.dateModified, Literal(photo.date_modified, datatype=XSD.dateTime)))
        if photo.date_added is not None:
            graph.add((photo_ref, APPLE_TYPE.dateAdded, Literal(photo.date_added, datatype=XSD.dateTime)))
        if photo.title is not None:
            graph.add((photo_ref, APPLE_TYPE.title, Literal(photo.title)))
        if photo.description is not None:
            graph.add((photo_ref, APPLE_TYPE.description, Literal(photo.description)))
        graph.add((photo_ref, APPLE_TYPE.width, Literal(photo.width, datatype=XSD.integer)))
        graph.add((photo_ref, APPLE_TYPE.height, Literal(photo.height, datatype=XSD.integer)))
        graph.add((photo_ref, APPLE_TYPE.isScreenshot, Literal(photo.screenshot, datatype=XSD.boolean)))
        graph.add((photo_ref, APPLE_TYPE.isPortrait, Literal(photo.portrait, datatype=XSD.boolean)))
        for keyword in photo.keywords:
            graph.add((photo_ref, APPLE_TYPE.keywords, APPLE_DATA[f"photos/keyword/{urllib.parse.quote(keyword)}"]))
        for album in photo.albums:
            graph.add((photo_ref, APPLE_TYPE.album, APPLE_DATA[f"photos/album/{urllib.parse.quote(album)}"]))
        for person in photo.persons:
            graph.add((photo_ref, APPLE_TYPE.person, APPLE_DATA[f"photos/person/{urllib.parse.quote(person)}"]))


def discover_and_parse(graph):
    prepare_photos(graph)


def main():
    graph = Graph()
    graph.bind("rdf", RDF)
    graph.bind("xsd", XSD)
    graph.bind("own_apple", APPLE_TYPE)

    prepare_photos(graph)

    print(f"Triples: {len(graph)}")

    graph.serialize("cache/apple_photos.ttl", format="turtle")


if __name__ == "__main__":
    main()
