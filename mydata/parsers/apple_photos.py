"""
Parser for Apple Photos
"""

import sys
import urllib.parse

from rdflib import Graph
from rdflib.namespace import Namespace

try:
    import osxphotos
except ImportError:
    print('osxphotos not installed. Please install it with "pip install osxphotos"')
    sys.exit(1)


APPLE_TYPE = Namespace("https://ownld.org/service/apple/")
APPLE_DATA = Namespace("mydata://db/service/apple/")


context_jsonld = {
    "xsd": "http://www.w3.org/2001/XMLSchema#",
    "@vocab": "https://ownld.org/service/apple/",
    "dateCreated": {
        "@id": "dateCreated",
        "@type": "xsd:dateTime",
    },
    "dateModified": {
        "@id": "dateModified",
        "@type": "xsd:dateTime",
    },
    "dateAdded": {
        "@id": "dateAdded",
        "@type": "xsd:dateTime",
    },
    "keywords": {
        "@id": "keyword",
        "@type": "@id",
    },
    "albums": {
        "@id": "album",
        "@type": "@id",
    },
    "persons": {
        "@id": "person",
        "@type": "@id",
    },
    "label": "http://www.w3.org/2000/01/rdf-schema#label",
}


def photos_to_jsonld():
    photos = osxphotos.PhotosDB().photos()

    for photo in photos:
        yield {
            "@id": APPLE_DATA[f"photos/photo/{photo.uuid}"],
            "@type": "Photo",
            "uuid": photo.uuid,
            "name": photo.original_filename,
            "dateCreated": photo.date if photo.date is not None else None,
            "dateModified": photo.date_modified if photo.date_modified is not None else None,
            "dateAdded": photo.date_added if photo.date_added is not None else None,
            "title": photo.title,
            "description": photo.description,
            "width": photo.width,
            "height": photo.height,
            "isScreenshot": photo.screenshot,
            "isPortrait": photo.portrait,
            "keywords": [
                {
                    "@id": APPLE_DATA[f"photos/keyword/{urllib.parse.quote(keyword)}"],
                    "@type": "Keyword",
                    "label": keyword,
                }
                for keyword in photo.keywords
            ],
            "albums": [
                {
                    "@id": APPLE_DATA[f"photos/album/{urllib.parse.quote(album)}"],
                    "@type": "Album",
                    "label": album,
                }
                for album in photo.albums
            ],
            "persons": [
                {
                    "@id": APPLE_DATA[f"photos/person/{urllib.parse.quote(person)}"],
                    "@type": "Person",
                    "label": person,
                }
                for person in photo.persons
            ],
        }


def discover_and_parse(graph):
    photos_to_jsonld(graph)


def main():
    graph = Graph()

    photos = photos_to_jsonld()
    for photo in photos:
        graph.parse(data=photo, format="json-ld", context=context_jsonld)

    print(f"Triples: {len(graph)}")

    graph.serialize("cache/apple_photos_jsonld.ttl", format="turtle")


if __name__ == "__main__":
    main()
