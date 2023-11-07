import glob
import hashlib
import json
import zipfile

import dateutil.parser
from rdflib import Graph, URIRef
from rdflib.namespace import XSD, Namespace
from rdflib.term import _is_valid_uri

from mydata.utils import add_records_to_graph

GOOGLE_TYPE = Namespace("https://ownld.org/service/google/")
GOOGLE_DATA = Namespace("mydata://db/service/google/")

context_jsonld = {
    "@vocab": GOOGLE_TYPE,
    "url": {"@type": XSD["anyURI"]},
    "time": {"@type": XSD["dateTime"]},
    "titleUrl": {"@type": "@id"},
    "products": {"@id": "product", "@type": "@id"},
    "label": "http://www.w3.org/2000/01/rdf-schema#label",
}


def parse_activity(record):
    time = dateutil.parser.parse(record["time"])

    # resource URI based on the hash of (time, header, title, titleUrl)
    activity_hash = hashlib.sha256(
        f'{time.strftime("%Y-%m-%dT%H:%M:%S%z")}/{record.get("header")}/{record.get("title")}/{record.get("titleUrl")}'.encode(
            "utf-8"
        )
    ).hexdigest()
    activity_ref = GOOGLE_DATA[f"activity/{activity_hash}"]

    url_ref = None
    if record.get("titleUrl") is not None:
        url = record["titleUrl"].replace(" ", "+")
        if _is_valid_uri(url):
            url_ref = URIRef(url)

    return {
        "@id": activity_ref,
        "@type": "Activity",
        "time": time,
        "header": record.get("header"),
        "title": record.get("title"),
        "titleUrl": url_ref,
        "products": [
            {
                "@id": GOOGLE_TYPE["product/" + product.replace(" ", "+")],
                "@type": "Product",
                "label": product,
            }
            for product in record.get("products", [])
        ],
    }


def parse_takeout_file(zip_archive, file):
    if file.filename.endswith("MyActivity.json") or file.filename.endswith("My Activity.json"):
        print(f"    parsing {file.filename}")
        records = json.load(zip_archive.open(file))
        for record in records:
            yield parse_activity(record)

    if file.filename.endswith("MyActivity.html"):
        pass  # TODO: implement parsing html


def discover_and_parse(graph):
    for takeout_filename in glob.glob("exports/**/takeout-*.zip", recursive=True):
        takeout_zip = zipfile.ZipFile(takeout_filename)
        print(f"Processing {takeout_filename}")
        for file in takeout_zip.filelist:
            add_records_to_graph(graph, context_jsonld, parse_takeout_file(takeout_zip, file))


def main():
    graph = Graph()
    discover_and_parse(graph)
    print(f"Triples: {len(graph)}")

    # Using N-Triples, because Turtle is too slow (likely due to the large number of unique URIs)
    graph.serialize("cache/google_takeout_jsonld.nt", format="nt", encoding="utf-8")


if __name__ == "__main__":
    main()


# TODO implement this parser (sadly google takeout uses html by default)
"""
def parse_html_activity(filename):
    # TODO throw warning suggesting to use the json version instead
    # TODO make sure that URIRef parsed from json and html are the same

    # The code below can be helpful for implementing this parser
    raise NotImplementedError()

    @dataclass
    class Token:
        text: str
        url: str = None

        @property
        def is_link(self):
            return self.url is not None

    @dataclass
    class Entry:
        title: str
        action: str
        timestamp: datetime.datetime
        product: str
        params: List[str]
        urls: List[str]

    time_re = re.compile(r"[A-Za-z]+ [0-9]+, [0-9]{4}, [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}[\w ]+") # noqa: W605

    def clean_text(text):
        return text.replace("\u2003", " ").replace("\xa0", " ").strip()

    def html_to_tokens(element, depth=0):
        if isinstance(element, etree._Element):
            if element.tag == "a":
                yield Token(clean_text(element.text), url=element.attrib["href"])
            else:
                for child in element.xpath("child::node()"):
                    for token in html_to_tokens(child, depth + 1):
                        yield token
        else:
            yield Token(clean_text(element))

    PARAM_PREFIXES = [
        "Viewed area around",
        "Searched for",
        "Watched story from",
    ]

    def fix_params(params):
        params_new = []
        for param in params:
            found_prefix = None
            for prefix in PARAM_PREFIXES:
                if param.startswith(prefix):
                    found_prefix = prefix
                    break
            if found_prefix:
                params_new.append(found_prefix)
                params_new.append(param[len(found_prefix) :].lstrip())
            else:
                params_new.append(param)
        return params_new

    def parse_cell(cell_el):
        tokens = list(html_to_tokens(cell_el.root))

        timestamp = None
        product = None

        timestamp_loc = None
        for i, token in enumerate(tokens):
            if token.text == "Products:" and len(tokens) > i + 1:
                product = tokens[i + 1].text
            if timestamp is None and time_re.match(token.text):
                timestamp = dateutil.parser.parse(token.text)
                timestamp_loc = i

        param_tokens = []
        if timestamp_loc is not None:
            param_tokens = tokens[1:timestamp_loc]
        params = fix_params([token.text for token in param_tokens if len(token.text.strip()) > 0])
        urls = [token.url for token in param_tokens if token.is_link]

        return Entry(
            title=(tokens[0].text if len(tokens) > 0 else None),
            action=(params[0] if len(params) > 0 else None),
            timestamp=timestamp,
            product=product,
            params=params[1:],
            urls=urls,
        )

    def read_entries(takeout_dir="exports"):
        activity_logs = list(Path(takeout_dir).glob("**/My Activity/**/*.html")) + list(
            Path(takeout_dir).glob("**/YouTube and YouTube Music/history/*.html")
        )

        cells = []
        for filename in activity_logs:
            with open(filename) as fin:
                content = fin.read()
            doc = scrapy.Selector(text=content)
            cells.extend([parse_cell(cell_el) for cell_el in tqdm(doc.css("div.outer-cell"), desc=str(filename))])

        return cells

    return []

"""
