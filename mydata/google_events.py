import datetime
import glob
import os
import re
from dataclasses import dataclass
from typing import List

import dateutil.parser
import scrapy
from lxml import etree
from tqdm import tqdm


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


time_re = re.compile(r'[A-Za-z]+ [0-9]+, [0-9]{4}, [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}[\w ]+')


def clean_text(text):
    return text.replace(u'\u2003', ' ').replace(u'\xa0', ' ').strip()


def html_to_tokens(element, depth=0):
    if isinstance(element, etree._Element):
        if element.tag == 'a':
            yield Token(clean_text(element.text), url=element.attrib['href'])
        else:
            for child in element.xpath("child::node()"):
                for token in html_to_tokens(child, depth + 1):
                    yield token
    else:
        yield Token(clean_text(element))


PARAM_PREFIXES = [
    'Viewed area around',
    'Searched for',
    'Watched story from',
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
            params_new.append(param[len(found_prefix):].lstrip())
        else:
            params_new.append(param)
    return params_new


def parse_cell(cell_el):
    tokens = list(html_to_tokens(cell_el.root))

    timestamp = None
    product = None

    timestamp_loc = None
    for i, token in enumerate(tokens):
        if token.text == 'Products:' and len(tokens) > i + 1:
            product = tokens[i + 1].text
        if timestamp is None and time_re.match(token.text):
            timestamp = dateutil.parser.parse(token.text)
            timestamp_loc = i

    param_tokens = []
    if timestamp_loc is not None:
        param_tokens = tokens[1:timestamp_loc]
    params = fix_params([
        token.text
        for token in param_tokens
        if len(token.text.strip()) > 0
    ])
    urls = [
        token.url
        for token in param_tokens
        if token.is_link
    ]

    return Entry(
        title=(tokens[0].text if len(tokens) > 0 else None),
        action=(params[0] if len(params) > 0 else None),
        timestamp=timestamp,
        product=product,
        params=params[1:],
        urls=urls,
    )


def read_entries(takeout_dir):
    activity_logs = (
            glob.glob(str(Path(takeout_dir)/'**/My Activity/**/*.html')) +
            glob.glob(str(Path(takeout_dir)/'**/YouTube and YouTube Music/history/*.html'))
    )

    cells = []
    for filename in activity_logs:
        with open(os.path.join(takeout_dir, filename)) as fin:
            content = fin.read()
        doc = scrapy.Selector(text=content)
        cells.extend([
            parse_cell(cell_el)
            for cell_el in tqdm(doc.css('div.outer-cell'), desc=filename)
        ])

    return cells
