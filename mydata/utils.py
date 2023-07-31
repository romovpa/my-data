import csv
import re

from IPython.display import HTML
import jinja2
import itertools
import rdflib
import rdflib.plugins.sparql.processor
from rdflib.term import URIRef, Literal


class CSVWriter:
    def __init__(self, filename):
        self.csvfile = open(filename, "w")
        self.writer = None

    def writerow(self, row):
        if self.writer is None:
            self.fields = list(row.keys())
            self.writer = csv.DictWriter(self.csvfile, fieldnames=self.fields)
            self.writer.writeheader()
        self.writer.writerow(row)

    def close(self):
        self.csvfile.close()


def rdf_table(records, header=None, limit=100, graph=None):
    """Make a displayable table from SPAQRL query results or list of triples."""

    if (
        isinstance(records, rdflib.plugins.sparql.processor.SPARQLResult)
        and header is None
    ):
        header = records.vars
    namespace_manager = graph.namespace_manager if graph is not None else None

    records = itertools.islice(records, limit)

    template = """
    <table>
        {% if header is not none %}
        <tr>
            {% for col in header %}
            <th>{{ col }}</th>
            {% endfor %}
        </tr>
        {% endif %}

        {% for row in records %}
        <tr>
            {% for col in row %}
            <td>
                {% if col is none %}
                <span title="nil">&nbsp;&nbsp;&nbsp;</span>
                {% elif col|is_uri %}
                <span title="{{ col }}">{{ col|n3_notation|e }}</span>
                {% elif col|is_literal %}
                <span title="{{ col.datatype }}">{{ col.value }}</span>
                {% else %}
                {{ col }}
                {% endif %}
            </td>
            {% endfor %}
        </tr>
        {% endfor %}
    </table>
    """

    env = jinja2.Environment(loader=jinja2.BaseLoader())
    env.filters["is_uri"] = lambda obj: isinstance(obj, URIRef)
    env.filters["is_literal"] = lambda obj: isinstance(obj, Literal)
    env.filters["n3_notation"] = lambda obj: obj.n3(namespace_manager)

    template = env.from_string(template)
    rendered = template.render(**locals())

    return HTML(rendered)
