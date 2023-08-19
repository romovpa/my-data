import csv
import itertools
import shutil
import sqlite3
from tempfile import TemporaryDirectory

import jinja2
import rdflib
from IPython.display import HTML
from rdflib.term import Literal, URIRef


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

    if isinstance(records, rdflib.query.Result) and header is None:
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


class SQLiteConnection:
    """
    Makes a temporary copy of a SQLite database to overcome the locking issue and sets the row factory.
    """

    def __init__(self, db_file):
        self.db_file = db_file
        self.temp_dir = TemporaryDirectory()
        self.conn = None

    def __enter__(self):
        # Create a temporary copy of the database file
        temp_db_path = shutil.copy(self.db_file, self.temp_dir.name)

        # Connect to the temporary database
        self.conn = sqlite3.connect(temp_db_path)
        self.conn.row_factory = sqlite3.Row

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # Close the connection
        if self.conn:
            self.conn.close()

        # Cleanup the temporary directory
        self.temp_dir.cleanup()

    def sql(self, sql_query):
        # Execute the given SQL query and return the rows
        cursor = self.conn.cursor()
        cursor.execute(sql_query)
        return cursor
