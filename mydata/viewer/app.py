"""
Minimalistic RDF viewer

Running:
$ flask --app mydata.viewer:app run -h localhost -p 4999  --debug

Then open
http://localhost:4999/
"""

from typing import Union
import itertools

import jinja2
import rdflib
from flask import Flask, render_template
from rdflib import URIRef, Literal, Graph
from rdflib.plugins.stores.sparqlstore import SPARQLStore

app = Flask(__name__)

store = SPARQLStore('http://localhost:3030/mydata/sparql')
graph = rdflib.ConjunctiveGraph(store)

schema = Graph().parse('schema_standard.ttl')

namespace_manager = graph.namespace_manager if graph is not None else None
app.jinja_env.filters["is_uri"] = lambda obj: isinstance(obj, URIRef)
app.jinja_env.filters["is_literal"] = lambda obj: isinstance(obj, Literal)
app.jinja_env.filters["n3_notation"] = lambda obj: obj.n3(namespace_manager)


def get_label(uri: rdflib.URIRef):
    label = schema.value(
        uri,
        URIRef('http://www.w3.org/2000/01/rdf-schema#label')
    )
    if label is not None:
        return label.value
    else:
        return uri.n3(namespace_manager)


def get_description(uri: rdflib.URIRef):
    comment = schema.value(
        uri,
        URIRef('http://www.w3.org/2000/01/rdf-schema#comment')
    )
    if comment is not None:
        return comment.value


app.jinja_env.filters["label"] = get_label
app.jinja_env.filters["description"] = get_description



def query_all_predicates(graph: rdflib.Graph, uri: Union[str, rdflib.URIRef]):
    return graph.query(f'''
        SELECT ?direction ?predicate ?node
        WHERE {{
            {{ 
                BIND("forward" AS ?direction)
                <{uri}> ?predicate ?node . 
            }}
            UNION
            {{ 
                BIND("backward" AS ?direction)
                ?node ?predicate <{uri}> . 
            }}
        }}
    ''')


@app.route("/")
def hello_world():
    result = graph.query('''
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

    SELECT ?type (COUNT(DISTINCT ?subject) AS ?count)
    WHERE {
        ?subject rdf:type ?type .
    }
    GROUP BY ?type
    ORDER BY ?type 
    ''')

    return jinja2.Template("""
    
    <h1>MyData Viewer</h1>
    
    <table class="table table-hover table-sm table-light">
        <thead>
            <tr>
                <th class="col-xs-3 ">Type</th>
                <th class="col-xs-9 px-3">Count</th>
            </tr>
        </thead>
        <tbody>
            {% for type, count in result %}
                <tr class="{{ loop.cycle('odd', 'even') }}">
                    <td><a href="/resource/{{ type }}">{{ type }}</a></td>
                    <td>{{ count }}</td>
                </tr>
            {% endfor %}
        </tbody>
    </table>
    """).render(**locals())


@app.route("/resource/<path:uri>")
def resource(uri):
    predicates = query_all_predicates(graph, uri)
    grouped_predicates = itertools.groupby(predicates, lambda row: (row.direction, row.predicate))

    return render_template('node.html', **locals())
