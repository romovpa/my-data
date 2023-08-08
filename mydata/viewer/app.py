"""
Minimalistic RDF viewer

Running:
$ flask --app mydata.viewer:app run -h localhost -p 4999  --debug

Then open
http://localhost:4999/
"""

from typing import Union

import jinja2
import rdflib
from flask import Flask
from rdflib import URIRef, Literal
from rdflib.plugins.stores.sparqlstore import SPARQLStore

app = Flask(__name__)

store = SPARQLStore('http://localhost:3030/ds/sparql')
graph = rdflib.ConjunctiveGraph(store)


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
    ORDER BY DESC(?count)
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

    template = """
        <h1>{{ uri }}</h1>
        
        <table class="table table-hover table-sm table-light">
		    <thead>
                <tr>
                    <th class="col-xs-3 ">Property</th>
                    <th class="col-xs-9 px-3">Value</th>
                </tr>
		    </thead>
		    <tbody>
		        {% for direction, predicate, node in predicates %}
		            <tr class="{{ loop.cycle('odd', 'even') }}">
		                <td>
		                    {% if direction == "forward" %}
		                    {{ predicate }}
		                    {% else %}
		                    is {{ predicate }} of
		                    {% endif %}
		                </td>
		                <td>
		                    {% if node|is_uri %}
		                    <a href="/resource/{{ node }}">{{ node }}</a>
		                    {% elif node|is_literal %}
		                    {{ node.value }}
		                    {% else %}
		                    {{ node }}
		                    {% endif %}
		                </td>
		            </tr>
		        {% endfor %}
		    </tbody>
        </table>
    """

    namespace_manager = graph.namespace_manager if graph is not None else None
    env = jinja2.Environment(loader=jinja2.BaseLoader())
    env.filters["is_uri"] = lambda obj: isinstance(obj, URIRef)
    env.filters["is_literal"] = lambda obj: isinstance(obj, Literal)
    env.filters["n3_notation"] = lambda obj: obj.n3(namespace_manager)

    template = env.from_string(template)

    html = template.render(**locals())

    return html
