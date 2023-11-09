"""
Minimalistic RDF viewer

Running:
$ flask --app mydata.viewer:app run -h localhost -p 4999 --debug

Then open
http://localhost:4999/
"""

import itertools
from typing import Union
from urllib.parse import quote, unquote

import jinja2
import rdflib
from flask import Flask, render_template
from rdflib import Graph, Literal, URIRef
from rdflib.plugins.stores.sparqlstore import SPARQLStore

app = Flask(__name__)

store = SPARQLStore("http://localhost:3030/mydata/sparql")
graph = rdflib.ConjunctiveGraph(store)

schema = Graph().parse("knowledge/schema_standard.ttl")

namespace_manager = graph.namespace_manager if graph is not None else None
app.jinja_env.filters["is_uri"] = lambda obj: isinstance(obj, URIRef)
app.jinja_env.filters["is_literal"] = lambda obj: isinstance(obj, Literal)
app.jinja_env.filters["n3_notation"] = lambda obj: obj.n3(namespace_manager)
app.jinja_env.filters["quote"] = quote
app.jinja_env.filters["unquote"] = unquote


def get_label(uri: rdflib.URIRef):
    label = schema.value(uri, URIRef("http://www.w3.org/2000/01/rdf-schema#label"))
    if label is not None:
        return label.value
    else:
        return uri.n3(namespace_manager)


def get_description(uri: rdflib.URIRef):
    comment = schema.value(uri, URIRef("http://www.w3.org/2000/01/rdf-schema#comment"))
    if comment is not None:
        return comment.value


app.jinja_env.filters["label"] = get_label
app.jinja_env.filters["description"] = get_description


def query_all_predicates(graph: rdflib.Graph, uri: Union[str, rdflib.URIRef], predicate_limit=30):
    predicate_query = jinja2.Template(
        """
    SELECT
        ?direction
        ?predicate
        ?total
        ?node

    WHERE {
        # Retrieve all predicates related to the node
        {
            BIND("forward" AS ?direction)
            <{{uri}}> ?predicate ?node .
        }
        UNION
        {
            BIND("backward" AS ?direction)
            ?node ?predicate <{{uri}}> .
        }

        # Determine the number of related nodes for each predicate
        {
            SELECT
                ?direction
                ?predicate
                (COUNT(?node) AS ?total)
            WHERE {
                {
                    BIND("forward" AS ?direction)
                    <{{uri}}> ?predicate ?node .
                }
                UNION
                {
                    BIND("backward" AS ?direction)
                    ?node ?predicate <{{uri}}> .
                }
            }
            GROUP BY ?direction ?predicate
        }

        # Apply limiting for each predicate by random sampling
        FILTER(RAND() <= ({{predicate_limit}} / ?total))
    }

    ORDER BY DESC(?direction) ?predicate
    """
    )

    predicates = graph.query(predicate_query.render(uri=uri, predicate_limit=predicate_limit))

    predicate_types = []
    grouped_predicates = itertools.groupby(predicates, lambda row: (row.direction, row.predicate, int(row.total)))
    for (direction, predicate, total), group in grouped_predicates:
        predicate_types.append(
            {
                "direction": direction,
                "predicate": predicate,
                "total": total,
                "nodes": [node for _, _, _, node in group],
            }
        )

    metainfo_query = jinja2.Template(
        """
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT ?uri ?label ?description
    WHERE {
        VALUES ?uri {
            {% for predicate in predicate_types %}
            <{{ predicate.predicate }}>
            {% endfor %}
        }

        OPTIONAL { ?uri rdfs:label ?label . }
        OPTIONAL { ?uri rdfs:comment ?description . }
    }
    """
    )

    metainfo = graph.query(metainfo_query.render(predicate_types=predicate_types))
    predicate_label = {}
    predicate_description = {}
    for row in metainfo:
        if row.label:
            predicate_label[row.uri] = row.label
        if row.description:
            predicate_description[row.uri] = row.description
    for predicate_type in predicate_types:
        predicate_type["label"] = predicate_label.get(predicate_type["predicate"])
        predicate_type["description"] = predicate_description.get(predicate_type["predicate"])

    return list(
        sorted(predicate_types, key=lambda x: (0 if x["direction"] == "forward" else 1, x.get("label"), x["predicate"]))
    )


@app.route("/")
def list_types():
    result = graph.query(
        """
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

    SELECT ?type (COUNT(?subject) AS ?count)
    WHERE {
        ?subject rdf:type ?type .
        FILTER(!ISBLANK(?type))
    }
    GROUP BY ?type
    ORDER BY ?type
    """
    )

    result = sorted(result, key=lambda row: "" if str(row.type).startswith("https://ownld.org") else str(row.type))
    return render_template("types.html", **locals())


@app.route("/resource/<path:uri>")
def resource(uri):
    predicates = query_all_predicates(graph, uri)

    return render_template("node.html", **locals())
