"""Optional RDF/Turtle output stage.

Decoupled from the primary write: it consumes the transform result and
serializes rows to Turtle. Failure handling is configurable (soft/hard fail).
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import FOAF, RDF

from ..config.schema import TtlOutputSpec

# Common prefixes available to mapping predicates like "foaf:name".
_PREFIXES = {"foaf": FOAF}


def _resolve_predicate(ns: Namespace, qname: str) -> URIRef:
    if ":" in qname:
        prefix, local = qname.split(":", 1)
        if prefix in _PREFIXES:
            return _PREFIXES[prefix][local]
        return URIRef(ns[local])
    return URIRef(ns[qname])


def run_ttl_stage(df: DataFrame, spec: TtlOutputSpec) -> str:
    """Serialize ``df`` to Turtle per ``spec`` and write it to ``spec.path``.

    Returns the serialized Turtle string.
    """
    ns = Namespace(spec.namespace)
    graph = Graph()
    graph.bind("ex", ns)
    graph.bind("foaf", FOAF)

    columns = [spec.subject_column, *spec.mapping.keys()]
    for row in df.select(*columns).collect():
        subject = URIRef(ns[str(row[spec.subject_column])])
        graph.add((subject, RDF.type, ns["Entity"]))
        for col, predicate in spec.mapping.items():
            value = row[col]
            if value is None:
                continue
            graph.add((subject, _resolve_predicate(ns, predicate), Literal(value)))

    turtle = graph.serialize(format="turtle")
    if spec.path:
        with open(spec.path, "w", encoding="utf-8") as fh:
            fh.write(turtle)
    return turtle
