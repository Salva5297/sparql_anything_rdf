"""
SPARQL Anything RDF - Python Implementation

A Python library for processing RDF data and querying it with SPARQL.
This implementation supports multiple RDF formats and follows the 
SPARQL Anything specification for RDF processing.

Supported formats:
- RDF/XML (.rdf, .xml)
- Turtle (.ttl)
- N-Triples (.nt)
- JSON-LD (.jsonld)
- Trig (.trig)
- N-Quads (.nq)
- TriX (.trix)
- RDF/Thrift (.trdf)
- OWL/XML (.owl)
"""

__version__ = "1.0.0"
__author__ = "SPARQL Anything Python Contributors"

from .rdf_parser import RDFParser
from .rdf_to_rdf import RDFToRDF
from .sparql_processor import SPARQLProcessor
from .format_handler import FormatHandler

__all__ = [
    'RDFParser',
    'RDFToRDF', 
    'SPARQLProcessor',
    'FormatHandler'
]
