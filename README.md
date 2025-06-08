# SPARQL Anything RDF - Python Implementation

A Python implementation of SPARQL Anything for RDF processing. This module provides functionality to read, parse, and query RDF data in various formats including RDF/XML, Turtle, N-Triples, JSON-LD, and more.

## Features

- Support for multiple RDF formats:
  - RDF/XML (.rdf, .xml)
  - Turtle (.ttl)
  - N-Triples (.nt)
  - JSON-LD (.jsonld)
  - Trig (.trig)
  - N-Quads (.nq)
  - TriX (.trix)
  - RDF/Thrift (.trdf)
  - OWL/XML (.owl)

- HTTP content negotiation for remote RDF resources
- Integration with SPARQL queries
- Compatible with the SPARQL Anything ecosystem

## Installation

```bash
pip install -r requirements.txt
```

## Usage

### Basic RDF Processing

```python
from sparql_anything_rdf import RDFParser, RDFToRDF, SPARQLProcessor

# Parse RDF file
parser = RDFParser()
data = parser.parse('example.ttl')

# Convert to RDF graph
converter = RDFToRDF()
graph = converter.convert(data)

# Query with SPARQL
processor = SPARQLProcessor()
results = processor.query(graph, "SELECT * WHERE { ?s ?p ?o }")
```

### Command Line Interface

```bash
sparql-anything-rdf --input example.ttl --query "SELECT * WHERE { ?s ?p ?o }"
```

## Supported MIME Types

- `application/rdf+xml`
- `text/turtle`
- `application/n-triples`
- `application/ld+json`
- `text/trig`
- `application/n-quads`
- `application/trix+xml`
- `application/rdf+thrift`
- `application/owl+xml`

## Development

This implementation follows the same patterns and structure as other SPARQL Anything Python modules, ensuring consistency and maintainability.

## License

Licensed under the Apache License, Version 2.0.
