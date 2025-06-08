"""
RDF to RDF Converter for SPARQL Anything RDF

This module handles conversion of RDF data between different formats
and provides utilities for RDF graph manipulation.
"""

from typing import Optional, Dict, Any, Union, List
import logging
from pathlib import Path
from io import StringIO
import rdflib
from rdflib import Graph, Dataset, Namespace, URIRef, Literal, BNode
from rdflib.namespace import RDF, RDFS, XSD, OWL

from .format_handler import FormatHandler, RDFFormat
from .rdf_parser import RDFParser


logger = logging.getLogger(__name__)


class RDFConversionError(Exception):
    """Exception raised when RDF conversion fails"""
    pass


class RDFToRDF:
    """
    Converter for RDF data between different formats and representations.
    
    This class provides functionality to convert RDF data between formats,
    serialize graphs, and manipulate RDF structures.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the RDF converter.
        
        Args:
            config: Optional configuration dictionary
        """
        self.config = config or {}
        self.format_handler = FormatHandler()
        self.parser = RDFParser(config)
        
        # Common namespaces
        self.namespaces = {
            'rdf': RDF,
            'rdfs': RDFS,
            'xsd': XSD,
            'owl': OWL,
        }
    
    def convert(self, source: Union[str, Path, Graph], 
                source_format: Optional[str] = None,
                target_format: str = 'turtle') -> Graph:
        """
        Convert RDF data from one format to another.
        
        Args:
            source: Source data (file path, Graph, or string)
            source_format: Source format hint
            target_format: Target format for conversion
            
        Returns:
            RDF Graph in the target format
            
        Raises:
            RDFConversionError: If conversion fails
        """
        try:
            # Parse source if not already a Graph
            if isinstance(source, Graph):
                graph = source
            else:
                graph = self.parser.parse(source, source_format)
            
            # Add common namespaces
            self._add_common_namespaces(graph)
            
            # Apply any configured transformations
            if 'transformations' in self.config:
                graph = self._apply_transformations(graph, self.config['transformations'])
            
            logger.info(f"Converted RDF data with {len(graph)} triples to {target_format}")
            return graph
            
        except Exception as e:
            logger.error(f"Failed to convert RDF data: {e}")
            raise RDFConversionError(f"RDF conversion failed: {e}") from e
    
    def serialize(self, graph: Graph, format_name: str = 'turtle', 
                 destination: Optional[Union[str, Path]] = None) -> str:
        """
        Serialize an RDF graph to a string or file.
        
        Args:
            graph: RDF graph to serialize
            format_name: Output format name
            destination: Optional file path to write to
            
        Returns:
            Serialized RDF data as string
            
        Raises:
            RDFConversionError: If serialization fails
        """
        try:
            # Convert format name to RDFFormat enum
            try:
                rdf_format = RDFFormat(format_name.lower())
            except ValueError:
                rdf_format = RDFFormat.TURTLE
            
            # Get RDFLib format string
            rdflib_format = self.format_handler.get_rdflib_format(rdf_format)
            
            # Add common namespaces for better serialization
            self._add_common_namespaces(graph)
            
            # Serialize
            if destination:
                graph.serialize(destination=str(destination), format=rdflib_format)
                logger.info(f"Serialized graph to file {destination} in format {rdflib_format}")
                return f"Serialized to {destination}"
            else:
                serialized = graph.serialize(format=rdflib_format)
                if isinstance(serialized, bytes):
                    serialized = serialized.decode('utf-8')
                logger.debug(f"Serialized graph to string in format {rdflib_format}")
                return serialized
                
        except Exception as e:
            logger.error(f"Failed to serialize RDF graph: {e}")
            raise RDFConversionError(f"RDF serialization failed: {e}") from e
    
    def merge_graphs(self, graphs: List[Graph], 
                    target_graph: Optional[Graph] = None) -> Graph:
        """
        Merge multiple RDF graphs into one.
        
        Args:
            graphs: List of RDF graphs to merge
            target_graph: Optional target graph (creates new if None)
            
        Returns:
            Merged RDF graph
        """
        if target_graph is None:
            target_graph = Graph()
        
        # Add common namespaces
        self._add_common_namespaces(target_graph)
        
        total_triples = 0
        for graph in graphs:
            for triple in graph:
                target_graph.add(triple)
                total_triples += 1
        
        logger.info(f"Merged {len(graphs)} graphs into one with {total_triples} total triples")
        return target_graph
    
    def filter_graph(self, graph: Graph, 
                    predicate_filter: Optional[List[str]] = None,
                    subject_filter: Optional[List[str]] = None,
                    object_filter: Optional[List[str]] = None) -> Graph:
        """
        Filter an RDF graph by predicates, subjects, or objects.
        
        Args:
            graph: Source RDF graph
            predicate_filter: List of predicate URIs to include
            subject_filter: List of subject URIs to include
            object_filter: List of object values to include
            
        Returns:
            Filtered RDF graph
        """
        filtered_graph = Graph()
        self._add_common_namespaces(filtered_graph)
        
        for subject, predicate, obj in graph:
            include = True
            
            # Apply filters
            if predicate_filter and str(predicate) not in predicate_filter:
                include = False
            
            if subject_filter and str(subject) not in subject_filter:
                include = False
            
            if object_filter and str(obj) not in object_filter:
                include = False
            
            if include:
                filtered_graph.add((subject, predicate, obj))
        
        logger.info(f"Filtered graph from {len(graph)} to {len(filtered_graph)} triples")
        return filtered_graph
    
    def validate_graph(self, graph: Graph) -> Dict[str, Any]:
        """
        Validate an RDF graph and return statistics.
        
        Args:
            graph: RDF graph to validate
            
        Returns:
            Dictionary with validation results and statistics
        """
        stats = {
            'total_triples': len(graph),
            'subjects': set(),
            'predicates': set(),
            'objects': set(),
            'namespaces': dict(graph.namespaces()),
            'blank_nodes': 0,
            'literals': 0,
            'uris': 0,
            'errors': []
        }
        
        try:
            for subject, predicate, obj in graph:
                # Count subjects
                stats['subjects'].add(subject)
                stats['predicates'].add(predicate)
                stats['objects'].add(obj)
                
                # Count node types
                if isinstance(subject, BNode):
                    stats['blank_nodes'] += 1
                elif isinstance(subject, URIRef):
                    stats['uris'] += 1
                
                if isinstance(obj, Literal):
                    stats['literals'] += 1
                elif isinstance(obj, URIRef):
                    stats['uris'] += 1
                elif isinstance(obj, BNode):
                    stats['blank_nodes'] += 1
        
        except Exception as e:
            stats['errors'].append(f"Validation error: {e}")
        
        # Convert sets to counts for JSON serialization
        stats['unique_subjects'] = len(stats['subjects'])
        stats['unique_predicates'] = len(stats['predicates'])
        stats['unique_objects'] = len(stats['objects'])
        
        # Remove sets from final stats
        del stats['subjects']
        del stats['predicates']
        del stats['objects']
        
        logger.info(f"Validated graph with {stats['total_triples']} triples")
        return stats
    
    def extract_schema(self, graph: Graph) -> Graph:
        """
        Extract schema information (classes, properties) from an RDF graph.
        
        Args:
            graph: Source RDF graph
            
        Returns:
            Graph containing only schema information
        """
        schema_graph = Graph()
        self._add_common_namespaces(schema_graph)
        
        # Extract classes
        for subject, predicate, obj in graph:
            if predicate == RDF.type and obj in [RDFS.Class, OWL.Class]:
                schema_graph.add((subject, predicate, obj))
            elif predicate in [RDFS.subClassOf, OWL.equivalentClass]:
                schema_graph.add((subject, predicate, obj))
            elif predicate == RDF.type and obj in [RDF.Property, OWL.ObjectProperty, 
                                                  OWL.DatatypeProperty, OWL.AnnotationProperty]:
                schema_graph.add((subject, predicate, obj))
            elif predicate in [RDFS.domain, RDFS.range, RDFS.subPropertyOf]:
                schema_graph.add((subject, predicate, obj))
        
        logger.info(f"Extracted schema with {len(schema_graph)} triples")
        return schema_graph
    
    def _add_common_namespaces(self, graph: Graph) -> None:
        """Add common namespaces to a graph"""
        for prefix, namespace in self.namespaces.items():
            graph.bind(prefix, namespace)
        
        # Add any configured namespaces
        if 'namespaces' in self.config:
            for prefix, uri in self.config['namespaces'].items():
                graph.bind(prefix, Namespace(uri))
    
    def _apply_transformations(self, graph: Graph, 
                             transformations: List[Dict[str, Any]]) -> Graph:
        """
        Apply transformation rules to an RDF graph.
        
        Args:
            graph: Source graph
            transformations: List of transformation configurations
            
        Returns:
            Transformed graph
        """
        for transform in transformations:
            transform_type = transform.get('type')
            
            if transform_type == 'add_namespace':
                prefix = transform.get('prefix')
                uri = transform.get('uri')
                if prefix and uri:
                    graph.bind(prefix, Namespace(uri))
            
            elif transform_type == 'replace_predicate':
                old_pred = URIRef(transform.get('old_predicate'))
                new_pred = URIRef(transform.get('new_predicate'))
                
                # Collect triples to modify
                to_remove = []
                to_add = []
                
                for subject, predicate, obj in graph:
                    if predicate == old_pred:
                        to_remove.append((subject, predicate, obj))
                        to_add.append((subject, new_pred, obj))
                
                # Apply changes
                for triple in to_remove:
                    graph.remove(triple)
                for triple in to_add:
                    graph.add(triple)
        
        return graph
    
    def get_format_info(self) -> Dict[str, Any]:
        """
        Get information about supported RDF formats.
        
        Returns:
            Dictionary with format information
        """
        return {
            'supported_formats': [fmt.value for fmt in RDFFormat],
            'mime_types': list(self.format_handler.get_mime_types()),
            'extensions': list(self.format_handler.get_extensions()),
            'default_format': RDFFormat.TURTLE.value
        }
