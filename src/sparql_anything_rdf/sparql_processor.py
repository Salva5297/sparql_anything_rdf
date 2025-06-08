"""
SPARQL Processor for SPARQL Anything RDF

This module handles SPARQL query execution on RDF graphs,
similar to the Java implementation's SPARQL processing functionality.
"""

from typing import Optional, Dict, Any, Union, List, Iterator
import logging
from pathlib import Path
import rdflib
from rdflib import Graph, Dataset, URIRef
from rdflib.plugins.sparql import prepareQuery
from rdflib.plugins.sparql.results.jsonresults import JSONResultSerializer
from rdflib.plugins.sparql.results.csvresults import CSVResultSerializer
from rdflib.plugins.sparql.results.xmlresults import XMLResultSerializer

from .rdf_parser import RDFParser
from .rdf_to_rdf import RDFToRDF
from .format_handler import FormatHandler


logger = logging.getLogger(__name__)


class SPARQLExecutionError(Exception):
    """Exception raised when SPARQL execution fails"""
    pass


class SPARQLProcessor:
    """
    SPARQL query processor for RDF graphs.
    
    This class provides functionality to execute SPARQL queries
    on RDF graphs and datasets, with support for various result formats.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the SPARQL processor.
        
        Args:
            config: Optional configuration dictionary
        """
        self.config = config or {}
        self.parser = RDFParser(config)
        self.converter = RDFToRDF(config)
        self.format_handler = FormatHandler()
        
        # Default prefixes for SPARQL queries
        self.default_prefixes = {
            'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
            'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
            'xsd': 'http://www.w3.org/2001/XMLSchema#',
            'owl': 'http://www.w3.org/2002/07/owl#',
            'foaf': 'http://xmlns.com/foaf/0.1/',
            'dc': 'http://purl.org/dc/elements/1.1/',
            'dcterms': 'http://purl.org/dc/terms/',
            'skos': 'http://www.w3.org/2004/02/skos/core#',
            'ex': 'http://example.org/',
        }
    
    def query(self, source: Union[str, Path, Graph, Dataset], 
              sparql_query: str,
              result_format: str = 'json',
              bindings: Optional[Dict[str, Any]] = None) -> Any:
        """
        Execute a SPARQL query on RDF data.
        
        Args:
            source: RDF source (file path, Graph, or Dataset)
            sparql_query: SPARQL query string
            result_format: Output format ('json', 'xml', 'csv', 'turtle', etc.)
            bindings: Optional variable bindings
            
        Returns:
            Query results in the specified format
            
        Raises:
            SPARQLExecutionError: If query execution fails
        """
        try:
            # Prepare the graph/dataset
            if isinstance(source, (Graph, Dataset)):
                target = source
            else:
                # Parse RDF source
                target = self.parser.parse(source)
            
            # Add default prefixes to the graph
            if isinstance(target, Graph):
                self._add_prefixes_to_graph(target)
            
            # Prepare and execute query
            prepared_query = self._prepare_query(sparql_query)
            
            # Execute query with optional bindings
            if bindings:
                results = target.query(prepared_query, initBindings=bindings)
            else:
                results = target.query(prepared_query)
            
            # Format results
            formatted_results = self._format_results(results, result_format)
            
            logger.info(f"Executed SPARQL query successfully, format: {result_format}")
            return formatted_results
            
        except Exception as e:
            logger.error(f"Failed to execute SPARQL query: {e}")
            raise SPARQLExecutionError(f"SPARQL execution failed: {e}") from e
    
    def query_multiple(self, sources: List[Union[str, Path]], 
                      sparql_query: str,
                      result_format: str = 'json') -> Any:
        """
        Execute a SPARQL query on multiple RDF sources.
        
        Args:
            sources: List of RDF sources
            sparql_query: SPARQL query string
            result_format: Output format
              Returns:
            Query results from all sources combined
        """
        try:
            # Parse all sources into a dataset
            dataset = self.parser.parse_to_dataset(sources)
            
            # Execute query on the dataset
            return self.query(dataset, sparql_query, result_format)
            
        except Exception as e:
            logger.error(f"Failed to execute SPARQL query on multiple sources: {e}")
            raise SPARQLExecutionError(f"Multi-source SPARQL execution failed: {e}") from e
    
    def construct_query(self, source: Union[str, Path, Graph], 
                       construct_query: str,
                       output_format: str = 'turtle') -> str:
        """
        Execute a SPARQL CONSTRUCT query and return the result graph.
        
        Args:
            source: RDF source
            construct_query: SPARQL CONSTRUCT query string
            output_format: Output format for the constructed graph
            
        Returns:
            Serialized constructed graph
        """
        try:
            # Execute the CONSTRUCT query
            result_graph = self.query(source, construct_query, result_format='graph')
            
            # Handle different types of CONSTRUCT results
            if hasattr(result_graph, 'graph') and result_graph.graph is not None:
                actual_graph = result_graph.graph
            elif hasattr(result_graph, '__iter__'):
                # Create a new graph and add triples from the result
                from rdflib import Graph
                actual_graph = Graph()
                for triple in result_graph:
                    actual_graph.add(triple)
            else:
                actual_graph = result_graph
            
            # Ensure the constructed graph has proper namespace bindings
            self._add_prefixes_to_graph(actual_graph)
            
            # If source is a Graph, copy its namespace bindings to preserve original prefixes
            if isinstance(source, Graph):
                for prefix, namespace in source.namespaces():
                    actual_graph.bind(prefix, namespace)
            
            # Serialize the result graph
            serialized = self.converter.serialize(actual_graph, output_format)
            
            logger.info(f"Executed CONSTRUCT query, result has {len(actual_graph)} triples")
            return serialized
            
        except Exception as e:
            logger.error(f"Failed to execute CONSTRUCT query: {e}")
            raise SPARQLExecutionError(f"CONSTRUCT query failed: {e}") from e
    
    def ask_query(self, source: Union[str, Path, Graph], 
                  ask_query: str) -> bool:
        """
        Execute a SPARQL ASK query and return boolean result.
        
        Args:
            source: RDF source
            ask_query: SPARQL ASK query string
            
        Returns:
            Boolean result of the ASK query
        """
        try:
            result = self.query(source, ask_query, result_format='bool')
            logger.info(f"Executed ASK query, result: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to execute ASK query: {e}")
            raise SPARQLExecutionError(f"ASK query failed: {e}") from e
    
    def describe_query(self, source: Union[str, Path, Graph], 
                      describe_query: str,
                      output_format: str = 'turtle') -> str:
        """
        Execute a SPARQL DESCRIBE query and return the result graph.
        
        Args:
            source: RDF source
            describe_query: SPARQL DESCRIBE query string
            output_format: Output format for the description graph
            
        Returns:
            Serialized description graph
        """
        try:
            # Execute the DESCRIBE query
            result_graph = self.query(source, describe_query, result_format='graph')
            
            # Handle different types of DESCRIBE results
            if hasattr(result_graph, 'graph') and result_graph.graph is not None:
                actual_graph = result_graph.graph
            elif hasattr(result_graph, '__iter__'):
                # Create a new graph and add triples from the result
                from rdflib import Graph
                actual_graph = Graph()
                for triple in result_graph:
                    actual_graph.add(triple)
            else:
                actual_graph = result_graph
            
            # Serialize the result graph
            serialized = self.converter.serialize(actual_graph, output_format)
            
            logger.info(f"Executed DESCRIBE query, result has {len(actual_graph)} triples")
            return serialized
            
        except Exception as e:
            logger.error(f"Failed to execute DESCRIBE query: {e}")
            raise SPARQLExecutionError(f"DESCRIBE query failed: {e}") from e
    
    def validate_query(self, sparql_query: str) -> Dict[str, Any]:
        """
        Validate a SPARQL query without executing it.
        
        Args:
            sparql_query: SPARQL query string to validate
            
        Returns:
            Dictionary with validation results
        """
        validation_result = {
            'valid': False,
            'query_type': None,
            'prefixes': {},
            'errors': []
        }
        
        try:
            # Try to prepare the query
            prepared_query = self._prepare_query(sparql_query)
            
            validation_result['valid'] = True
            validation_result['query_type'] = self._get_query_type(sparql_query)
            validation_result['prefixes'] = self._extract_prefixes(sparql_query)
            
            logger.debug(f"Query validation successful: {validation_result['query_type']}")
            
        except Exception as e:
            validation_result['errors'].append(str(e))
            logger.warning(f"Query validation failed: {e}")
        
        return validation_result
    
    def get_query_statistics(self, source: Union[str, Path, Graph], 
                           sparql_query: str) -> Dict[str, Any]:
        """
        Get statistics about query execution without returning full results.
        
        Args:
            source: RDF source
            sparql_query: SPARQL query string
            
        Returns:
            Dictionary with query execution statistics
        """
        try:
            # Execute query and count results
            results = self.query(source, sparql_query, result_format='raw')
            
            stats = {
                'result_count': 0,
                'variables': [],
                'query_type': self._get_query_type(sparql_query),
                'execution_time': None  # Could be added with timing
            }
            
            if hasattr(results, 'vars'):
                stats['variables'] = [str(var) for var in results.vars]
            
            # Count results
            result_count = 0
            for result in results:
                result_count += 1
            stats['result_count'] = result_count
            
            logger.info(f"Query statistics: {result_count} results, type: {stats['query_type']}")
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get query statistics: {e}")
            raise SPARQLExecutionError(f"Query statistics failed: {e}") from e
    
    def _prepare_query(self, sparql_query: str) -> Any:
        """Prepare a SPARQL query with default prefixes"""
        # Add default prefixes if not already present
        full_query = self._add_default_prefixes(sparql_query)
        
        # Prepare the query
        return prepareQuery(full_query)
    
    def _add_default_prefixes(self, sparql_query: str) -> str:
        """Add default prefixes to a SPARQL query if not already present"""
        query_upper = sparql_query.upper()
        prefix_declarations = []
        
        for prefix, uri in self.default_prefixes.items():
            prefix_pattern = f"PREFIX {prefix.upper()}:"
            if prefix_pattern not in query_upper:
                prefix_declarations.append(f"PREFIX {prefix}: <{uri}>")
        
        # Add custom prefixes from config
        if 'prefixes' in self.config:
            for prefix, uri in self.config['prefixes'].items():
                prefix_pattern = f"PREFIX {prefix.upper()}:"
                if prefix_pattern not in query_upper:
                    prefix_declarations.append(f"PREFIX {prefix}: <{uri}>")
        
        if prefix_declarations:
            return '\n'.join(prefix_declarations) + '\n\n' + sparql_query
        else:
            return sparql_query
    
    def _add_prefixes_to_graph(self, graph: Graph) -> None:
        """Add default prefixes to an RDF graph"""
        for prefix, uri in self.default_prefixes.items():
            graph.bind(prefix, uri)
        
        # Add custom prefixes from config
        if 'prefixes' in self.config:
            for prefix, uri in self.config['prefixes'].items():
                graph.bind(prefix, uri)
    
    def _format_results(self, results: Any, format_name: str) -> Any:
        """Format SPARQL query results in the specified format"""
        if format_name == 'raw':
            return results
        
        if format_name == 'bool':
            return bool(results)
        
        if format_name == 'graph':
            # For CONSTRUCT/DESCRIBE queries, check if results has a graph attribute
            if hasattr(results, 'graph') and results.graph is not None:
                return results.graph
            elif hasattr(results, '__iter__'):
                # For some SPARQL results, need to iterate to get the graph
                return results
            else:
                return results
        
        # For SELECT queries
        if format_name == 'json':
            return self._results_to_json(results)
        elif format_name == 'xml':
            return self._results_to_xml(results)
        elif format_name == 'csv':
            return self._results_to_csv(results)
        elif format_name == 'table':
            return self._results_to_table(results)
        else:
            # Default to list of dictionaries
            return self._results_to_list(results)
    
    def _results_to_json(self, results: Any) -> str:
        """Convert results to JSON format"""
        try:
            serializer = JSONResultSerializer(results)
            return serializer.serialize(format='json').decode('utf-8')
        except Exception:
            # Fallback to manual conversion
            return self._results_to_json_manual(results)
    
    def _results_to_json_manual(self, results: Any) -> str:
        """Manual conversion to JSON format"""
        import json
        
        result_list = []
        for result in results:
            row = {}
            for var in results.vars:
                value = result.get(var)
                if value is not None:
                    row[str(var)] = str(value)
            result_list.append(row)
        
        return json.dumps(result_list, indent=2)
    
    def _results_to_csv(self, results: Any) -> str:
        """Convert results to CSV format"""
        try:
            serializer = CSVResultSerializer(results)
            return serializer.serialize(format='csv').decode('utf-8')
        except Exception:
            # Fallback to manual conversion
            return self._results_to_csv_manual(results)
    
    def _results_to_csv_manual(self, results: Any) -> str:
        """Manual conversion to CSV format"""
        import csv
        from io import StringIO
        
        output = StringIO()
        writer = csv.writer(output)
        
        # Write header
        if hasattr(results, 'vars'):
            writer.writerow([str(var) for var in results.vars])
        
        # Write data
        for result in results:
            row = []
            for var in results.vars:
                value = result.get(var)
                row.append(str(value) if value is not None else '')
            writer.writerow(row)
        
        return output.getvalue()
    
    def _results_to_xml(self, results: Any) -> str:
        """Convert results to XML format"""
        try:
            serializer = XMLResultSerializer(results)
            return serializer.serialize(format='xml').decode('utf-8')
        except Exception:
            # Basic XML format fallback
            return self._results_to_xml_manual(results)
    
    def _results_to_xml_manual(self, results: Any) -> str:
        """Manual conversion to XML format"""
        xml_lines = ['<?xml version="1.0"?>', '<sparql>']
        
        for result in results:
            xml_lines.append('  <result>')
            for var in results.vars:
                value = result.get(var)
                if value is not None:
                    xml_lines.append(f'    <{var}>{str(value)}</{var}>')
            xml_lines.append('  </result>')
        
        xml_lines.append('</sparql>')
        return '\n'.join(xml_lines)
    
    def _results_to_table(self, results: Any) -> List[List[str]]:
        """Convert results to table format (list of lists)"""
        table = []
        
        # Add header
        if hasattr(results, 'vars'):
            table.append([str(var) for var in results.vars])
        
        # Add data rows
        for result in results:
            row = []
            for var in results.vars:
                value = result.get(var)
                row.append(str(value) if value is not None else '')
            table.append(row)
        
        return table
    
    def _results_to_list(self, results: Any) -> List[Dict[str, str]]:
        """Convert results to list of dictionaries"""
        result_list = []
        
        for result in results:
            row = {}
            for var in results.vars:
                value = result.get(var)
                if value is not None:
                    row[str(var)] = str(value)
            result_list.append(row)
        
        return result_list
    
    def _get_query_type(self, sparql_query: str) -> str:
        """Determine the type of SPARQL query"""
        query_upper = sparql_query.upper().strip()
        
        if query_upper.startswith('SELECT'):
            return 'SELECT'
        elif query_upper.startswith('CONSTRUCT'):
            return 'CONSTRUCT'
        elif query_upper.startswith('ASK'):
            return 'ASK'
        elif query_upper.startswith('DESCRIBE'):
            return 'DESCRIBE'
        else:
            return 'UNKNOWN'
    
    def _extract_prefixes(self, sparql_query: str) -> Dict[str, str]:
        """Extract prefix declarations from a SPARQL query"""
        prefixes = {}
        lines = sparql_query.split('\n')
        
        for line in lines:
            line = line.strip()
            if line.upper().startswith('PREFIX'):
                try:
                    parts = line.split(':', 1)
                    if len(parts) == 2:
                        prefix = parts[0].split()[-1]
                        uri = parts[1].strip().strip('<>')
                        prefixes[prefix] = uri
                except Exception:
                    continue
        
        return prefixes
