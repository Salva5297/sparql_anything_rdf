"""
RDF Parser for SPARQL Anything RDF

This module handles parsing RDF files and data from various sources,
similar to the Java implementation's RDF parsing functionality.
"""

from typing import Optional, Dict, Any, Union, IO
import logging
from pathlib import Path
import requests
from urllib.parse import urlparse
import rdflib
from rdflib import Graph, ConjunctiveGraph, Dataset
from rdflib.plugins.stores.memory import Memory

from .format_handler import FormatHandler, RDFFormat


logger = logging.getLogger(__name__)


class RDFParsingError(Exception):
    """Exception raised when RDF parsing fails"""
    pass


class RDFParser:
    """
    Parser for RDF data from files, URLs, and strings.
    
    This class handles the parsing of RDF data in various formats,
    following the same pattern as other SPARQL Anything parsers.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the RDF parser.
        
        Args:
            config: Optional configuration dictionary
        """
        self.config = config or {}
        self.format_handler = FormatHandler()
        
    def parse(self, source: Union[str, Path, IO], 
              format_hint: Optional[str] = None,
              content_type: Optional[str] = None) -> Graph:
        """
        Parse RDF data from a source.
        
        Args:
            source: File path, URL, or file-like object
            format_hint: Optional format hint
            content_type: Optional MIME type from HTTP headers
            
        Returns:
            RDFLib Graph containing the parsed data
            
        Raises:
            RDFParsingError: If parsing fails
        """
        try:
            # Create RDF graph
            graph = Graph()
            
            # Determine source type and parse accordingly
            if isinstance(source, (str, Path)):
                source_str = str(source)
                
                # Check if it's a URL
                if self._is_url(source_str):
                    return self._parse_from_url(graph, source_str, format_hint, content_type)
                else:
                    return self._parse_from_file(graph, source_str, format_hint)
            else:
                # Assume file-like object
                return self._parse_from_stream(graph, source, format_hint)
                
        except Exception as e:
            logger.error(f"Failed to parse RDF data: {e}")
            raise RDFParsingError(f"RDF parsing failed: {e}") from e
    
    def _is_url(self, source: str) -> bool:
        """Check if source is a URL"""
        try:
            result = urlparse(source)
            return result.scheme in ('http', 'https', 'ftp')
        except Exception:
            return False
    
    def _parse_from_file(self, graph: Graph, file_path: str, 
                        format_hint: Optional[str] = None) -> Graph:
        """
        Parse RDF from a local file.
        
        Args:
            graph: RDFLib graph to populate
            file_path: Path to the RDF file
            format_hint: Optional format hint
            
        Returns:
            Populated RDF graph
        """
        path = Path(file_path)
        
        if not path.exists():
            raise RDFParsingError(f"File not found: {file_path}")
        
        # Detect format
        if format_hint:
            try:
                rdf_format = RDFFormat(format_hint.lower())
            except ValueError:
                rdf_format = self.format_handler.detect_format(file_path)
        else:
            rdf_format = self.format_handler.detect_format(file_path)
        
        # Get RDFLib format string
        rdflib_format = self.format_handler.get_rdflib_format(rdf_format)
        
        logger.debug(f"Parsing file {file_path} with format {rdflib_format}")
        
        try:
            # Parse the file
            graph.parse(str(path), format=rdflib_format)
            logger.info(f"Successfully parsed {len(graph)} triples from {file_path}")
            return graph
            
        except Exception as e:
            # Try alternative formats if initial parsing fails
            return self._try_alternative_formats(graph, str(path), rdf_format)
    
    def _parse_from_url(self, graph: Graph, url: str, 
                       format_hint: Optional[str] = None,
                       content_type: Optional[str] = None) -> Graph:
        """
        Parse RDF from a URL.
        
        Args:
            graph: RDFLib graph to populate
            url: URL to fetch RDF data from
            format_hint: Optional format hint
            content_type: Optional MIME type from headers
            
        Returns:
            Populated RDF graph
        """
        # Prepare headers for content negotiation
        headers = self.format_handler.get_content_negotiation_headers()
        
        # Add custom headers from config
        if 'headers' in self.config:
            headers.update(self.config['headers'])
        
        logger.debug(f"Fetching RDF data from {url}")
        
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            # Get content type from response
            actual_content_type = content_type or response.headers.get('content-type')
            
            # Detect format
            if format_hint:
                try:
                    rdf_format = RDFFormat(format_hint.lower())
                except ValueError:
                    rdf_format = self.format_handler.detect_format(url, actual_content_type)
            else:
                rdf_format = self.format_handler.detect_format(url, actual_content_type)
            
            # Get RDFLib format string
            rdflib_format = self.format_handler.get_rdflib_format(rdf_format)
            
            logger.debug(f"Parsing URL {url} with format {rdflib_format}")
            
            # Parse the content
            graph.parse(data=response.text, format=rdflib_format, publicID=url)
            logger.info(f"Successfully parsed {len(graph)} triples from {url}")
            return graph
            
        except requests.RequestException as e:
            raise RDFParsingError(f"Failed to fetch RDF data from {url}: {e}") from e
        except Exception as e:
            # Try parsing as different formats
            return self._try_alternative_formats_from_data(graph, response.text, url, rdf_format)
    
    def _parse_from_stream(self, graph: Graph, stream: IO, 
                          format_hint: Optional[str] = None) -> Graph:
        """
        Parse RDF from a stream/file-like object.
        
        Args:
            graph: RDFLib graph to populate
            stream: File-like object containing RDF data
            format_hint: Optional format hint
            
        Returns:
            Populated RDF graph
        """
        # Read content from stream
        content = stream.read()
        if isinstance(content, bytes):
            content = content.decode('utf-8')
        
        # Default to turtle if no hint provided
        rdf_format = RDFFormat.TURTLE
        if format_hint:
            try:
                rdf_format = RDFFormat(format_hint.lower())
            except ValueError:
                pass
        
        rdflib_format = self.format_handler.get_rdflib_format(rdf_format)
        
        logger.debug(f"Parsing stream with format {rdflib_format}")
        
        try:
            graph.parse(data=content, format=rdflib_format)
            logger.info(f"Successfully parsed {len(graph)} triples from stream")
            return graph
            
        except Exception as e:
            return self._try_alternative_formats_from_data(graph, content, "stream", rdf_format)
    
    def _try_alternative_formats(self, graph: Graph, source: str, 
                               failed_format: RDFFormat) -> Graph:
        """
        Try parsing with alternative formats if the initial format fails.
        
        Args:
            graph: RDFLib graph to populate
            source: Source file path
            failed_format: Format that failed to parse
            
        Returns:
            Populated RDF graph
        """
        # Common alternative formats to try
        alternatives = [
            RDFFormat.TURTLE,
            RDFFormat.RDF_XML,
            RDFFormat.N_TRIPLES,
            RDFFormat.JSON_LD
        ]
        
        # Remove the failed format from alternatives
        if failed_format in alternatives:
            alternatives.remove(failed_format)
        
        for alt_format in alternatives:
            try:
                rdflib_format = self.format_handler.get_rdflib_format(alt_format)
                logger.debug(f"Trying alternative format {rdflib_format} for {source}")
                
                # Clear graph before retrying
                graph = Graph()
                graph.parse(source, format=rdflib_format)
                
                logger.info(f"Successfully parsed {len(graph)} triples with format {rdflib_format}")
                return graph
                
            except Exception as e:
                logger.debug(f"Alternative format {rdflib_format} also failed: {e}")
                continue
        
        raise RDFParsingError(f"Failed to parse {source} with any supported format")
    
    def _try_alternative_formats_from_data(self, graph: Graph, data: str, 
                                         source_name: str, failed_format: RDFFormat) -> Graph:
        """
        Try parsing data with alternative formats.
        
        Args:
            graph: RDFLib graph to populate
            data: RDF data string
            source_name: Name of the source (for logging)
            failed_format: Format that failed to parse
            
        Returns:
            Populated RDF graph
        """
        # Common alternative formats to try
        alternatives = [
            RDFFormat.TURTLE,
            RDFFormat.RDF_XML,
            RDFFormat.N_TRIPLES,
            RDFFormat.JSON_LD
        ]
        
        # Remove the failed format from alternatives
        if failed_format in alternatives:
            alternatives.remove(failed_format)
        
        for alt_format in alternatives:
            try:
                rdflib_format = self.format_handler.get_rdflib_format(alt_format)
                logger.debug(f"Trying alternative format {rdflib_format} for {source_name}")
                
                # Clear graph before retrying
                graph = Graph()
                graph.parse(data=data, format=rdflib_format)
                
                logger.info(f"Successfully parsed {len(graph)} triples with format {rdflib_format}")
                return graph
                
            except Exception as e:
                logger.debug(f"Alternative format {rdflib_format} also failed: {e}")
                continue
        
        raise RDFParsingError(f"Failed to parse {source_name} with any supported format")
    
    def parse_to_dataset(self, sources: list, 
                        format_hints: Optional[Dict[str, str]] = None) -> Dataset:
        """
        Parse multiple RDF sources into a dataset with named graphs.
        
        Args:
            sources: List of file paths or URLs
            format_hints: Optional dictionary mapping sources to format hints
            
        Returns:
            RDFLib Dataset with named graphs
        """
        format_hints = format_hints or {}
        dataset = Dataset()
        
        for source in sources:
            try:
                # Parse individual source
                format_hint = format_hints.get(str(source))
                graph = self.parse(source, format_hint)
                
                # Add to dataset as named graph
                source_uri = rdflib.URIRef(str(source))
                dataset.add_graph(graph, context=source_uri)
                
                logger.info(f"Added graph for {source} with {len(graph)} triples")
                
            except Exception as e:
                logger.error(f"Failed to parse source {source}: {e}")
                # Continue with other sources
                continue
        
        return dataset
