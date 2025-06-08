"""
Format Handler for SPARQL Anything RDF

This module handles different RDF formats and configurations,
similar to the Java implementation's format handling.
"""

from typing import Dict, List, Any, Optional, Set
from enum import Enum
import mimetypes
from pathlib import Path
import requests


class RDFFormat(Enum):
    """Supported RDF formats"""
    RDF_XML = "rdf_xml"
    TURTLE = "turtle"
    N_TRIPLES = "n_triples"
    JSON_LD = "json_ld"
    TRIG = "trig"
    N_QUADS = "n_quads"
    TRIX = "trix"
    RDF_THRIFT = "rdf_thrift"
    OWL_XML = "owl_xml"
    DEFAULT = "default"


class FormatHandler:
    """Handle different RDF formats and their configurations"""
    
    # MIME types to RDF format mapping
    MIME_TYPE_MAPPING = {
        'application/rdf+xml': RDFFormat.RDF_XML,
        'application/xml': RDFFormat.RDF_XML,
        'text/xml': RDFFormat.RDF_XML,
        'text/turtle': RDFFormat.TURTLE,
        'application/turtle': RDFFormat.TURTLE,
        'application/x-turtle': RDFFormat.TURTLE,
        'application/n-triples': RDFFormat.N_TRIPLES,
        'text/plain': RDFFormat.N_TRIPLES,
        'application/ld+json': RDFFormat.JSON_LD,
        'application/json': RDFFormat.JSON_LD,
        'text/trig': RDFFormat.TRIG,
        'application/trig': RDFFormat.TRIG,
        'application/n-quads': RDFFormat.N_QUADS,
        'text/n-quads': RDFFormat.N_QUADS,
        'application/trix+xml': RDFFormat.TRIX,
        'application/rdf+thrift': RDFFormat.RDF_THRIFT,
        'application/owl+xml': RDFFormat.OWL_XML,
    }
    
    # File extensions to RDF format mapping
    EXTENSION_MAPPING = {
        '.rdf': RDFFormat.RDF_XML,
        '.xml': RDFFormat.RDF_XML,
        '.ttl': RDFFormat.TURTLE,
        '.nt': RDFFormat.N_TRIPLES,
        '.jsonld': RDFFormat.JSON_LD,
        '.trig': RDFFormat.TRIG,
        '.nq': RDFFormat.N_QUADS,
        '.trix': RDFFormat.TRIX,
        '.trdf': RDFFormat.RDF_THRIFT,
        '.owl': RDFFormat.OWL_XML,
    }
    
    # RDFLib format strings
    RDFLIB_FORMAT_MAPPING = {
        RDFFormat.RDF_XML: 'xml',
        RDFFormat.TURTLE: 'turtle',
        RDFFormat.N_TRIPLES: 'nt',
        RDFFormat.JSON_LD: 'json-ld',
        RDFFormat.TRIG: 'trig',
        RDFFormat.N_QUADS: 'nquads',
        RDFFormat.TRIX: 'trix',
        RDFFormat.OWL_XML: 'xml',
    }
    
    # Format descriptions
    FORMAT_DESCRIPTIONS = {
        RDFFormat.DEFAULT: "Default RDF format (automatically detected)",
        RDFFormat.RDF_XML: "RDF/XML format - W3C standard XML serialization of RDF",
        RDFFormat.TURTLE: "Turtle format - Terse RDF Triple Language",
        RDFFormat.N_TRIPLES: "N-Triples format - Simple line-based format",
        RDFFormat.JSON_LD: "JSON-LD format - JSON for Linked Data",
        RDFFormat.TRIG: "TriG format - Turtle with named graphs",
        RDFFormat.N_QUADS: "N-Quads format - N-Triples with named graphs",
        RDFFormat.TRIX: "TriX format - Triples in XML",
        RDFFormat.RDF_THRIFT: "RDF Thrift format - Binary RDF serialization",
        RDFFormat.OWL_XML: "OWL/XML format - XML serialization of OWL ontologies",
    }
    
    @classmethod
    def detect_format(cls, source: str, content_type: Optional[str] = None) -> RDFFormat:
        """
        Detect RDF format from source file path or URL and content type.
        
        Args:
            source: File path or URL
            content_type: MIME type from HTTP headers
            
        Returns:
            Detected RDF format
        """
        # Try content type first (for HTTP responses)
        if content_type:
            # Handle content type with charset
            if ';' in content_type:
                content_type = content_type.split(';')[0].strip()
            
            if content_type in cls.MIME_TYPE_MAPPING:
                return cls.MIME_TYPE_MAPPING[content_type]
        
        # Try file extension
        path = Path(source)
        extension = path.suffix.lower()
        
        if extension in cls.EXTENSION_MAPPING:
            return cls.EXTENSION_MAPPING[extension]
        
        # Default to RDF/XML if detection fails
        return RDFFormat.RDF_XML
    
    @classmethod
    def get_rdflib_format(cls, rdf_format: RDFFormat) -> str:
        """
        Get the RDFLib format string for a given RDF format.
        
        Args:
            rdf_format: RDF format enum
            
        Returns:
            RDFLib format string
        """
        return cls.RDFLIB_FORMAT_MAPPING.get(rdf_format, 'xml')
    
    @classmethod
    def get_mime_types(cls) -> Set[str]:
        """
        Get all supported MIME types.
        
        Returns:
            Set of supported MIME types
        """
        return set(cls.MIME_TYPE_MAPPING.keys())
    
    @classmethod
    def get_extensions(cls) -> Set[str]:
        """
        Get all supported file extensions.
        
        Returns:
            Set of supported file extensions
        """
        return set(cls.EXTENSION_MAPPING.keys())
    
    @classmethod
    def get_format_info(cls, format_name: str) -> Dict[str, Any]:
        """
        Get configuration and description for a format.
        
        Args:
            format_name: Name of the format
            
        Returns:
            Dictionary with format configuration and description
        """
        try:
            rdf_format = RDFFormat(format_name.lower())
        except ValueError:
            rdf_format = RDFFormat.DEFAULT
        
        config = {
            'format': rdf_format.value,
            'rdflib_format': cls.get_rdflib_format(rdf_format),
            'mime_types': [k for k, v in cls.MIME_TYPE_MAPPING.items() if v == rdf_format],
            'extensions': [k for k, v in cls.EXTENSION_MAPPING.items() if v == rdf_format],
        }
        
        description = cls.FORMAT_DESCRIPTIONS.get(rdf_format, "Unknown RDF format")
        
        return {
            'config': config,
            'description': description
        }
    
    @classmethod
    def validate_format_config(cls, format_name: str, config: Dict[str, Any], strict: bool = True) -> bool:
        """
        Validate format configuration.
        
        Args:
            format_name: Name of the format
            config: Configuration dictionary
            strict: Whether to use strict validation
            
        Returns:
            True if configuration is valid
        """
        try:
            rdf_format = RDFFormat(format_name.lower())
            
            # Basic validation - ensure format is supported
            if rdf_format not in cls.FORMAT_DESCRIPTIONS:
                return False
                
            # Additional validation could be added here
            if strict:
                # Strict validation rules
                required_keys = ['format']
                for key in required_keys:
                    if key not in config:
                        return False
            
            return True
            
        except (ValueError, KeyError):
            return False
    
    @classmethod
    def get_content_negotiation_headers(cls) -> Dict[str, str]:
        """
        Get HTTP headers for content negotiation with RDF sources.
        
        Returns:
            Dictionary of HTTP headers
        """
        # Prioritize common RDF formats
        accept_header = (
            "application/rdf+xml, "
            "text/turtle, "
            "application/ld+json, "
            "application/n-triples, "
            "text/trig, "
            "application/n-quads, "
            "*/*;q=0.1"
        )
        
        return {
            'Accept': accept_header,
            'User-Agent': 'SPARQL-Anything-RDF/1.0.0'
        }
