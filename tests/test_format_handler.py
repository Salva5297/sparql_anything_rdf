"""
Tests for Format Handler

This module contains tests for the RDF format handling functionality.
"""

import pytest
from sparql_anything_rdf.format_handler import FormatHandler, RDFFormat


class TestFormatHandler:
    
    def setup_method(self):
        """Setup test fixtures"""
        self.handler = FormatHandler()
    
    def test_detect_format_by_extension(self):
        """Test format detection by file extension"""
        test_cases = [
            ('file.ttl', RDFFormat.TURTLE),
            ('file.rdf', RDFFormat.RDF_XML),
            ('file.xml', RDFFormat.RDF_XML),
            ('file.nt', RDFFormat.N_TRIPLES),
            ('file.jsonld', RDFFormat.JSON_LD),
            ('file.trig', RDFFormat.TRIG),
            ('file.nq', RDFFormat.N_QUADS),
            ('file.owl', RDFFormat.OWL_XML),
        ]
        
        for filename, expected_format in test_cases:
            detected = self.handler.detect_format(filename)
            assert detected == expected_format, f"Failed for {filename}"
    
    def test_detect_format_by_content_type(self):
        """Test format detection by MIME type"""
        test_cases = [
            ('text/turtle', RDFFormat.TURTLE),
            ('application/rdf+xml', RDFFormat.RDF_XML),
            ('application/n-triples', RDFFormat.N_TRIPLES),
            ('application/ld+json', RDFFormat.JSON_LD),
            ('text/trig', RDFFormat.TRIG),
            ('application/n-quads', RDFFormat.N_QUADS),
            ('application/owl+xml', RDFFormat.OWL_XML),
        ]
        
        for content_type, expected_format in test_cases:
            detected = self.handler.detect_format('unknown.file', content_type)
            assert detected == expected_format, f"Failed for {content_type}"
    
    def test_detect_format_with_charset(self):
        """Test format detection with charset in content type"""
        content_type = 'text/turtle; charset=utf-8'
        detected = self.handler.detect_format('unknown.file', content_type)
        assert detected == RDFFormat.TURTLE
    
    def test_get_rdflib_format(self):
        """Test RDFLib format string mapping"""
        test_cases = [
            (RDFFormat.TURTLE, 'turtle'),
            (RDFFormat.RDF_XML, 'xml'),
            (RDFFormat.N_TRIPLES, 'nt'),
            (RDFFormat.JSON_LD, 'json-ld'),
            (RDFFormat.TRIG, 'trig'),
            (RDFFormat.N_QUADS, 'nquads'),
        ]
        
        for rdf_format, expected_rdflib_format in test_cases:
            result = self.handler.get_rdflib_format(rdf_format)
            assert result == expected_rdflib_format
    
    def test_get_mime_types(self):
        """Test getting all supported MIME types"""
        mime_types = self.handler.get_mime_types()
        
        assert 'text/turtle' in mime_types
        assert 'application/rdf+xml' in mime_types
        assert 'application/ld+json' in mime_types
        assert len(mime_types) > 5
    
    def test_get_extensions(self):
        """Test getting all supported file extensions"""
        extensions = self.handler.get_extensions()
        
        assert '.ttl' in extensions
        assert '.rdf' in extensions
        assert '.jsonld' in extensions
        assert len(extensions) > 5
    
    def test_get_format_info_default(self):
        """Test getting format information for default format"""
        info = self.handler.get_format_info('default')
        
        assert 'config' in info
        assert 'description' in info
        assert info['config']['format'] == 'default'
        assert isinstance(info['description'], str)
    
    def test_get_format_info_turtle(self):
        """Test getting format information for Turtle format"""
        info = self.handler.get_format_info('turtle')
        
        assert 'config' in info
        assert 'description' in info
        assert info['config']['format'] == 'turtle'
        assert info['config']['rdflib_format'] == 'turtle'
        assert 'text/turtle' in info['config']['mime_types']
        assert '.ttl' in info['config']['extensions']
    
    def test_get_format_info_unknown(self):
        """Test getting format information for unknown format"""
        info = self.handler.get_format_info('unknown_format')
        
        assert 'config' in info
        assert 'description' in info
        assert info['config']['format'] == 'default'  # Falls back to default
    
    def test_validate_format_config_valid(self):
        """Test format configuration validation with valid config"""
        config = {
            'format': 'turtle',
            'rdflib_format': 'turtle'
        }
        
        result = self.handler.validate_format_config('turtle', config)
        assert result == True
    
    def test_validate_format_config_invalid(self):
        """Test format configuration validation with invalid format"""
        config = {
            'format': 'turtle'
        }
        
        result = self.handler.validate_format_config('invalid_format', config)
        assert result == False
    
    def test_validate_format_config_strict(self):
        """Test strict format configuration validation"""
        config = {}  # Missing required 'format' key
        
        result = self.handler.validate_format_config('turtle', config, strict=True)
        assert result == False
        
        # Valid config should pass
        valid_config = {'format': 'turtle'}
        result = self.handler.validate_format_config('turtle', valid_config, strict=True)
        assert result == True
    
    def test_get_content_negotiation_headers(self):
        """Test HTTP content negotiation headers"""
        headers = self.handler.get_content_negotiation_headers()
        
        assert 'Accept' in headers
        assert 'User-Agent' in headers
        
        accept_header = headers['Accept']
        assert 'application/rdf+xml' in accept_header
        assert 'text/turtle' in accept_header
        assert 'application/ld+json' in accept_header


if __name__ == '__main__':
    pytest.main([__file__])
