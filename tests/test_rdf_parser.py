"""
Tests for RDF Parser

This module contains tests for the RDF parsing functionality.
"""

import pytest
import tempfile
import os
from pathlib import Path
from rdflib import Graph

from sparql_anything_rdf.rdf_parser import RDFParser, RDFParsingError


class TestRDFParser:
    
    def setup_method(self):
        """Setup test fixtures"""
        self.parser = RDFParser()
    
    def test_parse_turtle_string(self):
        """Test parsing Turtle format from string data"""
        turtle_data = """
        @prefix ex: <http://example.org/> .
        ex:subject ex:predicate "object" .
        ex:subject ex:hasNumber 42 .
        """
        
        # Create temporary file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.ttl', delete=False) as f:
            f.write(turtle_data)
            temp_file = f.name
        
        try:
            # Parse the file
            graph = self.parser.parse(temp_file)
            
            # Verify results
            assert isinstance(graph, Graph)
            assert len(graph) == 2  # Two triples
            
        finally:
            os.unlink(temp_file)
    
    def test_parse_rdf_xml_string(self):
        """Test parsing RDF/XML format"""
        rdf_xml_data = """<?xml version="1.0"?>
        <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                 xmlns:ex="http://example.org/">
            <ex:Thing rdf:about="http://example.org/subject">
                <ex:predicate>object</ex:predicate>
                <ex:hasNumber rdf:datatype="http://www.w3.org/2001/XMLSchema#integer">42</ex:hasNumber>
            </ex:Thing>
        </rdf:RDF>
        """
        
        # Create temporary file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.rdf', delete=False) as f:
            f.write(rdf_xml_data)
            temp_file = f.name
        
        try:
            # Parse the file
            graph = self.parser.parse(temp_file)
            
            # Verify results
            assert isinstance(graph, Graph)
            assert len(graph) >= 2  # At least two triples
            
        finally:
            os.unlink(temp_file)
    
    def test_parse_json_ld_string(self):
        """Test parsing JSON-LD format"""
        json_ld_data = """{
            "@context": {
                "ex": "http://example.org/"
            },
            "@id": "ex:subject",
            "ex:predicate": "object",
            "ex:hasNumber": 42
        }
        """
        
        # Create temporary file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonld', delete=False) as f:
            f.write(json_ld_data)
            temp_file = f.name
        
        try:
            # Parse the file
            graph = self.parser.parse(temp_file)
            
            # Verify results
            assert isinstance(graph, Graph)
            assert len(graph) >= 2  # At least two triples
            
        finally:
            os.unlink(temp_file)
    
    def test_parse_nonexistent_file(self):
        """Test error handling for nonexistent file"""
        with pytest.raises(RDFParsingError):
            self.parser.parse("nonexistent_file.ttl")
    
    def test_parse_with_format_hint(self):
        """Test parsing with explicit format hint"""
        turtle_data = """
        @prefix ex: <http://example.org/> .
        ex:subject ex:predicate "object" .
        """
        
        # Create temporary file with wrong extension
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write(turtle_data)
            temp_file = f.name
        
        try:
            # Parse with format hint
            graph = self.parser.parse(temp_file, format_hint='turtle')
            
            # Verify results
            assert isinstance(graph, Graph)
            assert len(graph) == 1
            
        finally:
            os.unlink(temp_file)
    
    def test_format_detection(self):
        """Test automatic format detection"""
        # Test file extension detection
        assert self.parser._is_url("http://example.org/data.ttl") == True
        assert self.parser._is_url("https://example.org/data.rdf") == True
        assert self.parser._is_url("/local/path/data.ttl") == False
        assert self.parser._is_url("data.ttl") == False
    
    def test_parse_empty_file(self):
        """Test parsing empty file"""
        # Create empty file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.ttl', delete=False) as f:
            temp_file = f.name
        
        try:
            # Parse the empty file
            graph = self.parser.parse(temp_file)
            
            # Verify results
            assert isinstance(graph, Graph)
            assert len(graph) == 0
            
        finally:
            os.unlink(temp_file)


if __name__ == '__main__':
    pytest.main([__file__])
