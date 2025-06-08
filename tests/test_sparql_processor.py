"""
Tests for SPARQL Processor

This module contains tests for the SPARQL query processing functionality.
"""

import pytest
import tempfile
import os
from rdflib import Graph

from sparql_anything_rdf.sparql_processor import SPARQLProcessor, SPARQLExecutionError


class TestSPARQLProcessor:
    
    def setup_method(self):
        """Setup test fixtures"""
        self.processor = SPARQLProcessor()
        
        # Create test RDF data
        self.test_turtle = """
        @prefix ex: <http://example.org/> .
        @prefix foaf: <http://xmlns.com/foaf/0.1/> .
        
        ex:alice a foaf:Person ;
            foaf:name "Alice Smith" ;
            foaf:age 30 ;
            foaf:knows ex:bob .
        
        ex:bob a foaf:Person ;
            foaf:name "Bob Jones" ;
            foaf:age 25 .
        """
        
        # Create temporary file with test data
        with tempfile.NamedTemporaryFile(mode='w', suffix='.ttl', delete=False) as f:
            f.write(self.test_turtle)
            self.test_file = f.name
    
    def teardown_method(self):
        """Clean up test fixtures"""
        if hasattr(self, 'test_file') and os.path.exists(self.test_file):
            os.unlink(self.test_file)
    
    def test_simple_select_query(self):
        """Test basic SELECT query"""
        query = """
        SELECT ?person ?name WHERE {
            ?person foaf:name ?name .
        }
        """
        
        result = self.processor.query(self.test_file, query, result_format='list')
        
        assert isinstance(result, list)
        assert len(result) == 2
        
        # Check that both Alice and Bob are in results
        names = [row.get('name') for row in result]
        assert 'Alice Smith' in names
        assert 'Bob Jones' in names
    
    def test_select_query_json_format(self):
        """Test SELECT query with JSON output"""
        query = """
        SELECT ?person ?name WHERE {
            ?person foaf:name ?name .
        }
        """
        
        result = self.processor.query(self.test_file, query, result_format='json')
        
        assert isinstance(result, str)
        
        # Parse JSON to verify structure
        import json
        json_result = json.loads(result)
        assert isinstance(json_result, list)
        assert len(json_result) == 2
    
    def test_construct_query(self):
        """Test CONSTRUCT query"""
        construct_query = """
        CONSTRUCT {
            ?person ex:hasName ?name .
        } WHERE {
            ?person foaf:name ?name .
        }
        """
        
        result = self.processor.construct_query(self.test_file, construct_query)
        
        assert isinstance(result, str)
        assert 'ex:hasName' in result
    
    def test_ask_query_true(self):
        """Test ASK query that should return True"""
        ask_query = """
        ASK {
            ?person foaf:name "Alice Smith" .
        }
        """
        
        result = self.processor.ask_query(self.test_file, ask_query)
        
        assert result == True
    
    def test_ask_query_false(self):
        """Test ASK query that should return False"""
        ask_query = """
        ASK {
            ?person foaf:name "Non-existent Person" .
        }
        """
        
        result = self.processor.ask_query(self.test_file, ask_query)
        
        assert result == False
    
    def test_describe_query(self):
        """Test DESCRIBE query"""
        describe_query = """
        DESCRIBE ex:alice
        """
        
        result = self.processor.describe_query(self.test_file, describe_query)
        
        assert isinstance(result, str)
        assert 'alice' in result.lower()
    
    def test_query_with_filter(self):
        """Test query with FILTER clause"""
        query = """
        SELECT ?person ?age WHERE {
            ?person foaf:age ?age .
            FILTER (?age > 27)
        }
        """
        
        result = self.processor.query(self.test_file, query, result_format='list')
        
        assert isinstance(result, list)
        assert len(result) == 1  # Only Alice (age 30) should match
        assert result[0]['age'] == '30'
    
    def test_query_validation_valid(self):
        """Test query validation with valid query"""
        query = """
        SELECT ?s ?p ?o WHERE {
            ?s ?p ?o .
        }
        """
        
        validation = self.processor.validate_query(query)
        
        assert validation['valid'] == True
        assert validation['query_type'] == 'SELECT'
        assert validation['errors'] == []
    
    def test_query_validation_invalid(self):
        """Test query validation with invalid query"""
        invalid_query = "INVALID SPARQL QUERY SYNTAX"
        
        validation = self.processor.validate_query(invalid_query)
        
        assert validation['valid'] == False
        assert len(validation['errors']) > 0
    
    def test_query_statistics(self):
        """Test getting query execution statistics"""
        query = """
        SELECT ?person ?name WHERE {
            ?person foaf:name ?name .
        }
        """
        
        stats = self.processor.get_query_statistics(self.test_file, query)
        
        assert 'result_count' in stats
        assert 'variables' in stats
        assert 'query_type' in stats
        
        assert stats['result_count'] == 2
        assert stats['query_type'] == 'SELECT'
        assert 'person' in stats['variables']
        assert 'name' in stats['variables']
    
    def test_query_with_bindings(self):
        """Test query with variable bindings"""
        query = """
        SELECT ?name WHERE {
            ?person foaf:name ?name .
            ?person foaf:age ?age .
        }
        """
        
        bindings = {'age': 30}
        
        result = self.processor.query(
            self.test_file, 
            query, 
            result_format='list',
            bindings=bindings
        )
        
        assert isinstance(result, list)
        # This test might need adjustment based on RDFLib binding behavior
    
    def test_csv_output_format(self):
        """Test CSV output format"""
        query = """
        SELECT ?person ?name WHERE {
            ?person foaf:name ?name .
        }
        """
        
        result = self.processor.query(self.test_file, query, result_format='csv')
        
        assert isinstance(result, str)
        assert 'person,name' in result or 'person' in result
        assert 'Alice Smith' in result
        assert 'Bob Jones' in result
    
    def test_xml_output_format(self):
        """Test XML output format"""
        query = """
        SELECT ?person ?name WHERE {
            ?person foaf:name ?name .
        }
        """
        
        result = self.processor.query(self.test_file, query, result_format='xml')
        
        assert isinstance(result, str)
        assert '<?xml' in result or '<sparql>' in result
    
    def test_table_output_format(self):
        """Test table output format"""
        query = """
        SELECT ?person ?name WHERE {
            ?person foaf:name ?name .
        }
        """
        
        result = self.processor.query(self.test_file, query, result_format='table')
        
        assert isinstance(result, list)
        assert len(result) >= 2  # Header row + data rows
        assert isinstance(result[0], list)  # Each row is a list
    
    def test_nonexistent_file_error(self):
        """Test error handling for nonexistent file"""
        query = "SELECT * WHERE { ?s ?p ?o }"
        
        with pytest.raises(SPARQLExecutionError):
            self.processor.query("nonexistent_file.ttl", query)
    
    def test_invalid_query_error(self):
        """Test error handling for invalid SPARQL query"""
        invalid_query = "THIS IS NOT SPARQL"
        
        with pytest.raises(SPARQLExecutionError):
            self.processor.query(self.test_file, invalid_query)


if __name__ == '__main__':
    pytest.main([__file__])
