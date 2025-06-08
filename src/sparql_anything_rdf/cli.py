"""
Command Line Interface for SPARQL Anything RDF

This module provides a command-line interface for the RDF processing functionality.
"""

import argparse
import sys
import logging
from pathlib import Path
from typing import Optional, List

from .rdf_parser import RDFParser
from .rdf_to_rdf import RDFToRDF
from .sparql_processor import SPARQLProcessor
from .format_handler import FormatHandler


def setup_logging(verbose: bool = False):
    """Setup logging configuration"""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='SPARQL Anything RDF - Process RDF data with SPARQL queries'
    )
    
    # Input options
    parser.add_argument(
        '-i', '--input',
        type=str,
        help='Input RDF file or URL'
    )
    
    parser.add_argument(
        '--inputs',
        nargs='+',
        help='Multiple input RDF files'
    )
    
    parser.add_argument(
        '--format',
        type=str,
        help='Input format hint (turtle, rdf_xml, json_ld, etc.)'
    )
    
    # Query options
    parser.add_argument(
        '-q', '--query',
        type=str,
        help='SPARQL query string'
    )
    
    parser.add_argument(
        '--query-file',
        type=str,
        help='File containing SPARQL query'
    )
    
    # Output options
    parser.add_argument(
        '-o', '--output',
        type=str,
        help='Output file path'
    )
    
    parser.add_argument(
        '--output-format',
        type=str,
        default='json',
        choices=['json', 'xml', 'csv', 'turtle', 'table'],
        help='Output format for query results'
    )
    
    # Conversion options
    parser.add_argument(
        '--convert',
        action='store_true',
        help='Convert RDF between formats'
    )
    
    parser.add_argument(
        '--target-format',
        type=str,
        default='turtle',
        help='Target format for conversion'
    )
    
    # Analysis options
    parser.add_argument(
        '--validate',
        action='store_true',
        help='Validate RDF data'
    )
    
    parser.add_argument(
        '--stats',
        action='store_true',
        help='Show RDF statistics'
    )
    
    parser.add_argument(
        '--schema',
        action='store_true',
        help='Extract schema information'
    )
    
    # General options
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose output'
    )
    
    parser.add_argument(
        '--version',
        action='version',
        version='SPARQL Anything RDF 1.0.0'
    )
    
    return parser.parse_args()


def load_query_from_file(query_file: str) -> str:
    """Load SPARQL query from file"""
    try:
        with open(query_file, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        print(f"Error loading query file {query_file}: {e}")
        sys.exit(1)


def save_output(content: str, output_file: Optional[str] = None):
    """Save content to file or print to stdout"""
    if output_file:
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"Output saved to {output_file}")
        except Exception as e:
            print(f"Error saving output to {output_file}: {e}")
            sys.exit(1)
    else:
        print(content)


def main():
    """Main CLI entry point"""
    args = parse_arguments()
    setup_logging(args.verbose)
    
    logger = logging.getLogger(__name__)
    
    try:
        # Initialize components
        parser = RDFParser()
        converter = RDFToRDF()
        processor = SPARQLProcessor()
        format_handler = FormatHandler()
        
        # Determine input source
        if args.input:
            source = args.input
        elif args.inputs:
            source = args.inputs
        else:
            print("Error: No input specified. Use --input or --inputs")
            sys.exit(1)
        
        # Handle different operations
        if args.convert:
            # Convert RDF between formats
            logger.info("Converting RDF format")
            
            if isinstance(source, list):
                print("Error: Format conversion supports only single input file")
                sys.exit(1)
            
            # Parse and convert
            graph = parser.parse(source, args.format)
            result = converter.serialize(graph, args.target_format)
            
            save_output(result, args.output)
            
        elif args.validate:
            # Validate RDF data
            logger.info("Validating RDF data")
            
            if isinstance(source, list):
                print("Error: Validation supports only single input file")
                sys.exit(1)
            
            # Parse and validate
            graph = parser.parse(source, args.format)
            validation_result = converter.validate_graph(graph)
            
            # Format validation result
            result = f"Validation Results:\n"
            result += f"Total triples: {validation_result['total_triples']}\n"
            result += f"Unique subjects: {validation_result['unique_subjects']}\n"
            result += f"Unique predicates: {validation_result['unique_predicates']}\n"
            result += f"Unique objects: {validation_result['unique_objects']}\n"
            result += f"Blank nodes: {validation_result['blank_nodes']}\n"
            result += f"Literals: {validation_result['literals']}\n"
            result += f"URIs: {validation_result['uris']}\n"
            
            if validation_result['errors']:
                result += f"Errors: {', '.join(validation_result['errors'])}\n"
            else:
                result += "No errors found\n"
            
            save_output(result, args.output)
            
        elif args.stats:
            # Show statistics
            logger.info("Generating RDF statistics")
            
            if isinstance(source, list):
                print("Error: Statistics supports only single input file")
                sys.exit(1)
            
            # Parse and analyze
            graph = parser.parse(source, args.format)
            stats = converter.validate_graph(graph)
            
            # Format statistics
            import json
            result = json.dumps(stats, indent=2)
            
            save_output(result, args.output)
            
        elif args.schema:
            # Extract schema
            logger.info("Extracting schema information")
            
            if isinstance(source, list):
                print("Error: Schema extraction supports only single input file")
                sys.exit(1)
            
            # Parse and extract schema
            graph = parser.parse(source, args.format)
            schema_graph = converter.extract_schema(graph)
            result = converter.serialize(schema_graph, args.target_format)
            
            save_output(result, args.output)
            
        else:
            # Execute SPARQL query (default operation)
            
            # Get query
            if args.query:
                query = args.query
            elif args.query_file:
                query = load_query_from_file(args.query_file)
            else:
                print("Error: No query specified. Use --query or --query-file")
                sys.exit(1)
            
            logger.info("Executing SPARQL query")
            
            # Execute query
            if isinstance(source, list):
                # Multiple sources
                result = processor.query_multiple(source, query, args.output_format)
            else:
                # Single source
                result = processor.query(source, query, args.output_format)
            
            # Format output based on result type
            if isinstance(result, (dict, list)):
                import json
                output = json.dumps(result, indent=2)
            else:
                output = str(result)
            
            save_output(output, args.output)
        
        logger.info("Operation completed successfully")
        
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Operation failed: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
