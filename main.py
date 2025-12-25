"""
Informatica Mapping XML Parser - Simple Version
Parses Informatica mapping XML and creates in-memory model
"""

import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import List, Dict, Optional
from pathlib import Path
from collections import Counter
import json


@dataclass
class Port:
    """Represents a port in a transformation"""
    name: str
    datatype: str
    precision: Optional[int] = None
    scale: Optional[int] = None
    nullable: str = "NOTNULL"
    port_type: str = "INPUT"  # INPUT, OUTPUT, INPUT/OUTPUT, VARIABLE
    expression: Optional[str] = None
    
    def __repr__(self):
        return f"Port({self.name}, {self.datatype}, {self.port_type})"


@dataclass
class Transformation:
    """Represents an Informatica transformation"""
    name: str
    type: str
    description: Optional[str] = None
    ports: List[Port] = field(default_factory=list)
    properties: Dict[str, str] = field(default_factory=dict)
    
    def __repr__(self):
        return f"Transformation({self.name}, type={self.type}, ports={len(self.ports)})"


@dataclass
class Connection:
    """Represents a connection between transformations"""
    from_transformation: str
    from_port: str
    to_transformation: str
    to_port: str
    
    def __repr__(self):
        return f"{self.from_transformation}.{self.from_port} -> {self.to_transformation}.{self.to_port}"


@dataclass
class Mapping:
    """Represents an Informatica mapping"""
    name: str
    description: Optional[str] = None
    folder: Optional[str] = None
    transformations: List[Transformation] = field(default_factory=list)
    connections: List[Connection] = field(default_factory=list)
    
    def get_transformation_by_name(self, name: str) -> Optional[Transformation]:
        """Get transformation by name"""
        for trans in self.transformations:
            if trans.name == name:
                return trans
        return None
    
    def get_sources(self) -> List[Transformation]:
        """Get all source transformations"""
        return [t for t in self.transformations if t.type == "Source Definition"]
    
    def get_targets(self) -> List[Transformation]:
        """Get all target transformations"""
        return [t for t in self.transformations if t.type == "Target Definition"]
    
    def get_transformation_counts(self) -> Dict[str, int]:
        """Count transformations by type"""
        return dict(Counter(t.type for t in self.transformations))
    
    def get_summary(self) -> Dict:
        """Get mapping summary"""
        return {
            "name": self.name,
            "folder": self.folder,
            "total_transformations": len(self.transformations),
            "total_connections": len(self.connections),
            "sources": len(self.get_sources()),
            "targets": len(self.get_targets()),
            "transformation_counts": self.get_transformation_counts()
        }
    
    def __repr__(self):
        return f"Mapping({self.name}, transformations={len(self.transformations)})"


class InformaticaXMLParser:
    """Parser for Informatica mapping XML files"""
    
    # Mapping of Informatica transformation types
    TRANSFORMATION_TYPES = {
        'Source Definition': 'Source Definition',
        'Source Qualifier': 'Source Qualifier',
        'Target Definition': 'Target Definition',
        'Expression': 'Expression',
        'Filter': 'Filter',
        'Aggregator': 'Aggregator',
        'Joiner': 'Joiner',
        'Lookup Procedure': 'Lookup',
        'Router': 'Router',
        'Sorter': 'Sorter',
        'Update Strategy': 'Update Strategy',
        'Normalizer': 'Normalizer',
        'Rank': 'Rank',
        'Sequence Generator': 'Sequence Generator',
        'Stored Procedure': 'Stored Procedure',
        'Union': 'Union',
    }
    
    def __init__(self, xml_path: str):
        """Initialize parser with XML file path"""
        self.xml_path = Path(xml_path)
        self.tree = None
        self.root = None
        self.namespace = {}
        
    def parse(self) -> Mapping:
        """Parse the XML file and return Mapping object"""
        # Load and parse XML
        self.tree = ET.parse(self.xml_path)
        self.root = self.tree.getroot()
        
        # Extract namespace if present
        self._extract_namespace()
        
        # Find the mapping element
        mapping_elem = self._find_element('.//MAPPING')
        if mapping_elem is None:
            raise ValueError("No MAPPING element found in XML")
        
        # Create mapping object
        mapping = Mapping(
            name=mapping_elem.get('NAME', 'Unknown'),
            description=mapping_elem.get('DESCRIPTION'),
            folder=self._get_folder_name()
        )
        
        # Parse all transformations
        mapping.transformations = self._parse_transformations(mapping_elem)
        
        # Parse all connections
        mapping.connections = self._parse_connections(mapping_elem)
        
        return mapping
    
    def _extract_namespace(self):
        """Extract XML namespace if present"""
        # Informatica XMLs may or may not have namespaces
        if self.root.tag.startswith('{'):
            self.namespace = {'ns': self.root.tag.split('}')[0].strip('{')}
    
    def _find_element(self, path: str):
        """Find element with or without namespace"""
        elem = self.root.find(path, self.namespace)
        if elem is None and self.namespace:
            # Try without namespace
            elem = self.root.find(path)
        return elem
    
    def _find_all_elements(self, path: str):
        """Find all elements with or without namespace"""
        elems = self.root.findall(path, self.namespace)
        if not elems and self.namespace:
            # Try without namespace
            elems = self.root.findall(path)
        return elems
    
    def _get_folder_name(self) -> Optional[str]:
        """Extract folder name from XML"""
        folder_elem = self._find_element('.//FOLDER')
        return folder_elem.get('NAME') if folder_elem is not None else None
    
    def _parse_transformations(self, mapping_elem) -> List[Transformation]:
        """Parse all transformations in the mapping"""
        transformations = []
        
        # Find all transformation elements
        trans_elements = mapping_elem.findall('.//TRANSFORMATION')
        
        for trans_elem in trans_elements:
            trans_type = trans_elem.get('TYPE', 'Unknown')
            trans_name = trans_elem.get('NAME', 'Unknown')
            trans_desc = trans_elem.get('DESCRIPTION')
            
            # Create transformation object
            transformation = Transformation(
                name=trans_name,
                type=self.TRANSFORMATION_TYPES.get(trans_type, trans_type),
                description=trans_desc
            )
            
            # Parse ports
            transformation.ports = self._parse_ports(trans_elem)
            
            # Parse transformation-specific properties
            transformation.properties = self._parse_properties(trans_elem)
            
            transformations.append(transformation)
        
        return transformations
    
    def _parse_ports(self, trans_elem) -> List[Port]:
        """Parse all ports in a transformation"""
        ports = []
        
        # Find all TRANSFORMFIELD elements (ports)
        port_elements = trans_elem.findall('.//TRANSFORMFIELD')
        
        for port_elem in port_elements:
            port = Port(
                name=port_elem.get('NAME', 'Unknown'),
                datatype=port_elem.get('DATATYPE', 'string'),
                precision=self._safe_int(port_elem.get('PRECISION')),
                scale=self._safe_int(port_elem.get('SCALE')),
                nullable=port_elem.get('NULLABLE', 'NOTNULL'),
                port_type=port_elem.get('PORTTYPE', 'INPUT'),
                expression=port_elem.get('EXPRESSION')
            )
            ports.append(port)
        
        return ports
    
    def _parse_properties(self, trans_elem) -> Dict[str, str]:
        """Parse transformation properties"""
        properties = {}
        
        # Get all attributes as properties
        properties.update(trans_elem.attrib)
        
        # Look for additional property elements
        for prop_elem in trans_elem.findall('.//TABLEATTRIBUTE'):
            prop_name = prop_elem.get('NAME')
            prop_value = prop_elem.get('VALUE')
            if prop_name and prop_value:
                properties[prop_name] = prop_value
        
        return properties
    
    def _parse_connections(self, mapping_elem) -> List[Connection]:
        """Parse all connections (links) between transformations"""
        connections = []
        
        # Find all connector elements
        connector_elements = mapping_elem.findall('.//CONNECTOR')
        
        for conn_elem in connector_elements:
            from_field = conn_elem.get('FROMFIELD')
            from_instance = conn_elem.get('FROMINSTANCE')
            to_field = conn_elem.get('TOFIELD')
            to_instance = conn_elem.get('TOINSTANCE')
            
            if all([from_field, from_instance, to_field, to_instance]):
                connection = Connection(
                    from_transformation=from_instance,
                    from_port=from_field,
                    to_transformation=to_instance,
                    to_port=to_field
                )
                connections.append(connection)
        
        return connections
    
    @staticmethod
    def _safe_int(value: Optional[str]) -> Optional[int]:
        """Safely convert string to int"""
        if value is None:
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None


def print_mapping_summary(mapping: Mapping):
    """Print a detailed summary of the mapping"""
    print("=" * 80)
    print(f"MAPPING SUMMARY: {mapping.name}")
    print("=" * 80)
    
    summary = mapping.get_summary()
    
    print(f"\n📁 Folder: {summary['folder']}")
    print(f"📊 Total Transformations: {summary['total_transformations']}")
    print(f"🔗 Total Connections: {summary['total_connections']}")
    print(f"📥 Source Count: {summary['sources']}")
    print(f"📤 Target Count: {summary['targets']}")
    
    print("\n" + "=" * 80)
    print("TRANSFORMATION BREAKDOWN")
    print("=" * 80)
    
    for trans_type, count in sorted(summary['transformation_counts'].items()):
        print(f"  {trans_type:.<50} {count:>3}")
    
    print("\n" + "=" * 80)
    print("SOURCE TRANSFORMATIONS")
    print("=" * 80)
    
    for source in mapping.get_sources():
        print(f"\n  📥 {source.name}")
        print(f"     Type: {source.type}")
        print(f"     Ports: {len(source.ports)}")
        if source.ports:
            print("     Sample Ports:")
            for port in source.ports[:5]:  # Show first 5 ports
                print(f"       - {port.name} ({port.datatype})")
    
    print("\n" + "=" * 80)
    print("TARGET TRANSFORMATIONS")
    print("=" * 80)
    
    for target in mapping.get_targets():
        print(f"\n  📤 {target.name}")
        print(f"     Type: {target.type}")
        print(f"     Ports: {len(target.ports)}")
        if target.ports:
            print("     Sample Ports:")
            for port in target.ports[:5]:  # Show first 5 ports
                print(f"       - {port.name} ({port.datatype})")
    
    print("\n" + "=" * 80)
    print("TRANSFORMATION DETAILS")
    print("=" * 80)
    
    for trans in mapping.transformations:
        if trans.type not in ['Source Definition', 'Target Definition']:
            print(f"\n  🔧 {trans.name}")
            print(f"     Type: {trans.type}")
            print(f"     Ports: {len(trans.ports)}")
            
            # Show expressions if present
            expr_ports = [p for p in trans.ports if p.expression]
            if expr_ports:
                print(f"     Expressions: {len(expr_ports)}")
                for port in expr_ports[:3]:  # Show first 3 expressions
                    print(f"       - {port.name} = {port.expression}")
    
    print("\n" + "=" * 80)
    print("DATA FLOW (First 10 connections)")
    print("=" * 80)
    
    for i, conn in enumerate(mapping.connections[:10], 1):
        print(f"  {i:>2}. {conn}")
    
    if len(mapping.connections) > 10:
        print(f"  ... and {len(mapping.connections) - 10} more connections")
    
    print("\n" + "=" * 80)


def export_to_json(mapping: Mapping, output_path: str):
    """Export mapping to JSON format"""
    data = {
        "name": mapping.name,
        "description": mapping.description,
        "folder": mapping.folder,
        "transformations": [
            {
                "name": t.name,
                "type": t.type,
                "description": t.description,
                "ports": [
                    {
                        "name": p.name,
                        "datatype": p.datatype,
                        "precision": p.precision,
                        "scale": p.scale,
                        "nullable": p.nullable,
                        "port_type": p.port_type,
                        "expression": p.expression
                    }
                    for p in t.ports
                ],
                "properties": t.properties
            }
            for t in mapping.transformations
        ],
        "connections": [
            {
                "from_transformation": c.from_transformation,
                "from_port": c.from_port,
                "to_transformation": c.to_transformation,
                "to_port": c.to_port
            }
            for c in mapping.connections
        ]
    }
    
    with open(output_path, 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f"\n✅ Mapping exported to: {output_path}")


# Example usage
if __name__ == "__main__":
    import sys

    xml_file = 'test.xml'
    
    try:
        # Parse the XML
        print(f"🔄 Parsing Informatica mapping: {xml_file}")
        parser = InformaticaXMLParser(xml_file)
        mapping = parser.parse()
        
        # Print summary
        print_mapping_summary(mapping)
        
        # Export to JSON
        json_output = xml_file.replace('.xml', '_parsed.json')
        export_to_json(mapping, json_output)
        
        print("\n✅ Parsing completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Error parsing XML: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)