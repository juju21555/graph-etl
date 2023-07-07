import os
import csv
import logging

from typing import List, Dict, Union, Literal

import yaml
from neo4j import GraphDatabase

from .loader import Loader

class Neo4JLoader(Loader):

    def type_mapping(prop):
        if "Utf8" in prop:
            return "string"
        elif "Float" in prop:
            return "float"
        elif "Int" in prop:
            return "int"
        elif "Datetime" in prop:
            return "local datetime"
        elif "Date" in prop:
            return "date"
        elif "Boolean" in prop:
            return "boolean"
        return "string"

    def csv_mapping(prop):
        if prop.startswith("List"):
            return f"type: '{Neo4JLoader.type_mapping(prop[5:-1])}', array: true"
        else:
            return f"type: '{Neo4JLoader.type_mapping(prop)}', nullValues:[''] "
    
    def __init__(
        self,
        node_finding_strategy: Union[Literal["match"], Literal["create"]] = "match",
        **kwargs
    ):
        """
        Loader object for Neo4J

        Parameters
        ----------
        node_finding_strategy : one of ``"match"`` or ``"create"``
            - if `"match"`: create edges only if nodes of both ends are found in the graph
            - if `"create"`: create edges and create nodes if nodes are not found in the graph
            
        **kwargs : optional
            Everything in kwargs argument will be passed to create a ``Driver`` from ``neo4j.GraphDatabase``
            
        Examples
        --------

        >>> loader = Neo4JLoader(url="bolt://localhost:7687", username="neo4j", password="password")
        """
        if "host" in kwargs:
            kwargs["url"] = kwargs["host"]
        if "uri" in kwargs:
            kwargs["url"] = kwargs["uri"]
        
        config_ = {
            "url": "bolt://localhost:7687",
            "username": "neo4j",
            "password": "password",
            "database": "neo4j"
        }
        
        if kwargs:
            for k, v in kwargs.items():
                config_[k] = v
        elif os.path.exists("./output/config.yaml"):
            with open("./output/config.yaml", "r") as config_file:
                config_ = yaml.safe_load(config_file.read())
            
        config = config_
        with open("./output/config.yaml", "w+") as config_file:
            yaml.dump(config, config_file)
            
        self.graph = GraphDatabase.driver(
            uri=config["url"], 
            auth=(config["username"], config["password"]), 
            database=config["database"]
        )
        
        if self.graph is None:
            logging.error("No Neo4j instance found, check config at : ./output/config.yaml")
            raise ConnectionError("No Neo4j instance found, check config at : ./output/config.yaml")
        
        self.node_finding_strategy = node_finding_strategy
        if self.node_finding_strategy not in ("match", "create"):
            raise ValueError("`node_finding_strategy` must be either 'match' or 'create'")
    
        # for source in store._configs['nodes']:
        #     if clear_source is True or (clear_source and source in clear_source):
        #         graph.run(f"""
        #             MATCH (n)
        #             WHERE '{source}' in n.sources
        #             CALL {{
        #                 WITH n
        #                 DETACH DELETE n
        #             }} IN TRANSACTIONS OF 1000 ROWS
        #         """)
            
    def load_nodes(
        self,
        file_path: str,
        label: str,
        source: str,
        metadatas: Dict,
        properties_type: Dict[str, str],
        constraints: List[str],
        indexs: List[str]
    ) -> int:
        """
        Loading nodes and metadata in Neo4J\n
        /!\ Warning, you shouldn't use this function on your own\n
        /!\ The ETL Tool will do it for you

        Parameters
        ----------
        file_path : str
            Path of the file containing all nodes data
        label : str
            A string with the label of the node to load
        source: str
            The name of the source from where the data come from
        metadatas: Dict
            Metadatas on nodes
        properties_type: Dict[str, str]
            A dict object that maps each property name to their data type
        constraints: List[str]
            A sequence of property to put a unique constraint when loaded in the database
        indexs: List[str]
            A sequence of property to put an index when loaded in the database
            
        Examples
        --------
        Calling ``load_nodes`` function

        >>> load_nodes(
            "./output/nodes/ChEMBL_Molecule.csv", 
            "Molecule",
            "ChEMBL",
            {"licence": "CC-BY-4.0"},
            {"id": "Utf8", "name": "Utf8", ...},
            ["id"],
            ["name"],
            neo4j_connection
        )
        """
        
        file_path = os.path.abspath(f"./output/nodes/{file_path}").replace('\\', '/')
        if file_path[0] == '/':
            file_path = file_path[1:]
        
        prop_mapped = ",".join(f"{k}: {{ {Neo4JLoader.csv_mapping(property)} }}" for k, property in properties_type.items())
        loader_options = f"{{sep: ';', arraySep: '|', escapeChar:'NONE', mapping : {{ {prop_mapped} }} }}"
        
        
        QUERY = f"""
        CALL apoc.periodic.iterate(
            "CALL apoc.load.csv('file:/{file_path}', {loader_options}) YIELD map as row WHERE row.id IS NOT NULL RETURN row",
            "MERGE (n:{label} {{id: row.id}}) 
            ON MATCH SET n.sources = n.sources + '{source}'
            ON CREATE SET n.sources = ['{source}']
            SET n += row",
            {{batchSize: 50000, iterateList: true, parallel: false}}
        )"""
        
        with self.graph as g:
            
            for constraint in constraints:
                g.execute_query(
                    f"""CREATE CONSTRAINT {constraint}_{label} IF NOT EXISTS 
                        FOR (n:{label}) REQUIRE n.{constraint} IS UNIQUE"""
                )
                
            for index in indexs:
                g.execute_query(
                    f"""CREATE RANGE INDEX {index}_{label} IF NOT EXISTS 
                        FOR (n:{label}) ON (n.{index})"""
                )
                
            res = g.execute_query(QUERY)
        
        return res[0][0]['updateStatistics']['nodesCreated']
        
    def load_edges(
        self,
        file_path: str,
        edge_type: str,
        start: str,
        end: str,
        source: str,
        metadatas: Dict,
        properties_type: Dict[str, str]
    ) -> int:
        """
        Loading edges and metadata in Neo4J\n
        /!\ Warning, you shouldn't use this function on your own\n
        /!\ The ETL Tool will do it for you

        Parameters
        ----------
        file_path : str
            Path of the files containing all edges data
        edge_type : str
            A string with the type of the edge to load
        start : str
            A string of the form `Concept`:`property` to start the relationship
        end : str
            A string of the form `Concept`:`property` to end the relationship
        source: str
            The name of the source from where the data come from
        metadatas: Dict
            Metadatas on edges
        properties_type: Dict[str, str]
            A dict object that maps each property name to their data type
            
        Examples
        --------
        Constructing a EdgeLoader object:

        >>> score_opentarget = EdgeLoader(
            "./output/edges/GO_GOis_aGO.csv", 
            "is_a",
            "GO:id",
            "GO:id",
            "GO",
            {"licence": "CC-BY-4.0"},
            {"start": "Utf8", "end": "Utf8"},
            neo4j_connection
        )
        """
        
        file_path = os.path.abspath(f"./output/edges/{file_path}").replace('\\', '/')
        if file_path[0] == '/':
            file_path = file_path[1:]

        trailing_slash = "/" if ":/" not in file_path else ""

        with open(trailing_slash+file_path) as f:
            csv_reader = csv.reader(f, delimiter=';')
            header = next(csv_reader)
            header.remove("start")
            header.remove("end")

        start_label, start_id = start.split(':')
        end_label, end_id = end.split(':')

        bonus_properties = " ".join(f", {property}: row.{property}" for property in header)
        edges_properties = f"{{ source: '{source}' {bonus_properties} }}"

        prop_mapped = ",".join(f"{k}: {{ {Neo4JLoader.csv_mapping(property)} }}" for k, property in properties_type.items())
        loader_options = f"{{sep: ';', arraySep: '|', escapeChar:'NONE', mapping : {{ {prop_mapped} }} }}"
        
        row_start = 'toInteger(row.start)' if Neo4JLoader.type_mapping(properties_type['start'])=='int' else 'toString(row.start)'
        row_end = 'toInteger(row.end)' if Neo4JLoader.type_mapping(properties_type['end'])=='int' else f'toString(row.end)'

        if self.node_finding_strategy == "create":
            NODE_FINDING_STRATEGY = f"""
            MERGE (n:{start_label} {{{start_id}: {row_start} }})
                ON CREATE SET n :BlankNode
            MERGE (m:{end_label} {{{end_id}: {row_end} }}) 
                ON CREATE SET m :BlankNode
            """
        else:
            NODE_FINDING_STRATEGY = f"""
            MATCH (n:{start_label} {{{start_id}: {row_start} }})
            MATCH (m:{end_label} {{{end_id}: {row_end} }}) 
            """

        QUERY = f"""
        CALL apoc.periodic.iterate(
            "CALL apoc.load.csv('file:/{file_path}', {loader_options}) 
            YIELD map as row
            WHERE row.start <> '' AND row.end <> ''
            RETURN row",
            "{NODE_FINDING_STRATEGY}
            CREATE (n)-[:{edge_type} {edges_properties}]->(m)",
            {{batchSize: 20000}}
        )
        """

        with self.graph as g:
            res = g.execute_query(QUERY)
        
        return res[0][0]['updateStatistics']['relationshipsCreated']
