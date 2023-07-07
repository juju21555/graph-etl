import os
import csv
import logging

from typing import List, Dict

import yaml
import pyTigerGraph

from .loader import Loader

class TigerGraphLoader(Loader):

    def type_mapping(prop):
        if "Utf8" in prop:
            return "STRING"
        elif "Float" in prop:
            return "FLOAT"
        elif "Int" in prop:
            return "INT"
        elif "Date" in prop:
            return "DATE"
        elif "Boolean" in prop:
            return "BOOL"
        return "STRING"

    def csv_mapping(prop):
        if prop.startswith("List"):
            return f"LIST<{TigerGraphLoader.type_mapping(prop[5:-1])}>"
        else:
            return f"{TigerGraphLoader.type_mapping(prop)}"
    
    def __init__(
        self,
        **kwargs
    ):
        """
        Loader object for Neo4J

        Parameters
        ----------
        **kwargs : optional
            Everything in kwargs argument will be passed to create a ``pyTigerGraph.TigerGraphConnection``
            
        Examples
        --------

        >>> loader = Neo4JLoader(host="http://127.0.0.1", username="tigergraph", password="tigergraph")
        """
        if "url" in kwargs:
            kwargs["host"] = kwargs["url"]
        if "uri" in kwargs:
            kwargs["host"] = kwargs["uri"]
        
        config_ = {
            "host": "http://127.0.0.1",
            "username": "tigergraph",
            "password": "tigergraph",
            "graphname": "MyGraph"
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
            
        self.graph = pyTigerGraph.TigerGraphConnection(
            host=config["host"], 
            username=config["username"], 
            password=config["password"], 
            graphname=config["graphname"]
        )
        
        if self.graph is None:
            logging.error("No TigerGraph instance found, check config at : ./output/config.yaml")
            raise ConnectionError("No TigerGraph instance found, check config at : ./output/config.yaml")
        
        self.graph.gsql("CREATE GRAPH Default()")
    
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
        Loading nodes and metadata in TigerGraph\n
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

        >>> loader.load_nodes(
            "./output/nodes/ChEMBL_Molecule.csv", 
            "Molecule",
            "ChEMBL",
            {"licence": "CC-BY-4.0"},
            {"id": "Utf8", "name": "Utf8", ...},
            ["id"],
            ["name"]
        )
        """
        
        file_path = f"/data/nodes/{file_path}"
        
        prop_mapped = ",\n\t\t".join(
            f"{'PRIMARY_ID ' if k in constraints else ''}{k} { TigerGraphLoader.csv_mapping(property) }" for k, property in properties_type.items()
        )
        
        file_name = file_path.split("/")[-1].split(".")[0]
        
        QUERY = f"""
        USE GRAPH Default
        CREATE SCHEMA_CHANGE JOB add_node_{file_name} FOR GRAPH Default {{
            ADD VERTEX {label} (
                {prop_mapped},
                source STRING
            );
        }}
        RUN SCHEMA_CHANGE JOB add_node_{file_name}"""
        
        res = self.graph.gsql(QUERY)
        
        QUERY = f"""
        USE GRAPH Default
        CREATE LOADING JOB load_node_{file_name} FOR GRAPH Default {{
            DEFINE FILENAME file1="{file_path}";
            LOAD file1 TO VERTEX {label} VALUES ({', '.join(f'$"{k}"' for k in properties_type.keys())}, "{source}") USING header="true", separator=";";
        }}
        RUN LOADING JOB load_node_{file_name}"""
        
        res = self.graph.gsql(QUERY)
        n_loaded = int(next(line for line in res.splitlines() if ".csv |" in line).split("|")[3])
        print(n_loaded)
        
        self.graph.gsql(f"""USE GRAPH Default
        DROP LOADING JOB load_node_{file_name}""")
        
        return n_loaded
    
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
        
        start_label, start_id = start.split(':')
        end_label, end_id = end.split(':')
        
        if start_id != "id" or end_id != "id":
            raise ValueError("With TigerGraph, joins between vertex only is supported for `id` vertex attribute")
        
        header_path = os.path.abspath(f"./output/edges/{file_path}").replace('\\', '/')
        if header_path[0] == '/':
            header_path = header_path[1:]

        trailing_slash = "/" if ":/" not in header_path else ""

        file_path = f"/data/edges/{file_path}"
        
        with open(trailing_slash+header_path) as f:
            csv_reader = csv.reader(f, delimiter=';')
            header = next(csv_reader)
            header.remove("start")
            header.remove("end")


        prop_mapped = ",\n\t\t".join(
            f"{k} { TigerGraphLoader.csv_mapping(property) }" for k, property in properties_type.items() if k not in ("start", "end")
        )
        
        file_name = file_path.split("/")[-1].split(".")[0]
        
        QUERY = f"""
        USE GRAPH Default
        CREATE SCHEMA_CHANGE JOB add_edge_{file_name} FOR GRAPH Default {{
            ADD UNDIRECTED EDGE {edge_type} (
                FROM {start_label}, 
                TO {end_label},
                {prop_mapped},
                source STRING
            );
        }}
        RUN SCHEMA_CHANGE JOB add_edge_{file_name}"""
        
        res = self.graph.gsql(QUERY)
        
        QUERY = f"""
        USE GRAPH Default
        CREATE LOADING JOB load_edge_{file_name} FOR GRAPH Default {{
            DEFINE FILENAME file1="{file_path}";
            LOAD file1 TO EDGE {edge_type} VALUES ({', '.join(f'$"{k}"' for k in properties_type.keys())}, "{source}") USING header="true", separator=";";
        }}
        RUN LOADING JOB load_edge_{file_name}"""
        
        res = self.graph.gsql(QUERY)
        n_loaded = int(next(line for line in res.splitlines() if ".csv |" in line).split("|")[3])
        print(n_loaded)
        
        self.graph.gsql(f"""USE GRAPH Default
        DROP LOADING JOB load_edge_{file_name}""")
        
        return n_loaded
