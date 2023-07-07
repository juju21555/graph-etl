import pytest

import graph_etl as etl
import polars as pl
import json

def test_decorator():
    
    etl.init()
    
    @etl.Parser(
        source="test",
        metadata1=15_000,
        metadata2="metadata2"
    )
    def test_parsing(ctx: etl.Context):
        
        df = pl.from_dicts([
            {"id": 1, "name": "Tom"},
            {"id": 2, "name": "Marie"},
        ])
        
        ctx.save_nodes(df, "Person", indexs=["name"])
        
    etl.parse()

    with open("./output/configs/configs.json", "r") as f:
        configs = json.load(f)
        
    etl.clear()
        
    assert "Int" in configs["nodes"]["test"]["test_Person_0.csv"]["properties_type"]["id"]
    
    assert configs["nodes"]["test"]["test_Person_0.csv"]["label"] == "Person"
    
    assert "id" in configs["nodes"]["test"]["test_Person_0.csv"]["constraints"]
    assert len(configs["nodes"]["test"]["test_Person_0.csv"]["constraints"]) == 1
    
    assert "name" in configs["nodes"]["test"]["test_Person_0.csv"]["indexs"]
    assert len(configs["nodes"]["test"]["test_Person_0.csv"]["indexs"]) == 1
    
    assert configs["nodes"]["test"]["test_Person_0.csv"]["count"] == 2
    
    assert configs["nodes"]["test"]["test_Person_0.csv"]["metadatas"]["metadata1"] == 15_000
    assert configs["nodes"]["test"]["test_Person_0.csv"]["metadatas"]["metadata2"] == "metadata2"


def test_with_keyword():
    
    etl.init()
    
    with etl.Parser(
        source="test",
        metadata1=2_834,
        metadata2="metadata8"
    ) as ctx:
        
        df = pl.from_dicts([
            {"id": "5", "name": "Andrew"},
            {"id": "8", "name": "Chloe"},
            {"id": "8", "name": "Donald"}, # Same id should be removed
        ])
        
        ctx.save_nodes(df, "Person", indexs=["name"])


    with open("./output/configs/configs.json", "r") as f:
        configs = json.load(f)
        
    etl.clear()
        
    assert configs["nodes"]["test"]["test_Person_0.csv"]["label"] == "Person"
    
    assert "id" in configs["nodes"]["test"]["test_Person_0.csv"]["constraints"]
    assert len(configs["nodes"]["test"]["test_Person_0.csv"]["constraints"]) == 1
    
    assert "name" in configs["nodes"]["test"]["test_Person_0.csv"]["indexs"]
    assert len(configs["nodes"]["test"]["test_Person_0.csv"]["indexs"]) == 1
    
    assert configs["nodes"]["test"]["test_Person_0.csv"]["count"] == 2 
    
    assert configs["nodes"]["test"]["test_Person_0.csv"]["metadatas"]["metadata1"] == 2_834
    assert configs["nodes"]["test"]["test_Person_0.csv"]["metadatas"]["metadata2"] == "metadata8"
    
    
def test_decorator_mapping():
    
    etl.init()
    
    @etl.Parser(
        source="test",
        metadata1=15_000,
        metadata2="metadata2"
    )
    def test_parsing(ctx: etl.Context):
        
        df = pl.from_dicts([
            {"start": 1, "end": "Tom"},
            {"start": 2, "end": "Marie"},
            {"start": 2, "end": "Chloe"},
        ])
        
        mapping = pl.from_dicts([
            {"old_value": 2, "new_value": "F432OP"},
            {"old_value": 2, "new_value": "DUPLICATE_F432OP"}, # a duplicate will be ignored in a mapping file
            {"old_value": 1, "new_value": "P821DS"},
        ])
        
        ctx.save_edges(df, "DRIVED_BY", start_id="Car:id", end_id="Person:id")
        ctx.map_ids(mapping, "Car:id")
        
    etl.parse()

    with open("./output/edges/test_CarDRIVED_BYPerson_0.csv", "r") as f:
        line_tom = next(line for line in f.read().splitlines() if "Tom" in line).split(';')
    
        
    with open("./output/configs/configs.json", "r") as f:
        configs = json.load(f)
        
    etl.clear()
        
    assert "P821DS" in line_tom
    
    assert "Utf8" in configs["edges"]["test"]["test_CarDRIVED_BYPerson_0.csv"]["properties_type"]["start"]
    
    assert configs["edges"]["test"]["test_CarDRIVED_BYPerson_0.csv"]["count"] == 3


def test_decorator_auto_mapping():
    """
    If an edge has a side joined on an attribute other than `id`, the csv file will be edited
    automatically so that the edge with the attribute will me mapped with the corresponding `id` 
    using the node csv as a mapping.
    """
    
    etl.init()
    
    @etl.Parser(source="test")
    def test_parsing(ctx: etl.Context):
        
        drived_by = pl.from_dicts([
            {"start": 1, "end": "Tom"},
            {"start": 2, "end": "Marie"},
            {"start": 2, "end": "Chloe"},
        ])
        
        person = pl.from_dicts([
            {"id": 101, "name": "Tom"},
            {"id": 102, "name": "Marie"},
            {"id": 103, "name": "Chloe"},
        ])
        
        ctx.save_nodes(person, "Person")
        ctx.save_edges(drived_by, "DRIVED_BY", start_id="Car:id", end_id="Person:name") 
        
    etl.parse()

    with open("./output/edges/test_CarDRIVED_BYPerson_0.csv", "r") as f:
        line_tom = next(line for line in f.read().splitlines() if "101" in line).split(';')
    
        
    with open("./output/configs/configs.json", "r") as f:
        configs = json.load(f)
        
    etl.clear()
        
    assert "1" in line_tom
    
    assert "Int" in configs["edges"]["test"]["test_CarDRIVED_BYPerson_0.csv"]["properties_type"]["start"]
    
    assert configs["edges"]["test"]["test_CarDRIVED_BYPerson_0.csv"]["end"] == "Person:id"
    
    assert configs["edges"]["test"]["test_CarDRIVED_BYPerson_0.csv"]["count"] == 3

