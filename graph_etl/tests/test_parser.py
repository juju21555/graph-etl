import pytest

import graph_etl as etl
import json

def test_decorator():
    
    etl.init()
    
    @etl.Parser(
        source="test",
        metadata1=15_000,
        metadata2="metadata2"
    )
    def test_parsing(ctx: etl.Context):
        
        df = [
            {"id": 1, "name": "Tom"},
            {"id": 2, "name": "Marie"},
        ]
        
        ctx.save_nodes(df, "Person", indexs=["name"])
        
    etl.parse()

    with open("./output/configs/configs.json", "r") as f:
        configs = json.load(f)
        
    assert "Int" in configs["nodes"]["Person"]["properties_type"]["id"]
    
    assert "id" in configs["nodes"]["Person"]["constraints"]
    assert len(configs["nodes"]["Person"]["constraints"]) == 1
    
    assert "name" in configs["nodes"]["Person"]["indexs"]
    assert len(configs["nodes"]["Person"]["indexs"]) == 1
    
    first_file_info = list(configs["nodes"]["Person"]["files"].values())[0]
    
    assert first_file_info["count"] == 2
    
    assert first_file_info["metadata1"] == 15_000
    assert first_file_info["metadata2"] == "metadata2"

    etl.clear()

def test_with_keyword():
    
    etl.init()
    
    with etl.Parser(
        source="test",
        metadata1=2_834,
        metadata2="metadata8"
    ) as ctx:
        
        df = [
            {"id": "5", "name": "Andrew"},
            {"id": "8", "name": "Chloe"},
            {"id": "8", "name": "Donald"}, # Same id should be removed
        ]
        
        ctx.save_nodes(df, "Person", indexs=["name"])


    with open("./output/configs/configs.json", "r") as f:
        configs = json.load(f)
        
    assert "id" in configs["nodes"]["Person"]["constraints"]
    assert len(configs["nodes"]["Person"]["constraints"]) == 1
    
    assert "name" in configs["nodes"]["Person"]["indexs"]
    assert len(configs["nodes"]["Person"]["indexs"]) == 1
    
    first_file_info = list(configs["nodes"]["Person"]["files"].values())[0]
    
    assert first_file_info["count"] == 2
    
    assert first_file_info["metadata1"] == 2_834
    assert first_file_info["metadata2"] == "metadata8"
    
    etl.clear()
        
def test_decorator_mapping():
    
    etl.init()
    
    @etl.Parser(
        source="test",
        metadata1=15_000,
        metadata2="metadata2"
    )
    def test_parsing(ctx: etl.Context):
        
        df = [
            {"start": 1, "end": "Tom"},
            {"start": 2, "end": "Marie"},
            {"start": 2, "end": "Chloe"},
        ]
        
        mapping = [
            {"old_value": 2, "new_value": "F432OP"},
            {"old_value": 2, "new_value": "DUPLICATE_F432OP"}, # a duplicate will be ignored in a mapping file
            {"old_value": 1, "new_value": "P821DS"},
        ]
        
        ctx.save_edges(df, "DRIVED_BY", start_id="Car:id", end_id="Person:id")
        ctx.map_ids(mapping, "Car:id")
        
    etl.parse()
        
    with open("./output/configs/configs.json", "r") as f:
        configs = json.load(f)
        
    f_name = f"./output/edges/{list(configs['edges']['DRIVED_BY']['files'].keys())[0]}"
    with open(f_name, 'r') as f: 
        line_tom = next(line for line in f.read().splitlines() if "Tom" in line).split(';')
        
    assert "P821DS" in line_tom
    
    assert "Utf8" in configs["edges"]["DRIVED_BY"]["properties_type"]["start"]
    
    assert list(configs["edges"]["DRIVED_BY"]["files"].values())[0]["count"] == 3
        
    etl.clear()


def test_decorator_auto_mapping():
    """
    If an edge has a side joined on an attribute other than `id`, the csv file will be edited
    automatically so that the edge with the attribute will me mapped with the corresponding `id` 
    using the node csv as a mapping.
    """
    
    etl.init()
    
    @etl.Parser(source="test")
    def test_parsing(ctx: etl.Context):
        
        drived_by = [
            {"start": 1, "end": "Tom"},
            {"start": 2, "end": "Marie"},
            {"start": 2, "end": "Chloe"},
        ]
        
        person = [
            {"id": 101, "name": "Tom"},
            {"id": 102, "name": "Marie"},
            {"id": 103, "name": "Chloe"},
        ]
        
        ctx.save_nodes(person, "Person")
        ctx.save_edges(drived_by, "DRIVED_BY", start_id="Car:id", end_id="Person:name") 
        
    etl.parse()
        
    with open("./output/configs/configs.json", "r") as f:
        configs = json.load(f)
        
    f_name = f"./output/edges/{list(configs['edges']['DRIVED_BY']['files'].keys())[0]}"
    with open(f_name, 'r') as f: 
        line_tom = next(line for line in f.read().splitlines() if "101" in line).split(';')
        
    assert "1" in line_tom
    
    assert "Int" in configs["edges"]["DRIVED_BY"]["properties_type"]["start"]
    
    assert configs["edges"]["DRIVED_BY"]["end"] == "Person:id"
    
    assert list(configs["edges"]["DRIVED_BY"]["files"].values())[0]["count"] == 3

    etl.clear()
        
        
def test_decorator_filter():
    
    @etl.Parser(source="test")
    def test_parsing_1(ctx: etl.Context):
        df = [
            {"id": 1, "name": "Tom"},
            {"id": 2, "name": "Marie"},
        ]
        
        ctx.save_nodes(df, "Person", indexs=["name"])
        
    @etl.Parser(source="test2")
    def test_parsing_2(ctx: etl.Context):
        df = [
            {"id": 8, "name": "Tom"},
            {"id": 4, "name": "Marie"},
        ]
        
        ctx.save_nodes(df, "Person", indexs=["name"])
    
    filters = etl.Filter().add_metadata("source", "test2")
    
    etl.init(filters=filters)
        
    etl.parse()

    with open("./output/configs/configs.json", "r") as f:
        configs = json.load(f)
        
    f_name = f"./output/nodes/{list(configs['nodes']['Person']['files'].keys())[0]}"
    with open(f_name, 'r') as f: 
        line_tom = next(line for line in f.read().splitlines() if "Tom" in line).split(';')
        
    assert "8" in line_tom

    assert len(configs["nodes"]["Person"]["files"]) == 1
    
    first_file_info = list(configs["nodes"]["Person"]["files"].values())[0]
    
    assert first_file_info["count"] == 2

    etl.clear()
    
test_decorator_filter()