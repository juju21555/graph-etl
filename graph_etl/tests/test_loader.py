import pytest

import graph_etl as etl
import json

def test_load_neo4j():
    
    etl.init()
    
    try:
        neo_connection = etl.Neo4JLoader()
    except ConnectionError: 
        return
    
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

    etl.load(neo_connection)
    
    with neo_connection.graph as g:
        res_andrew = g.execute_query("MATCH (p:Person {id: \"5\"}) RETURN p.name as name")
        res_metadata = g.execute_query("MATCH (m:Metadata)<--(p:Person {id: \"5\"}) RETURN m.metadata1 as metadata1, m.metadata2 as metadata2")
        
        g.execute_query("MATCH (n) DETACH DELETE n")
    
    assert res_andrew[0][0]['name'] == "Andrew"
    
    assert res_metadata[0][0]['metadata1'] == "2834"
    assert res_metadata[0][0]['metadata2'] == "metadata8"

    etl.clear()