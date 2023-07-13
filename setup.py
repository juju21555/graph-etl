import setuptools

setuptools.setup(
    name="graph-etl",
    version="0.0.1",
    description="""
graph-etl: Graph ETL (Extract, Transform, Load) for Neo4j or TigerGraph

The `graph-etl` package is a Python library designed to simplify the process of parsing and loading datasets into a graph database. 

Features:
- With this library, you can easily add metadata to source from which you are parsing your data.
- You can filter parsing and loading to only restricted set of nodes/edges or metadatas.
- It can automatically create a SHACL or OWL schema of the graph.
- Dataframe sent to the graph-etl are converted to a polars Dataframe to ensure type-safety.
- After parsing, dataframe are saved in temporary csv files, they are loaded in the database directly from the files to avoid sending too much data over a REST API which would give bad performance.

Note: This package requires a Neo4j or TigerGraph database running.
""",
    packages=["graph_etl"],
    install_requires=[
        "tqdm",
        "polars>=0.18.7",
        "PyYAML",
        "pyTigerGraph",
        "neo4j",
        "dotwiz"
    ]
)
