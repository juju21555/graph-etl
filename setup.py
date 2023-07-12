import setuptools

setuptools.setup(
    name="graph-etl",
    version="0.0.1",
    author="Justin Bizouard",
    description="Easy to use Graph ETL",
    packages=["graph_etl"],
    install_requires=[
        "polars",
        "PyYAML",
        "pyTigerGraph",
        "neo4j",
        "dotwiz"
    ]
)
