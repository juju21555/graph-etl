import graph_etl as getl
import polars as pl

@getl.Parser(
    sources_path="./example/data/the_oscar_award.csv",
    source="kaggle"
)
def parse_imdb(context: getl.Context):
    df = pl.read_csv("./example/data/the_oscar_award.csv")
    
    context.save_nodes(
        df.select("category"),
        "Award",
        primary_key="category"
    )
    
    context.save_edges(
        df.select(["name", "category"]).rename({"name": "start", "category": "end"}),
        "AWARDED_FOR",
        start_id="Person:name",
        end_id="Award:category"
    )
    