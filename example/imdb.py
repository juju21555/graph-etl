import graph_etl as getl
import polars as pl


@getl.Parser(
    sources_path="./example/data/IMDB-Movie-Data.csv",
    source="kaggle"
)
def parse_imdb(context: getl.Context):
    
    df = (
        pl.read_csv("./example/data/IMDB-Movie-Data.csv")
            .select(["Rank", "Title", "Director", "Actors"])
            .with_columns([
                pl.col("Actors").str.split(", ")
            ])
            .explode("Actors")
            .rename({
                "Rank": "id",
                "Title": "title"
            })
    )
    
    context.save_nodes(
        df.select(["id", "title"]),
        "Movie"
    )
    
    context.save_nodes(
        pl.concat((
            df.select("Actors").rename({"Actors": "name"}),
            df.select("Director").rename({"Director": "name"})
        )),
        "Person",
        primary_key="name"
    )
    
    context.save_edges(
        df.select(["id", "Actors"]).rename({"id": "end",  "Actors": "start"}),
        "ACTED_IN",
        start_id="Person:name",
        end_id="Movie:id"
    )
    
    context.save_edges(
        df.select(["id", "Director"]).rename({"id": "end",  "Director": "start"}),
        "DIRECTED",
        start_id="Person:name",
        end_id="Movie:id"
    )

