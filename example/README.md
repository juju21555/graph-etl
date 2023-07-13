# Example of a project using the `graph-etl` library

Parsing are defined in `imdb.py` and `oscar.py`.
The script to start the ETL is the `main.py` file with filter and callbacks example.

From the oscar csv, we extract relationship between actor and their oscar.
From the imdb csv, we extract actors, directors, movies and the link between them (acted_in and directed).

## Sources

- data/IMDB-Movie-Data.csv: <https://www.kaggle.com/datasets/PromptCloudHQ/imdb-data>

- data/the_oscar_award.csv: <https://www.kaggle.com/datasets/unanimad/the-oscar-award>
