# Graph-etl

Graph-etl is a Python library to help you load large data from various source in Neo4J or TigerGraph and handle metadata.

## Requirements

If you use Neo4J, you will need an instance of Neo4J v5 with `apoc.core` and `apoc.extended` plugins.
And in the conf folder, an `apoc.conf`:

```conf
apoc.import.file.enabled=true
apoc.import.file.use_neo4j_config=false
```

## Installation

Clone the repo, cd into it and install using:

```bash
pip install .
```

## Usage in a single file or in a jupyter notebook

```python
import graph_etl as getl

dataset_car = [
    {"id": "E584-FRD", "type": "Diesel", "brand": "BMW"},
    {"id": "P219-NDP", "type": "Diesel", "brand": "Mercedes-Benz"}
]

dataset_person = [
    {"name": "Matthew"},
    {"name": "George"}
]

dataset_person_car = [
    {"start": "Matthew", "end": "E584-FRD", "since": "2021-05-01"},
    {"start": "George", "end": "P219-NDP", "since": "2021-05-01"}
]

getl.init()

with @getl.Parser(source="www.car.com", version="1.2") as context:
    context.save_nodes(dataset_car, "Car")
    
with @getl.Parser(source="private_client", private=True) as context:
    context.save_nodes(dataset_person, "Person", primary_key="name")
    context.save_nodes(dataset_person_car, "HAS_CAR", start="Person:name", end="Car:id")

neo_connection = getl.Neo4JLoader()
getl.load(neo_connection)
```

## Usage in multiple file

### car.py

```python
import graph_etl as getl

@getl.Parser(source="www.car.com", version="1.2"):
def parse_car(context):
    dataset_car = [
        {"id": "E584-FRD", "type": "Diesel", "brand": "BMW"},
        {"id": "P219-NDP", "type": "Diesel", "brand": "Mercedes-Benz"}
    ]

    context.save_nodes(dataset_car, "Car")
```

### person.py

```python
import graph_etl as getl

@getl.Parser(source="private_client", private=True)
def parse_person(context):

    dataset_person = [
        {"name": "Matthew"},
        {"name": "George"}
    ]

    dataset_person_car = [
        {"start": "Matthew", "end": "E584-FRD", "since": "2021-05-01"},
        {"start": "George", "end": "P219-NDP", "since": "2021-05-01"}
    ]

    context.save_nodes(dataset_person, "Person", primary_key="name")
    context.save_nodes(dataset_person_car, "HAS_CAR", start="Person:name", end="Car:id")

```

### main.py

```python
import graph_etl as getl

getl.init()
getl.parse()
getl.load(getl.Neo4JLoader())
```

## Parser object

The `Parser` object can be used either as a decorator or in a `with` statement to add metadata and define nodes, edges and eventually mapping of id properties after the parsing is done.

As a decorator, it should decorate a function with one argument called context (the decorator will inject a `graph_etl.Context` object), the function will be stored in the `graph_etl` module and will be executed when calling `graph_etl.parse()`.
You can then use the context variable to define nodes, edges and mapping

```python
@etl.Parser(metadata1="metadata1", ...)
def parse_fctn(context):
    ...
    context.save_nodes(...)
    context.save_edges(...)
    context.map_ids(...)
```

In a `with` statement, it will again return a `graph_etl.Context` object that can be used the same as inside a function.
But the code won't be stored in the `graph_etl`, and so, calling `graph_etl.parse()` will be useless.

```python
with etl.Parser(metadata1="metadata1", ...) as context:
    ...
    context.save_nodes(...)
    context.save_edges(...)
    context.map_ids(...)
```

## Context object

The `Context` object contains three functions:

```python
ctx.save_nodes(
    nodes: Any, 
    label: str,
    primary_key: str = "id",
    constraints: Sequence[str] = None,
    indexs: Sequence[str] = None, 
    **kwargs
)
```

It is used to store a dataframe `nodes` of same `label`, the ``primary_key`` define on which property in the dataframe will be used to join nodes.
A default unique constraints will be created in Neo4J on the `primary_key` and you can add a list of other constraint by passing a list of property to `constraints`
Indexes can also be added by passing a list of property to `indexs`.

```python
ctx.save_edges(
    edges: Any, 
    edge_type: str,
    start_id: str,
    end_id: str,
    ignore_mapping: bool = False,
    **kwargs
)
```

It is used to store a dataframe `edges` of same `edge_type` from a node label to another node label.
The start and end of the edge should be define with `start_id` as a string of the form `{label_start}:{property_start}`
Idem for the `end_id` of the form `{label_end}:{property_end}`

```python
ctx.map_ids(
    mapping: Any, 
    id_to_map: str
)
```

If you have start or end properties in edges that contains old value and need to be mapped to newer value using a dataset,
e.g. : `E584-FRD` -> `POI-8754-DEF`, `P219-NDP` -> `SCD-1521-CIJ`, ...
Then store it into a dataframe with two columns (`old_value` and `new_value`)
And use `ctx.map_ids(df, 'Car:id')`.
Each occurence of `Car:id` in edges dataframe will be replaced by the ``new_value`` contain in the mapping dataframe.

## Note on dataframes

Internally, `graph-etl` use polars dataframe to ensure type-safety, but you can use any object that support the python dataframe protocol or a list of Dict.

## graph-etl flow

First, you need to call `getl.init()` and can pass optional arguments, to later filter or add callbacks methods to the ETL:

Filters arguments, to skip parsing function containing specific metadata or a specific label or edge.

```python
filters = (
    Filter()
        .add_metadata("metadata1", "value1")
        .add_nodes(["City", "Person"])
        .add_edge("LIVE_IN")
)
getl.parse(filters)
```

Construct a `SHACL` schema or an `OWL2` schema of the graph by passing `Callback` object.

```python
getl.parse(callbacks=[getl.CallbackOWL(), getl.CallbackSHACL()])
```

`getl.CallbackOWL()` need the module `owlready2` and `getl.CallbackSHACL()` need the module `rdflib`

After defining parsing function using the `@getl.Parser` decorator, calling the `getl.parse()` function will call each functions to parse and save datasets in `csv` files and metadata in a `json` file.

Then calling `getl.load(connection)` with a connection object which is either `getl.Neo4JLoader()` or `getl.TigerGraphLoader()`, it will load everything in your graph database.
