from __future__ import annotations
from typing import List, Dict, Union, TYPE_CHECKING, Sequence, Any

import polars as pl
from uuid import uuid4

if TYPE_CHECKING:
    from .utils import StoreInfo


class Context:
    def __init__(self, store: StoreInfo, metadatas: Dict):
        self.store: StoreInfo = store
        self.metadatas: Dict = metadatas
        
    def map_ids(
        self, 
        mapping: Any, 
        id_to_map: str
    ):
        """
        Mapping object used to store link between id of different nodes

        Parameters
        ----------
        mapping : polars.Dataframe, pandas.DataFrame, or List[dict]
            Two-columns dataframe named `old_value` and `new_value`
        id_to_map : str
            A string of the form `Concept`:`property` for which the `old_value` should be 
            replaced by the corresponding `new_value` in the Dataframe
            
        Examples
        --------
        Constructing a mapping object :

        >>> df
        shape: (2, 2)
        ┌─────────────┬─────────────┐
        │ old_value   ┆ new_value   │
        │ ---         ┆ ---         │
        │ str         ┆ str         │
        ╞═════════════╪═════════════╡
        │ C0548912    ┆ EFO:7551    │
        │ CN985204    ┆ DO:0074559  │
        └─────────────┴─────────────┘
        >>> ctx.map_ids(df, "Disease:id")
        """
        if not self.store: return
        
        if hasattr(mapping, "__dataframe__"):
            mapping : pl.DataFrame = pl.from_dataframe(mapping)
        elif not isinstance(mapping, pl.DataFrame):
            mapping : pl.DataFrame = pl.from_dicts(mapping, infer_schema_length=10_000)
        else:
            mapping : pl.DataFrame = mapping
        
        self.store.add_mapping(id_to_map, mapping)        
        
    def save_nodes(
        self, 
        nodes: Any, 
        label: str,
        /, *,
        primary_key: str = "id",
        constraints: Sequence[str] = None,
        indexs: List[str] = None, 
        **kwargs
    ):
        """
        Saving nodes and metadata 

        Parameters
        ----------
        nodes : polars.Dataframe, pandas.DataFrame, or Sequence[dict]
            A dataframe containing at least a column `id` with all the data of the nodes to save
        label : str
            A string with the label of the node to load
        primary_key : str
            A string with the property used as primary key, default `id` but can be overrided using this argument
        constraints: List[str]
            A sequence of property to put a unique constraint when loaded in the database
        indexs: List[str]
            A sequence of property to put an index when loaded in the database
            
        Examples
        --------
        Inside a function decorated with @etl.Parser and the injection argument named `ctx`

        >>> df
        shape: (2, 2)
        ┌──────────────┬─────────────────┐
        │ id           ┆ name            │
        │ ---          ┆ ---             │
        │ str          ┆ str             │
        ╞══════════════╪═════════════════╡
        │ CHEMBL248702 ┆ DEXFENFLURAMINE │
        │ CHEMBL190461 ┆ CANNABIDIOL     │
        └──────────────┴─────────────────┘
        >>> ctx.save_nodes(df, "Molecule")
        """
        if not self.store: return
        
        if hasattr(nodes, "__dataframe__"):
            nodes : pl.DataFrame = pl.from_dataframe(nodes)
        elif not isinstance(nodes, pl.DataFrame):
            nodes : pl.DataFrame = pl.from_dicts(nodes, infer_schema_length=None)
        else:
            nodes : pl.DataFrame = nodes
            
        cols_type = {k: str(v) for (k, v) in nodes.schema.items()}
        
        if self.store._callbacks:
            for callback in self.store._callbacks:
                callback.save_nodes(label, cols_type, self.metadatas, **kwargs)
                
        if not primary_key:
            primary_key = 'id'
            
        nodes = (
            nodes.with_columns(pl.col(pl.List(pl.Utf8)).arr.join('|'))
                .with_columns(pl.col(pl.Utf8).str.replace_all('(\r|\n|\\\\)', ''))
                .unique(subset=[primary_key])
                .drop_nulls(primary_key)
                .with_row_count()
                .with_columns(pl.col("row_nr")//200_000)
        )
        
        
        if not constraints:
            constraints = [primary_key]
        else:
            constraints += [primary_key]
            
        if not indexs: indexs = []

        default_infos = {
            'primary_key': primary_key,
            'constraints': constraints,
            'indexs': indexs,
            'properties_type': cols_type,
            'files': {}
        }
        
        uuid = "FILE_"+str(uuid4())

        for i_chunk in nodes.get_column("row_nr").unique().to_list():
            file_name = f"{uuid}_{label}_{i_chunk}.csv"
            
            chunk = nodes.filter(pl.col("row_nr")==i_chunk).drop("row_nr")
            chunk.write_csv(f"./output/nodes/{file_name}", separator=';')
            
            self.store.update_nodes(label, file_name, default_infos, self.metadatas, chunk.shape[0])
        
    def save_edges(
        self, 
        edges: Any, 
        edge_type: str,
        /, *,
        start_id: str,
        end_id: str,
        ignore_mapping: bool = False,
        **kwargs
    ):
        """
        Saving edges and metadata 

        Parameters
        ----------
        edges : polars.Dataframe, pandas.DataFrame, or List[dict]
            A dataframe containing at least a column `start` and a column `end` 
            With all the data of the edges to save
        edge_type : str
            A string with the label of the node to load
        start_id : str
            A string of the form `Concept`:`property` to start the relationship
        end_id : str
            A string of the form `Concept`:`property` to end the relationship
        ignore_mapping : bool
            If the file should be ignored at the mapping step    
            
        Examples
        --------
        Inside a function decorated with @etl.Parser and the injection argument named `ctx`

        >>> df
        shape: (2, 2)
        ┌──────────────┬────────────┐
        │ start        ┆ end        │
        │ ---          ┆ ---        │
        │ str          ┆ str        │
        ╞══════════════╪════════════╡
        │ CHEMBL500    ┆ C0155709   │
        │ CHEMBL190461 ┆ C0038358   │
        └──────────────┴────────────┘
        >>> SIDER_SideEffect = ctx.save_edges(df, "SIDE_EFFECT", start_id="ChEMBL:id", end_id="Disease:id")
        """
        
        if not self.store: return
        
        if hasattr(edges, "__dataframe__"):
            edges : pl.DataFrame = pl.from_dataframe(edges)
        elif not isinstance(edges, pl.DataFrame):
            edges : pl.DataFrame = pl.from_dicts(edges, infer_schema_length=10_000)
        else:
            edges : pl.DataFrame = edges
            
        start_label = start_id.split(":")[0]
        end_label = end_id.split(":")[0]
        
        if self.store._callbacks:
            for callback in self.store._callbacks:
                callback.save_edges(
                    edge_type, 
                    start_id.split(":")[0],
                    end_id.split(":")[0],
                    **kwargs
                )
                
        cols_type = {k: str(v) for (k, v) in edges.schema.items()}
        
        edges = (
            edges.with_columns(pl.col(pl.List(pl.Utf8)).arr.join('|'))
                .with_columns(pl.col(pl.Utf8).str.replace_all('(\r|\n|\\\\)', ''))
                .unique(subset=['start', 'end'])
                .drop_nulls(['start', 'end'])
                .with_row_count()
                .with_columns(pl.col("row_nr")//500_000)
        )

        default_infos = {
            'start': start_id,
            'end': end_id,
            'properties_type': cols_type,
            'ignore_mapping': ignore_mapping,
            'files': {}
        }

        uuid = "FILE_"+str(uuid4())
        
        for i_chunk in edges.get_column("row_nr").unique().to_list():
            file_name = f"{uuid}_{start_label}{edge_type}{end_label}_{i_chunk}.csv"
            
            chunk = edges.filter(pl.col("row_nr")==i_chunk).drop("row_nr")
            chunk.write_csv(f"./output/edges/{file_name}", separator=';')
            
            self.store.update_edges(edge_type, file_name, default_infos, self.metadatas, chunk.shape[0])
