from __future__ import annotations
from typing import Union, List, TYPE_CHECKING, Type

import logging
import os
import time

import json
import polars as pl
from tqdm.auto import tqdm


if TYPE_CHECKING:
    from .loader import Loader
    from .utils import StoreInfo
    from .callbacks import Callback
    from .filters import Filter
    

def _init(store: StoreInfo, filters: Filter = None, callbacks: List[Callback] = None):
    store.set_filters(filters)
    store.set_callbacks(callbacks)
    
    os.makedirs("./output", exist_ok=True)
    os.makedirs("./output/configs", exist_ok=True)
    os.makedirs("./output/nodes", exist_ok=True)
    os.makedirs("./output/edges", exist_ok=True)

    logging.basicConfig(filename='./output/log.log', encoding='utf-8', level=logging.DEBUG)
    

def _parse(store: StoreInfo, use_mapper=True):
    if not os.path.isdir("./output"):
        print("ETL is not initialized, initializing...")
        _init(store)
    
    tqdm_parsing_func = tqdm([
        (wrapper, metadatas) for (wrapper, metadatas) in store._all_parsing_functions.values()
        if not (store._filters and store._filters.skip_parse(metadatas))
    ], desc='Parsing ...')
    
    for func, metadatas in tqdm_parsing_func:
        func()
        
    if use_mapper:
        _map_property(store)
    
def _map_property(store: StoreInfo):
    for edge_properties in store._configs.edges.values():
        for file, file_properties in edge_properties.items():
            
            if not file_properties.ignore_mapping and set((file_properties.start, file_properties.end)).intersection(set(store._ids_to_map)):
                df = pl.read_csv(f"./output/edges/{file}", separator=";", infer_schema_length=100_000)
                for prop in ("start", "end"):
                    if file_properties[prop] in store._ids_to_map.keys():
                        mapping = store._ids_to_map[file_properties[prop]]
                        df = df.join(
                            mapping,
                            left_on=prop,
                            right_on="old_value",
                            how="outer"
                        ).with_columns(
                            [pl.col("new_value").fill_null(pl.col(prop))]
                        ).rename({
                            prop: "mapped_from", 
                            "new_value": prop
                        })
                        
                    file_properties.properties_type[prop] = str(df.get_column(prop).dtype)
                
                df = df.unique(subset=['start', 'end'])
                df.write_csv(f"./output/edges/{file}", separator=";")
        

            start_label, start_id = file_properties.start.split(":")
            end_label, end_id = file_properties.end.split(":")
            
            if not (
                file_properties.ignore_mapping and
                start_label in store._configs.nodes and
                end_label in store._configs.nodes and
                start_id in store._configs.nodes[start_label].primary_key and
                end_id in store._configs.nodes[end_label].primary_key
            ):
                df = pl.read_csv(f"./output/edges/{file}", separator=";", infer_schema_length=100_000)
                    
                for prop in ('start', 'end'):
                    p = file_properties[prop]
                    p_label, p_id = p.split(":")
                    if "id" != p_id:
                        
                        
                        mapping = pl.concat((
                            pl.read_csv(f"./output/nodes/{file_}", separator=";", infer_schema_length=100_000).select(["id", p_id]) 
                            for file_ in store._configs.nodes[p_label].files.keys()
                        )).drop_nulls()
                        
                        df = (
                            df.join(
                                mapping,
                                left_on=prop,
                                right_on=p_id,
                                how="outer"
                            )
                            .with_columns([pl.col("id").fill_null(pl.col(prop))])
                            .drop(prop)
                            .rename({"id": prop})
                        )
                        
                        file_properties.properties_type[prop] = str(df.get_column(prop).dtype)
                        file_properties[prop] = f"{p_label}:id"
                        
                df = df.unique(subset=['start', 'end'])
                df.write_csv(f"./output/edges/{file}", separator=";")
                
            
    with open(f"./output/configs/configs.json", "w") as f:
        json.dump(store._configs, f, indent=4)
        
    logging.info(f"ETL took {store._stats_store['total_time']//60}m {store._stats_store['total_time']%60}s to finish")
    logging.info(f"| -- Total nodes : {store._stats_store['nodes_count']:>12} -- |")
    logging.info(f"| -- Total edges : {store._stats_store['edges_count']:>12} -- |")
    
    
def _load(store: StoreInfo, loader_obj: Loader, clear_source : Union[List[str], bool] = None):
    if not os.path.isdir("./output"):
        print("ETL is not parsed, parsing...")
        _parse(store)
    
    start = time.time()
    
    nodes_items = tqdm(store._configs.nodes.items(), desc='Loading nodes ...')
     
    for node, infos in nodes_items:
        
        for file_path, metadatas in infos.files.items():
            
            if store._filters and store._filters.skip_load_node(metadatas, node): continue
            
            if f"{file_path}\n" in store._already_loaded: continue
            
            logging.info(f"{file_path:<30} loading...")
            
            nodesCreated = loader_obj.load_nodes(
                file_path=file_path,
                label=node,
                primary_key=infos.primary_key,
                metadatas=metadatas.to_dict(),
                properties_type=infos.properties_type,
                constraints=infos.constraints,
                indexs=infos.indexs
            )
            
            logging.info(f"{file_path:<30} loaded    ")
            logging.info(f"| -- Total nodes in file : {metadatas.count:>12} -- |")
            logging.info(f"| -- Total nodes created : {nodesCreated:>12} -- |")

            with open(store._loader_path, "a") as f:
                f.write(f"{file_path}\n")
            
            
    edges_items = tqdm(store._configs.edges.items(), desc='Loading edges ...')
    
    for edge, infos in edges_items:
        
        for file_path, metadatas in infos.items():
            
            if store._filters and store._filters.skip_load_edge(metadatas, edge): continue
            
            if f"{file_path}\n" in store._already_loaded: continue
            
            logging.info(f"{file_path:<30} loading...")
            
            relationshipsCreated = loader_obj.load_edges(
                file_path=file_path,
                edge_type=edge,
                start=metadatas.start,
                end=metadatas.end,
                metadatas=metadatas.to_dict(),
                properties_type=metadatas.properties_type
            )
            
            logging.info(f"{file_path:<30} loaded    ")
            logging.info(f"| -- Total edges in file : {metadatas.count:>12} -- |")
            logging.info(f"| -- Total edges created : {relationshipsCreated:>12} -- |")
            
            with open(store._loader_path, "a") as f:
                f.write(f"{file_path}\n")
                
    end = time.time()
    logging.info(f"ETL Loading in database took {(end-start)//60}m {(end-start)%60}s to finish")   
    
    logging.info("End of ETL, cleaning log file...")
    if os.path.exists(store._mapper_path):
        os.remove(store._mapper_path)
    if os.path.exists(store._parser_path):
        os.remove(store._parser_path)
    if os.path.exists(store._loader_path):
        os.remove(store._loader_path)
