from __future__ import annotations
from typing import Union, List, TYPE_CHECKING, Type

import logging
import os
import time

import json
import polars as pl

try:
    from tqdm import tqdm
except:
    tqdm = lambda x: x

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
    
    print(list(store._all_parsing_functions.values()))
    
    tqdm_parsing_func = tqdm([
        (wrapper, metadatas) for (wrapper, metadatas) in store._all_parsing_functions.values()
        if not (store._filters and store._filters.skip_parse(metadatas))
    ])
    
    for func, _ in tqdm_parsing_func:
        tqdm_parsing_func.set_description(f"Parsing ...")
        func()
        
    if use_mapper:
        _map_property(store)
    
def _map_property(store: StoreInfo):
    for edge_properties in store._configs.edges.values():
        for file in edge_properties.files:
            
            if not edge_properties.ignore_mapping and set((edge_properties.start, edge_properties.end)).intersection(set(store._ids_to_map)):
                df = pl.read_csv(f"./output/edges/{file}", separator=";", infer_schema_length=100_000)
                for prop in ("start", "end"):
                    if edge_properties[prop] in store._ids_to_map.keys():
                        mapping = store._ids_to_map[edge_properties[prop]]
                        df = df.join(
                            mapping,
                            left_on=prop,
                            right_on="old_value",
                            how="left"
                        ).with_columns(
                            [pl.col("new_value").fill_null(pl.col(prop))]
                        ).rename({
                            prop: "mapped_from", 
                            "new_value": prop
                        })
                        
                    edge_properties.ignore_mapping = True
                    edge_properties.properties_type[prop] = str(df.get_column(prop).dtype)
                
                df = df.unique(subset=['start', 'end'])
                df.write_csv(f"./output/edges/{file}", separator=";")
        

            start_label, start_id = edge_properties["start"].split(":")
            end_label, end_id = edge_properties["end"].split(":")
            
            if not (
                start_label in store._configs.nodes and
                end_label in store._configs.nodes and
                start_id in store._configs.nodes[start_label].primary_key and
                end_id in store._configs.nodes[end_label].primary_key
            ):
                df = pl.read_csv(f"./output/edges/{file}", separator=";", infer_schema_length=100_000)
                    
                for prop in ('start', 'end'):
                    p = edge_properties[prop]
                    p_label, p_id = p.split(":")
                    if "id" != p_id:
                        
                        mapping_nodes_files = [
                            file_ for file_ in store._configs.nodes[p_label].files.keys()
                        ]
                        
                        mapping = pl.concat(
                            (pl.read_csv(f"./output/nodes/{file_}", separator=";", infer_schema_length=100_000) for file_ in mapping_nodes_files)
                        ).select(["id", p_id])
                        
                        df = (
                            df.join(
                                mapping,
                                left_on=prop,
                                right_on=p_id,
                                how="left"
                            )
                            .with_columns([pl.col("id").fill_null(pl.col(prop))])
                            .drop(prop)
                            .rename({"id": prop})
                        )
                        
                        edge_properties.ignore_mapping = True
                        edge_properties.properties_type[prop] = str(df.get_column(prop).dtype)
                        edge_properties[prop] = f"{p_label}:id"
                        
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
     
    for node, infos in store._configs.nodes.items():
        
        if store._filters and store._filters.skip_load_node(infos.metadatas, node): continue
        
        for file_path, metadatas in infos.files.items():
            print(file_path)
            
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
            
            
    for edge, infos in store._configs.edges.items():
        
        if store._filters and store._filters.skip_load_edge(infos.metadatas, edge): continue
        
        for file_path, metadatas in infos.files.items():
            
            if f"{file_path}\n" in store._already_loaded: continue
            
            logging.info(f"{file_path:<30} loading...")
            
            relationshipsCreated = loader_obj.load_edges(
                file_path=file_path,
                edge_type=edge,
                start=infos.start,
                end=infos.end,
                metadatas=metadatas.to_dict(),
                properties_type=infos.properties_type
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
