from __future__ import annotations
from typing import Union, List, TYPE_CHECKING, Type

import logging
import os
import json
import time

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
    
    tqdm_parsing_func = tqdm([
        (f, s, l) for _, (f, s, l) in store._all_parsing_functions.items()
        if not (store._filters and store._filters.skip_parse(s, l))
    ])
    
    for func, source, _ in tqdm_parsing_func:
        tqdm_parsing_func.set_description(f"Parsing {source} ...")
        func()
        
    if use_mapper:
        _map_property(store)
    
def _map_property(store: StoreInfo):
    for (source, files) in store._configs['edges'].items():
        for (file, properties) in files.items():
            
            if not properties['ignore_mapping'] and (properties['start'] in store._ids_to_map.keys() or properties['end'] in store._ids_to_map.keys()):
                df = pl.read_csv(f"./output/edges/{file}", separator=";", infer_schema_length=100_000)
                for prop in ["start", "end"]:
                    if properties[prop] in store._ids_to_map.keys():
                        mapping = store._ids_to_map[properties[prop]]
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
                        
                    store._configs["edges"][source][file]["ignore_mapping"] = True
                    store._configs["edges"][source][file]["properties_type"][prop] = str(df.get_column(prop).dtype)
                
                df = df.unique(subset=['start', 'end'])
                df.write_csv(f"./output/edges/{file}", separator=";")
                        
            if not (properties['start'].endswith(":id") and properties['end'].endswith(":id")):
                df = pl.read_csv(f"./output/edges/{file}", separator=";", infer_schema_length=100_000)
                    
                for prop in ('start', 'end'):
                    p = properties[prop]
                    p_label, p_id = p.split(":")
                    if "id" != p_id:
                        
                        mapping_nodes_files = [
                            file_
                            for (_, files_) in store._configs['nodes'].items()
                            for (file_, properties_) in files_.items() 
                            if properties_["label"] == p_label
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
                        
                        store._configs["edges"][source][file]["ignore_mapping"] = True
                        store._configs["edges"][source][file][prop] = f"{p_label}:id"
                        store._configs["edges"][source][file]["properties_type"][prop] = str(df.get_column(prop).dtype)
                
                        
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
    
    all_files_nodes = []
    for source in store._configs['nodes']:
        for file_path in store._configs['nodes'][source]:
            all_files_nodes.append(file_path)
    
    tqdm_load_nodes_source = tqdm(all_files_nodes, position=0)
    for file_path in tqdm_load_nodes_source:
        tqdm_load_nodes_source.set_description(f"Loading nodes from file {file_path}")
        if f"{file_path}\n" in store._already_loaded: continue
        infos = store._configs['nodes'][source][file_path]
                        
        if store._filters and store._filters.skip_load_node(source, infos['metadatas'], infos['label']): continue
        
        logging.info(f"{file_path:<30} loading...")
        
        nodesCreated = loader_obj.load_nodes(
            file_path=file_path,
            label=infos['label'],
            source=source,
            metadatas=infos['metadatas'],
            properties_type=infos['properties_type'],
            constraints=infos['constraints'],
            indexs=infos['indexs']
        )
        
        logging.info(f"{file_path:<30} loaded    ")
        logging.info(f"| -- Total nodes in file : {infos['count']:>12} -- |")
        logging.info(f"| -- Total nodes created : {nodesCreated:>12} -- |")

        with open(store._loader_path, "a") as f:
            f.write(f"{file_path}\n")
            
        
    all_files_edges = []
    
    for source in store._configs['edges']:
        for file_path in store._configs['edges'][source]:
            all_files_edges.append(file_path)
            
    tqdm_load_edges_source = tqdm(all_files_edges, position=0)
    for file_path in tqdm_load_edges_source:
        tqdm_load_edges_source.set_description(f"Loading edges from file {file_path}")
        if f"{file_path}\n" in store._already_loaded:continue
        
        infos = store._configs['edges'][source][file_path]
        if store._filters and store._filters.skip_load_edge(source, infos['metadatas'], infos['type']): continue
        
        logging.info(f"{file_path:<30} loading...")
        
        relationshipsCreated = loader_obj.load_edges(
            file_path=file_path,
            edge_type=infos['type'],
            start=infos['start'],
            end=infos['end'],
            source=source,
            metadatas=infos['metadatas'],
            properties_type=infos['properties_type']
        )
        
        logging.info(f"{file_path:<30} loaded    ")
        logging.info(f"| -- Total edges in file : {infos['count']:>12} -- |")
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
