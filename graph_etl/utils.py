from __future__ import annotations
from typing import Callable, List, Dict, Union, Tuple, TYPE_CHECKING, Any

import json
import os
import time
import logging

from .pipeline import _init, _load, _parse, _map_property
from .context import Context

if TYPE_CHECKING:
    from .callbacks import Callback
    from .filters import Filter
    from .loader import Loader
    
class StoreInfo:
    def __init__(self):
        self._stats_store = {
            "nodes_count": 0,
            "edges_count": 0,
            "nodes_count_source": 0,
            "edges_count_source": 0,
            "total_time": 0
        }

        self._all_parsing_functions : Dict[str, Tuple[Callable[..., None], str, str]] = {}
        self._all_mapping_functions : List[Tuple[Callable[..., None], str, str]] = []

        self._config_path = os.path.abspath("./output/configs/configs.json")
        self._parser_path = os.path.abspath("./output/log_parser.txt")
        self._mapper_path = os.path.abspath("./output/log_mapper.txt")
        self._loader_path = os.path.abspath("./output/log_loader.txt")

        if os.path.exists(self._parser_path):
            with open(self._parser_path) as f:
                self._already_parsed = f.readlines()
        else:
            self._already_parsed = []
            
        if os.path.exists(self._mapper_path):
            with open(self._mapper_path) as f:
                self._already_mapped = f.readlines()
        else:
            self._already_mapped = []
                
        if os.path.exists(self._loader_path):
            with open(self._loader_path) as f:
                self._already_loaded = f.readlines()
        else:
            self._already_loaded = []
            
        if os.path.exists(self._config_path):
            with open(self._config_path, "r") as f:
                self._configs = json.load(f)
        else:
            self._configs = {
                'nodes': {},
                'edges': {}
            }
        self._filters: Filter = None
        self._callbacks: List[Callback] = None
        
        self._ids_to_map = {}
    
    def add_mapping_id(self, id_to_map: str, mapping: Any):
        self._ids_to_map[id_to_map] = mapping
    
    def add_source(self, source : str):
        if source not in self._configs['nodes']:
            self._configs['nodes'].update({source: {}})
        if source not in self._configs['edges']:
            self._configs['edges'].update({source: {}})
        
    def update_nodes(self, source : str, file_name : str, infos: Dict):
        self._configs['nodes'][source].update({file_name: infos})
        self._stats_store['nodes_count_source'] += infos['count']
        
    def update_edges(self, source : str, file_name : str, infos: Dict):
        self._configs['edges'][source].update({file_name: infos})
        self._stats_store['edges_count_source'] += infos['count']
        
    def save_parser_infos(self, func_name : str, time : float, source : str):
        with open(f"./output/configs/configs.json", "w") as f:
            json.dump(self._configs, f, indent=4)
        
        logging.info(f"{func_name:<30} took {time//60}m {time%60}s to finish")
        logging.info(f"| -- Total nodes : {self._stats_store['nodes_count_source']:>12} -- |")
        logging.info(f"| -- Total edges : {self._stats_store['edges_count_source']:>12} -- |")
        
        self._stats_store['nodes_count'] += self._stats_store['nodes_count_source']
        self._stats_store['edges_count'] += self._stats_store['edges_count_source']
        
        self._stats_store['nodes_count_source'] = 0
        self._stats_store['edges_count_source'] = 0
        
        self._stats_store['total_time'] += time
        
        if func_name != "anonymous function":
            with open(self._parser_path, "a") as f:
                f.write(f"{source}_{func_name}\n")

    def save_mapper_infos(self, func_name : str, time : float, source : str):
        logging.info(f"{func_name:<30} took {time//60}m {time%60}s to finish")
        with open(self._mapper_path, "a") as f:
            f.write(f"{source}\n")
        
    def set_filters(self, filters: Filter = None):
        self._filters = filters
        
    def set_callbacks(self, callbacks: List[Callback] = None):
        self._callbacks = callbacks

INFOS_SINGLETON = StoreInfo()

def init(filters: Filter = None, callbacks: List[Callback] = None):
    global INFOS_SINGLETON
    _init(INFOS_SINGLETON, filters=filters, callbacks=callbacks)

def parse(use_mapper=True):
    """
    Call each method created with a `@etl.Parser` decorator
    to save nodes/edges then map old value to new value if 
    `use_mapper` is `True`

    Parameters
    ----------
    use_mapper : bool
        If use_mapper is False, mapping function won't be used
        
    Examples
    --------
    Parsing each file:

    >>> etl.parse()
    """
    global INFOS_SINGLETON
    _parse(INFOS_SINGLETON, use_mapper=use_mapper)
    
    
def load(loader_obj: Loader, clear_source : Union[List[str], bool] = None):
    """
    Use this function after calling `etl.parse()`

    Parameters
    ----------
    loader_obj : Loader
        An instance of a Loader subclass (either Neo4JLoader or TigerGraphLoader)
    use_mapper : bool
        If use_mapper is False, mapping function won't be used
        
    Examples
    --------
    Loading each file in a Neo4J database:

    >>> neo_loader = etl.Neo4JLoader(url="bolt://127.0.0.1:7687")
    >>> etl.load(neo_loader)
    """
    global INFOS_SINGLETON
    _load(INFOS_SINGLETON, loader_obj=loader_obj, clear_source=clear_source)

class Parser:
    """
    Use this method as a decorator to create a function that parse and write csv file along with metadata of nodes and edges.
    
    It is also possible to use it in a `with` statement that return a `Context` variable.

    Parameters
    ----------
    sources_path : List[str]
        A list of path to the different files used in the function to check 
        if all files exists before parsing
    source : str
        The name of the source from where the data come from
    **kwargs : Dict
        The metadatas of the source
        
    Examples
    --------
    Creating a parsing function:

    >>> @etl.Parser(
    >>>     sources_path=["./databases/ontologies/bto.obo"],
    >>>     source="BTO",
    >>>     licence="CC-BY-4.0"
    >>> )
    >>> def parse_bto(ctx: etl.Context):
    >>>     ...do your stuff here...
    >>>     ctx.save_nodes(...)
    >>>     ctx.save_edges(...)
    >>>     ctx.map_ids(...)
    
    Using in a `with` statement:

    >>> with etl.Parser(
    >>>     sources_path=["./databases/ontologies/bto.obo"],
    >>>     source="BTO",
    >>>     licence="CC-BY-4.0"
    >>> ) as ctx:
    >>>     ...do your stuff here...
    >>>     ctx.save_nodes(...)
    >>>     ctx.save_edges(...)
    >>>     ctx.map_ids(...)
    """
    
    def __init__(
        self, 
        source: str, 
        sources_path: List[str] | str = None, 
        ignore: bool = False,
        **kwargs
    ):
        INFOS_SINGLETON.add_source(source)
        self.source = source
        if not sources_path:
            self.sources_path = []
        elif isinstance(sources_path, str):
            self.sources_path = [sources_path]
        else:
            self.sources_path = sources_path
        
        self.metadatas = kwargs
        self.ignore = ignore
        
        self.context = Context(INFOS_SINGLETON, self.metadatas, self.source)
        
        
    def _should_skip(self, func_name):
        if f"{self.source}_{func_name}\n" in INFOS_SINGLETON._already_parsed or self.ignore:
            logging.warning(f"{func_name} | already parsed, skipping... ")
            return True
        
        files_not_found = []
        for path in self.sources_path:
            if not os.path.exists(os.path.abspath(path)):
                files_not_found.append(path)
        
        if files_not_found:
            logging.warning(f"{func_name} | files missing : {files_not_found}, skipping... ")
            return True
        
        return False
    
    def __enter__(self):
        if self._should_skip("anonymous function"): 
            return Context(None, None, None)
        
        self.start = time.time()
        return self.context
    
    def __exit__(self, *args):
        if self._should_skip("anonymous function"): 
            return None
        
        INFOS_SINGLETON.save_parser_infos("anonymous function", time.time() - self.start, self.source)
        _map_property(INFOS_SINGLETON)
        
    def __call__(self, f):
        if f.__code__.co_argcount != 1:
            raise Exception("""A function decorated with ``Parser`` must have exactly one argument.
It serves as a placholder for a context variable injection.
The variable `context` must be used in the function to save nodes or edges using : 
`context.save_nodes(...)`
`context.save_edges(...)`
`context.map_ids(...)`""")
        
        def wrapper():
            if self._should_skip(f.__name__): return
            
            start = time.time()
            f(self.context)
            INFOS_SINGLETON.save_parser_infos(f.__name__, time.time() - start, self.source)
            
        INFOS_SINGLETON._all_parsing_functions[f.__name__] = (wrapper, self.source, self.metadatas)
        return wrapper