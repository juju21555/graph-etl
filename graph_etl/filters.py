
from typing import List, Dict

class Filter:
    def __init__(self):
        self.sources = []
        self.nodes = []
        self.edges = []
        
    def add_source(self, source: str):
        if not isinstance(source, str): 
            raise TypeError
        self.sources.append(source)
        return self
    
    def add_sources(self, sources: List[str]):
        if not isinstance(sources, list): 
            raise TypeError
        self.sources += sources
        return self

    def add_metadata(self, key: str, val: str):
        if not isinstance(key, dict): 
            raise TypeError
        setattr(self, key, [val])
        return self
    
    def add_metadatas(self, key: str, vals: List[str]):
        if not isinstance(key, list): 
            raise TypeError
        setattr(self, key, vals)
        return self
        
    def add_node(self, node: str):
        if not isinstance(node, str): 
            raise TypeError
        self.nodes.append(node)
        return self
    
    def add_nodes(self, nodes: List[str]):
        if not isinstance(nodes, list): 
            raise TypeError
        self.nodes += nodes
        return self

    def add_edge(self, edge: str):
        if not isinstance(edge, str): 
            raise TypeError
        self.edges.append(edge)
        return self
    
    def add_edges(self, edges: List[str]):
        if not isinstance(edges, list): 
            raise TypeError
        self.edges += edges
        return self

    def skip_parse(self, source: str, metadatas: Dict):
        return (source not in self.sources) and all(k not in self.metadatas or v not in self.metadatas[k] for (k, v) in metadatas.items())

    def skip_load_node(self, source: str, metadatas: Dict, node: str):
        return (source not in self.sources) and (node not in self.nodes) and all(k not in self.metadatas or v not in self.metadatas[k] for (k, v) in metadatas.items())
    
    def skip_load_edge(self, source: str, metadatas: Dict, edge: str):
        return (source not in self.sources) and (edge not in self.edges) and all(k not in self.metadatas or v not in self.metadatas[k] for (k, v) in metadatas.items())
