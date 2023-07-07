from abc import ABC, abstractmethod
from typing import List, Dict


class Loader(ABC):
    
    @abstractmethod
    def __init__(
        self,
        **kwargs
    ) -> None: 
        pass
    
    @abstractmethod
    def load_nodes(
        self,
        file_path: str,
        label: str,
        source: str,
        metadatas: Dict,
        properties_type: Dict[str, str],
        constraints: List[str],
        indexs: List[str]
    ) -> int:
        pass

    @abstractmethod
    def load_edges(
        self,
        file_path: str,
        edge_type: str,
        start: str,
        end: str,
        source: str,
        metadatas: Dict,
        properties_type: Dict[str, str]
    ) -> int:
        pass

