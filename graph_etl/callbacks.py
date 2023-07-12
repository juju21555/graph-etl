import types
from typing import Dict
from abc import ABC, abstractmethod


class Callback(ABC):
    
    @abstractmethod
    def __init__(self) -> None:
        pass
    
    @abstractmethod
    def save_nodes(
        self,
        label: str,
        properties: Dict[str, str],
        metadatas: Dict,
        **kwargs
    ) :
        pass
        
    @abstractmethod
    def save_edges(
        self,
        edge_type: str,
        start_label: str,
        end_label: str,
        **kwargs
    ) :
        pass

    
class CallbackOWL(Callback):
    
    def __init__(self) -> None:
        import owlready2 as owl2
        self._owl2 = owl2
        try:
            self.ontology = owl2.get_ontology("./output/file.owl").load()
        except:
            self.ontology = owl2.get_ontology("http://aidd4h.org/")
    
    def type_mapping(
        self,
        prop
    ):
        if prop.startswith("Utf8"):
            return str
        elif prop.startswith("Float"):
            return float
        elif prop.startswith("Int"):
            return int
        elif prop.startswith("Boolean"):
            return bool
        return None
    
    def save_nodes(
        self,
        label: str,
        properties: Dict[str, str],
        metadatas: Dict,
        **kwargs
    ) :
        with self.ontology:
            nodeClass = types.new_class(label, (self._owl2.Thing, ))
            
            for k, v in metadatas.items():
                types.new_class(f"has_{k}", (self._owl2.AnnotationProperty, ))
                nodeClass.__getattribute__(f"has_{k}").append(v)
            
            prop_is_func = kwargs.get('functionnal_property', [])
            
            
            for property_name, property_range in properties.items():
                if (owl_property := self.type_mapping(property_range)):
                    if property_name in prop_is_func:
                        propertyClass = types.new_class(
                            property_name, 
                            (nodeClass >> owl_property, self._owl2.FunctionalProperty)
                        )                    
                    else:
                        propertyClass = types.new_class(
                            property_name, 
                            (nodeClass >> owl_property, )
                        )
            
            equivalent = kwargs.get('equivalent_to', None)
            subclass  = kwargs.get('subclass', None)
            
        self.ontology.save("./output/file.owl")
        
                
    def save_edges(
        self,
        edge_type: str,
        start_label: str,
        end_label: str,
        **kwargs
    ) :
        with self.ontology:
            nodeStartClass = types.new_class(start_label, (self._owl2.Thing, ))
            nodeEndClass = types.new_class(end_label, (self._owl2.Thing, ))
            
            edgeMetaclass = [nodeStartClass >> nodeEndClass]
            
            if kwargs.get('is_functional', False):
                edgeMetaclass += [self._owl2.FunctionalProperty]
            if kwargs.get('is_inverse_functional', False):
                edgeMetaclass += [self._owl2.InverseFunctionalProperty]
            if kwargs.get('is_transitive', False):
                edgeMetaclass += [self._owl2.TransitiveProperty]
            if kwargs.get('is_symmetric', False):
                edgeMetaclass += [self._owl2.SymmetricProperty]
            if kwargs.get('is_asymmetric', False):
                edgeMetaclass += [self._owl2.AsymmetricProperty]
            if kwargs.get('is_reflexive', False):
                edgeMetaclass += [self._owl2.ReflexiveProperty]
            if kwargs.get('is_irreflexive', False):
                edgeMetaclass += [self._owl2.IrreflexiveProperty]
                
            
            edgeClass = types.new_class(edge_type, tuple(edgeMetaclass))
            
            inverse_of = kwargs.get('inverse_of', None)
            if inverse_of:
                edgeClass.inverse_property = types.new_class(inverse_of, (self._owl2.ObjectProperty, ))
                
        self.ontology.save("./output/file.owl")



class CallbackSHACL(Callback):
    
    def __init__(self) -> None:
        import rdflib
        self._rdflib = rdflib
        
        self.NEO4J = self._rdflib.Namespace("neo4j://graph.schema#")
        
        self.g = self._rdflib.Graph()
        try:
            self.g.parse("./output/file.ttl")
        except:
            self.g.bind("sh", self._rdflib.namespace.SH)
            self.g.bind("neo4j", self.NEO4J)
    
    def type_mapping(
        self,
        prop
    ):
        if "Utf8" in prop:
            return self._rdflib.namespace.XSD.string
        elif "Float" in prop:
            return self._rdflib.namespace.XSD.float
        elif "Int" in prop:
            return self._rdflib.namespace.XSD.integer
        elif "Boolean" in prop:
            return self._rdflib.namespace.XSD.boolean
        return None
    
    def save_nodes(
        self,
        label: str,
        properties: Dict[str, str],
        metadatas: Dict,
        **kwargs
    ) :
        label_shape = label+"Shape"
        
        self.g.add((self._rdflib.NEO4J[label_shape], self._rdflib.namespace.RDF.type, self._rdflib.namespace.SH.NodeShape))
        self.g.add((self._rdflib.NEO4J[label_shape], self._rdflib.namespace.SH.targetClass, self._rdflib.NEO4J[label]))
        self.g.add((self._rdflib.NEO4J[label_shape], self._rdflib.namespace.SH.closed, self._rdflib.Literal(True)))
        
        prop_is_func = kwargs.get('functionnal_property', [])
        
        for property_name, property_range in properties.items():
            if (owl_property := self.type_mapping(property_range)):
                if property_name in prop_is_func:
                    b_node_prop = self._rdflib.BNode()
                    self.g.add((self._rdflib.NEO4J[label_shape], self._rdflib.namespace.SH.property, b_node_prop))
                    self.g.add((b_node_prop, self._rdflib.namespace.SH.path, self._rdflib.NEO4J[property_name]))
                    self.g.add((b_node_prop, self._rdflib.namespace.SH.maxCount, self._rdflib.Literal(1)))
                    self.g.add((b_node_prop, self._rdflib.namespace.SH.datatype, owl_property))
                     
                else:
                    b_node_prop = self._rdflib.BNode()
                    self.g.add((self._rdflib.NEO4J[label_shape], self._rdflib.namespace.SH.property, b_node_prop))
                    self.g.add((b_node_prop, self._rdflib.namespace.SH.path, self._rdflib.NEO4J[property_name]))
                    self.g.add((b_node_prop, self._rdflib.namespace.SH.datatype, owl_property))
        
        for k in metadatas.keys():
            b_node_prop = self._rdflib.BNode()
            self.g.add((self._rdflib.NEO4J[label_shape], self._rdflib.namespace.SH.property, b_node_prop))
            self.g.add((b_node_prop, self._rdflib.namespace.SH.path, self._rdflib.NEO4J[k]))
            self.g.add((b_node_prop, self._rdflib.namespace.SH.datatype, self._rdflib.namespace.XSD.string))
        
        self.g.serialize("./output/file.ttl", format="turtle")
        
                
    def save_edges(
        self,
        edge_type: str,
        start_label: str,
        end_label: str,
        **kwargs
    ) :
        
        start_label_shape = start_label + "Shape"
        
        b_node_prop = self._rdflib.BNode()
        self.g.add((self._rdflib.NEO4J[start_label_shape], self._rdflib.namespace.SH.property, b_node_prop))
        self.g.add((b_node_prop, self._rdflib.namespace.SH.path, self._rdflib.NEO4J[edge_type]))
        self.g.add((b_node_prop, self._rdflib.namespace.SH["class"], self._rdflib.NEO4J[end_label]))
        self.g.add((b_node_prop, self._rdflib.namespace.SH.nodeKind, self._rdflib.namespace.SH.IRI))
        
        self.g.serialize("./output/file.ttl", format="turtle")
        