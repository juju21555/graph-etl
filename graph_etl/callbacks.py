import types
from typing import Dict
from abc import ABC, abstractmethod


class Callback(ABC):
    
    @abstractmethod
    def save_nodes(
        label: str,
        properties: Dict[str, str],
        source: str,
        metadatas: Dict,
        **kwargs
    ) :
        pass
        
    @abstractmethod
    def save_edges(
        edge_type: str,
        start_label: str,
        end_label: str,
        **kwargs
    ) :
        pass

    
class CallbackOWL(Callback):
    import owlready2 as owl2
    try:
        ontology = owl2.get_ontology("./output/file.owl").load()
    except:
        ontology = owl2.get_ontology("http://aidd4h.org/")
        with ontology:
            class has_source(owl2.AnnotationProperty):
                pass
    
    def type_mapping(prop):
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
        label: str,
        properties: Dict[str, str],
        source: str,
        metadatas: Dict,
        **kwargs
    ) :
        with CallbackOWL.ontology:
            nodeClass = types.new_class(label, (CallbackOWL.owl2.Thing, ))
            
            nodeClass.has_source.append(source)
            for k, v in metadatas.items():
                types.new_class(f"has_{k}", (CallbackOWL.owl2.AnnotationProperty, ))
                nodeClass.__getattribute__(f"has_{k}").append(v)
            
            prop_is_func = kwargs.get('functionnal_property', [])
            
            
            for property_name, property_range in properties.items():
                if (owl_property := CallbackOWL.type_mapping(property_range)):
                    if property_name in prop_is_func:
                        propertyClass = types.new_class(
                            property_name, 
                            (nodeClass >> owl_property, CallbackOWL.owl2.FunctionalProperty)
                        )                    
                    else:
                        propertyClass = types.new_class(
                            property_name, 
                            (nodeClass >> owl_property, )
                        )
            
            equivalent = kwargs.get('equivalent_to', None)
            subclass  = kwargs.get('subclass', None)
            
        CallbackOWL.ontology.save("./output/file.owl")
        
                
    def save_edges(
        edge_type: str,
        start_label: str,
        end_label: str,
        **kwargs
    ) :
        with CallbackOWL.ontology:
            nodeStartClass = types.new_class(start_label, (CallbackOWL.owl2.Thing, ))
            nodeEndClass = types.new_class(end_label, (CallbackOWL.owl2.Thing, ))
            
            edgeMetaclass = [nodeStartClass >> nodeEndClass]
            
            if kwargs.get('is_functional', False):
                edgeMetaclass += [CallbackOWL.owl2.FunctionalProperty]
            if kwargs.get('is_inverse_functional', False):
                edgeMetaclass += [CallbackOWL.owl2.InverseFunctionalProperty]
            if kwargs.get('is_transitive', False):
                edgeMetaclass += [CallbackOWL.owl2.TransitiveProperty]
            if kwargs.get('is_symmetric', False):
                edgeMetaclass += [CallbackOWL.owl2.SymmetricProperty]
            if kwargs.get('is_asymmetric', False):
                edgeMetaclass += [CallbackOWL.owl2.AsymmetricProperty]
            if kwargs.get('is_reflexive', False):
                edgeMetaclass += [CallbackOWL.owl2.ReflexiveProperty]
            if kwargs.get('is_irreflexive', False):
                edgeMetaclass += [CallbackOWL.owl2.IrreflexiveProperty]
                
            
            edgeClass = types.new_class(edge_type, tuple(edgeMetaclass))
            
            inverse_of = kwargs.get('inverse_of', None)
            if inverse_of:
                edgeClass.inverse_property = types.new_class(inverse_of, (CallbackOWL.owl2.ObjectProperty, ))
                
        CallbackOWL.ontology.save("./output/file.owl")



class CallbackSHACL(Callback):
    
    from rdflib import Graph, BNode, Literal
    from rdflib.namespace import SH, XSD, RDF
    from rdflib import Namespace
    NEO4J = Namespace("neo4j://graph.schema#")
    
    g = Graph()
    try:
        g.parse("./output/file.ttl")
    except:
        g.bind("sh", SH)
        g.bind("neo4j", NEO4J)
    
    def type_mapping(prop):
        if "Utf8" in prop:
            return CallbackSHACL.XSD.string
        elif "Float" in prop:
            return CallbackSHACL.XSD.float
        elif "Int" in prop:
            return CallbackSHACL.XSD.integer
        elif "Boolean" in prop:
            return CallbackSHACL.XSD.boolean
        return None
    
    def save_nodes(
        label: str,
        properties: Dict[str, str],
        source: str,
        metadatas: Dict,
        **kwargs
    ) :
        label_shape = label+"Shape"
        
        CallbackSHACL.g.add((CallbackSHACL.NEO4J[label_shape], CallbackSHACL.RDF.type, CallbackSHACL.SH.NodeShape))
        CallbackSHACL.g.add((CallbackSHACL.NEO4J[label_shape], CallbackSHACL.SH.targetClass, CallbackSHACL.NEO4J[label]))
        CallbackSHACL.g.add((CallbackSHACL.NEO4J[label_shape], CallbackSHACL.SH.closed, CallbackSHACL.Literal(True)))
        
        prop_is_func = kwargs.get('functionnal_property', [])
        
        for property_name, property_range in properties.items():
            if (owl_property := CallbackSHACL.type_mapping(property_range)):
                if property_name in prop_is_func:
                    b_node_prop = CallbackSHACL.BNode()
                    CallbackSHACL.g.add((CallbackSHACL.NEO4J[label_shape], CallbackSHACL.SH.property, b_node_prop))
                    CallbackSHACL.g.add((b_node_prop, CallbackSHACL.SH.path, CallbackSHACL.NEO4J[property_name]))
                    CallbackSHACL.g.add((b_node_prop, CallbackSHACL.SH.maxCount, CallbackSHACL.Literal(1)))
                    CallbackSHACL.g.add((b_node_prop, CallbackSHACL.SH.datatype, owl_property))
                     
                else:
                    b_node_prop = CallbackSHACL.BNode()
                    CallbackSHACL.g.add((CallbackSHACL.NEO4J[label_shape], CallbackSHACL.SH.property, b_node_prop))
                    CallbackSHACL.g.add((b_node_prop, CallbackSHACL.SH.path, CallbackSHACL.NEO4J[property_name]))
                    CallbackSHACL.g.add((b_node_prop, CallbackSHACL.SH.datatype, owl_property))
        
        for k in metadatas.keys():
            b_node_prop = CallbackSHACL.BNode()
            CallbackSHACL.g.add((CallbackSHACL.NEO4J[label_shape], CallbackSHACL.SH.property, b_node_prop))
            CallbackSHACL.g.add((b_node_prop, CallbackSHACL.SH.path, CallbackSHACL.NEO4J[k]))
            CallbackSHACL.g.add((b_node_prop, CallbackSHACL.SH.datatype, CallbackSHACL.XSD.string))
        
        b_node_prop = CallbackSHACL.BNode()
        CallbackSHACL.g.add((CallbackSHACL.NEO4J[label_shape], CallbackSHACL.SH.property, b_node_prop))
        CallbackSHACL.g.add((b_node_prop, CallbackSHACL.SH.path, CallbackSHACL.NEO4J["sources"]))
        CallbackSHACL.g.add((b_node_prop, CallbackSHACL.SH.datatype, CallbackSHACL.XSD.string))
        
        CallbackSHACL.g.serialize("./output/file.ttl", format="turtle")
        
                
    def save_edges(
        edge_type: str,
        start_label: str,
        end_label: str,
        **kwargs
    ) :
        
        start_label_shape = start_label + "Shape"
        
        b_node_prop = CallbackSHACL.BNode()
        CallbackSHACL.g.add((CallbackSHACL.NEO4J[start_label_shape], CallbackSHACL.SH.property, b_node_prop))
        CallbackSHACL.g.add((b_node_prop, CallbackSHACL.SH.path, CallbackSHACL.NEO4J[edge_type]))
        CallbackSHACL.g.add((b_node_prop, CallbackSHACL.SH["class"], CallbackSHACL.NEO4J[end_label]))
        CallbackSHACL.g.add((b_node_prop, CallbackSHACL.SH.nodeKind, CallbackSHACL.SH.IRI))
        
        CallbackSHACL.g.serialize("./output/file.ttl", format="turtle")
        