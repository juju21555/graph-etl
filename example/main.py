from imdb import *
from oscar import *

import graph_etl as getl

filters = (
    getl.Filter()
        .add_edges(["AWARDED_FOR", "ACTED_IN"])
        .add_nodes(["Movie", "Person", "Award"])
)

getl.init(
    filters=filters,
    callbacks=[getl.CallbackOWL(), getl.CallbackSHACL()]
)

getl.parse()

getl.load(getl.Neo4JLoader())

# getl.clear()