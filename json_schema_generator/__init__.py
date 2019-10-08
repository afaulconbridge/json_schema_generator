import argparse

import simplejson as json
import dask
import dask.bag
from dask.distributed import Client, LocalCluster

# import everything so it is re-exported
from .schema import Schema, SchemaNode, SchemaNodeArray, SchemaNodeDict, \
    SchemaNodeLeaf, SchemaNodeRef


def schema_extractor(thing):
    return Schema(Schema.feature_extractor(thing))


def process_to_schema(dask_bag, visualize):

    schema_bag = dask_bag.map(schema_extractor)\
        .fold(binop=Schema.merge, combine=Schema.merge,
              initial=Schema(None))

    if visualize:
        # import this here, so if not used we don't need the requirements
        from dask.dot import dot_graph
        schema_bag.visualize(visualize)

    # this will block until complete
    schema_obj = schema_bag.compute()

    # post process the schema to compute definitions

    return Schema(schema_obj)


def process_to_json(dask_bag, visualize):
    return process_to_schema(dask_bag, visualize).to_json()


def main():
    parser = argparse.ArgumentParser(description='JSON schema from JSON lines')
    parser.add_argument("output", help="optional filename to write to")
    parser.add_argument("input", nargs='+', 
                        help="one or more JSON lines filenames")
    parser.add_argument("--blocksize", action="store", default=None, type=str,
                        help="Size of blocks of input e.g. 128MiB")
    parser.add_argument("--workers", action="store", default="8", type=int,
                        help="Number of processess to use")
    parser.add_argument("--visualize", action="store", default=None, type=str,
                        help="Flag if compute graph should be displayed")

    args = parser.parse_args()

    cluster = LocalCluster()
    # here the client registers itself as the default within Dask
    # TODO explicitly call the client
    # TODO explicitly allow client to be passed
    client = Client(cluster)
    cluster.scale(args.workers)

    items = dask.bag.read_text(args.input, blocksize=args.blocksize)\
        .map(json.loads)

    with open(args.output, "w") as outfile:
        schema_json = process_to_json(items, args.visualize)
        json.dump(schema_json, outfile, indent=2, sort_keys=True)
