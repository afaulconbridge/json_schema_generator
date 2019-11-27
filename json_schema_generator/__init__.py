import argparse
import functools
import itertools
import contextlib
import fileinput

import simplejson as json
import dask
import dask.bag
from dask.distributed import Client, LocalCluster

# import everything so it is re-exported
from .schema import Schema, SchemaNode, SchemaNodeArray, SchemaNodeDict, \
    SchemaNodeLeaf, SchemaNodeRef


def process_to_schema(items):
    schemas = map(Schema.schema_extractor, items)
    schema = functools.reduce(Schema.merge, schemas, Schema(None))
    # post-process the schema to compute definitions
    schema.infer_references()
    return schema


def process_to_schema_dask(dask_bag, visualize):
    dask_bag = dask_bag.map(Schema.schema_extractor)\
        .fold(binop=Schema.merge, combine=Schema.merge,
              initial=Schema(None))
    if visualize:
        # import this here, so if not used we don't need the requirements
        from dask.dot import dot_graph
        dask_bag.visualize(visualize)

    # this will block until complete
    schema = dask_bag.compute()

    # post-process the schema to compute definitions
    schema.infer_references()

    return schema


def process_to_json_dask(dask_bag, visualize):
    return process_to_schema_dask(dask_bag, visualize).to_json()


def main():
    parser = argparse.ArgumentParser(description='JSON schema from JSON lines')
    parser.add_argument("output", help="optional filename to write to, - for stdout")
    parser.add_argument("input", nargs='+', 
                        help="one or more JSON lines filenames")
    parser.add_argument("--blocksize", action="store", default=None, type=str,
                        help="Size of blocks of input e.g. 128MiB")
    parser.add_argument("--workers", action="store", default="1", type=int,
                        help="Number of processess to use")
    parser.add_argument("--visualize", action="store", default=None, type=str,
                        help="Flag if compute graph should be displayed")
    args = parser.parse_args()

    if args.workers > 1:
        cluster = LocalCluster()
        # here the client registers itself as the default within Dask
        # TODO explicitly call the client
        # TODO explicitly allow client to be passed
        client = Client(cluster)
        cluster.scale(args.workers)

        items = dask.bag.read_text(args.input, blocksize=args.blocksize)\
            .map(json.loads)
        schema = process_to_schema_dask(items, args.visualize)
    else:
        if len(args.input) == 1 and args.input[0] == "-":
            lines = fileinput.input()
            items = map(json.loads, lines)
            schema = process_to_schema(items)
        else:
            with fileinput.input(files=args.input) as files:
                items = map(json.loads, files)
                schema = process_to_schema(items)

    schema_json = schema.to_json()
    if args.output == "-":
        print(json.dumps(schema_json, indent=2, sort_keys=True))
    else:
        with open(args.output, "w") as outfile:
            json.dump(schema_json, outfile, indent=2, sort_keys=True)
