import collections.abc
import numbers
import argparse

import simplejson as json
import dask
import dask.bag
from dask.distributed import Client, LocalCluster

from .schema import Schema, SchemaNodeDict, SchemaNodeArray, \
    SchemaNodeLeaf, SchemaNodeRef


def feature_extractor(thing, name=None):

    # if its a dict, recurse
    if isinstance(thing, collections.abc.Mapping):
        children = set()
        for key in sorted(thing.keys()):
            child = feature_extractor(thing[key], key)
            children.add(child)
        children = frozenset(children)
        required = frozenset((x.name for x in children))
        return SchemaNodeDict(name, children, required)
    # if its a list, iterate
    # exclude strings because they are iterable by character
    elif isinstance(thing, collections.abc.Iterable) \
            and not isinstance(thing, str):
        children = [feature_extractor(x) for x in thing]
        return SchemaNodeArray(name, children)
    elif isinstance(thing, str):
        return SchemaNodeLeaf(name, [thing], "string")
    elif isinstance(thing, bool):
        return SchemaNodeLeaf(name, [thing], "boolean")
    elif isinstance(thing, int):
        return SchemaNodeLeaf(name, [thing], "integer")
    elif isinstance(thing, numbers.Number):
        return SchemaNodeLeaf(name, [thing], "number")
    elif thing is None:
        return SchemaNodeLeaf(name, [thing], "null")
    else:
        #shouldn't get here so raise an exception in case
        raise ValueError("Unrecognized thing {}".format(thing))

def process_to_schema(items, visualize):

    schema_bag = items.map(feature_extractor)\
        .fold(
            binop=SchemaNodeDict.merge,
            combine=SchemaNodeDict.merge,
            initial=SchemaNodeDict(None, frozenset(), frozenset()))

    if visualize:
        # import this here, so if not used we don't need the requirements
        from dask.dot import dot_graph
        schema_bag.visualize(visualize)

    # this will block until complete
    schema_obj = schema_bag.compute()

    #post process the schema to compute definitions

    return Schema(schema_obj)


def process_to_json(items, visualize):
    schema_obj = process_to_schema(items, visualize)
    schema_json = schema_obj.to_json()
    return schema_json


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
