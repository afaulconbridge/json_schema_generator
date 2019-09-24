
import argparse

import simplejson as json
import dask
import dask.bag
from dask.distributed import Client, LocalCluster

from . import feature_extractor, SchemaNodeDict


def process(input, blocksize, workers, visualize):
    cluster = LocalCluster()
    # here the client registers itself as the default within Dask
    # TODO explicitly call the client
    # TODO explicitly allow client to be passed
    client = Client(cluster)
    cluster.scale(workers)

    items = dask.bag.read_text(input, blocksize=blocksize)\
        .map(json.loads)

    schema_bag = items.map(feature_extractor)\
        .fold(
            binop=SchemaNodeDict.merge,
            combine=SchemaNodeDict.merge,
            initial=SchemaNodeDict(None, [], frozenset()))

    if visualize:
        # import this here, so if not used we don't need the requirements
        from dask.dot import dot_graph
        schema_bag.visualize(visualize)

    # this will block until complete
    schema_obj = schema_bag.compute()

    schema_json = schema_obj.to_json()
    schema_json["$schema"] = "http://json-schema.org/draft-07/schema#"

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

    with open(args.output, "w") as outfile:
        schema_json = process(args.input, args.blocksize, args.workers,
                              args.visualize)
        json.dump(schema_json, outfile, indent=2, sort_keys=True)


if __name__ == "__main__":
    main()
