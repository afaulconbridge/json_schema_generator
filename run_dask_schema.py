import simplejson as json
import collections.abc
import argparse
import csv
import functools
import itertools

import dask
import dask.bag
# pip install dask[bag] 
from dask.distributed import Client,LocalCluster
# pip install dask[distributed] 
from dask.dot import dot_graph
# pip install dask[dot] 



class SchemaNode(object):
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return "SchemaNode({})".format(self.name)

    def to_json(self):
        raise NotImplementedError()

    def merge(self, other):
        raise NotImplementedError()

class SchemaNodeDict(SchemaNode):
    def __init__(self, name, children, required):
        super().__init__(name)
        assert isinstance(children, collections.abc.Iterable), "children must be iterable"
        assert isinstance(required, collections.abc.Set), "required must be a set"
        self.children = tuple(children)
        self.is_dict = True
        self.required = frozenset(required)

    def __repr__(self):
        return "SchemaNodeDict({},{})".format(self.name, self.children)

    def to_json(self):
        json = {}
        json["type"] = "object"
        json["properties"] = {}
        for child in self.children:
            json["properties"][child.name] = child.to_json()
        #TODO required
        if len(self.required) > 0:
            json["required"] = sorted(self.required)
        return json

    def merge(self, other):
        if other is None:
            return self
        assert isinstance(other, SchemaNode)
        assert self.__class__ == other.__class__
        assert self.name == other.name

        children = []
        self_childnames = frozenset([x.name for x in self.children])
        other_childnames = frozenset([x.name for x in other.children])
        childnames = sorted(self_childnames | other_childnames)

        for childname in childnames:
            try:
                self_child = [x for x in self.children if x.name == childname][0]
            except IndexError:
                self_child = None
            try:
                other_child = [x for x in other.children if x.name == childname][0]
            except IndexError:
                other_child = None

            if self_child is None:
                assert other_child is not None
                children.append(other_child)
            elif other_child is None:
                assert self_child is not None
                children.append(self_child)
            else:
                children.append(self_child.merge(other_child))

        #things can be marked as required if they are required in both
        required = self.required & other.required

        return SchemaNodeDict(self.name, children, required)


class SchemaNodeArray(SchemaNode):
    def __init__(self, name, children):
        super().__init__(name)
        assert isinstance(children, collections.abc.Iterable), "children must be iterable"
        self.children = children
        self.is_array = True

    def __repr__(self):
        return "SchemaNodeArray({},{})".format(self.name, self.children)

    def to_json(self):
        json = {}
        json["type"] = "array"
        if len(self.children) > 0:
            #TODO use merged children
            json["items"] = self.children[0].to_json()
        #TODO minlen maxlen
        return json

    def merge(self, other):
        if other is None:
            return self
        assert isinstance(other, SchemaNodeArray)
        assert self.name == other.name

        children = []
        for self_child, other_child in itertools.zip_longest(self.children, other.children):
            if self_child is None:
                assert other_child is not None
                children.append(other_child)
            elif other_child is None:
                assert self_child is not None
                children.append(self_child)
            else:
                children.append(self_child.merge(other_child))
        return SchemaNodeArray(self.name, children)

class SchemaNodeLeaf(SchemaNode):
    def __init__(self, name):
        super().__init__(name)

    def __repr__(self):
        return "SchemaNodeLeaf({})".format(self.name)

    def to_json(self):
        json = {}
        #TODO infer type
        return json

    def merge(self, other):
        if other is None:
            return self
        assert isinstance(other, SchemaNodeLeaf)
        assert self.name == other.name
        
        #TODO merge values
        return self


class SchemaNodeString(SchemaNodeLeaf):
    def __init__(self, name, values):
        assert isinstance(values, collections.abc.Iterable), "values must be iterable"
        super().__init__(name)
        self.values = frozenset(values)

    def __repr__(self):
        return "SchemaNodeString({})".format(self.name)

    def to_json(self):
        json = {}
        json["type"] = "string"
        #TODO maxLength ?
        #TODO minLength ?
        #TODO pattern ?
        return json

    def merge(self, other):
        if other is None:
            return self
        assert isinstance(other, SchemaNodeString)
        assert self.name == other.name
        
        # merge values
        child_values = self.values | other.values

        return SchemaNodeString(self.name, child_values)

def feature_extractor(thing, name=None):

    #if its a dict, recurse
    if isinstance(thing, collections.abc.Mapping):
        children = []
        for key in sorted(thing.keys()):
            child = feature_extractor(thing[key], key)
            children.append(child)
        required = frozenset((x.name for x  in children))
        return SchemaNodeDict(name, children, required)
    #if its a list, iterate
    #exclude strings because they are iterable by character
    elif isinstance(thing, collections.abc.Iterable) \
            and not isinstance(thing, str):
        children = [feature_extractor(x) for x in thing]
        return SchemaNodeArray(name, children)
    elif isinstance(thing, str):
        return SchemaNodeString(name, (thing,))
    else:
        return SchemaNodeLeaf(name)
    #TODO add number/integer
    #TODO add boolean
    #TODO add null ?

if __name__=="__main__":
    parser = argparse.ArgumentParser(description='JSON to feature CSV')
    parser.add_argument("output")
    parser.add_argument("input", nargs='+')
    parser.add_argument("--workers", action="store", default="8", type=int)
    args = parser.parse_args()

    cluster = LocalCluster()
    client = Client(cluster)
    cluster.scale(args.workers)

    items = dask.bag.read_text(args.input)\
        .map(json.loads)

    schema_obj = items.map(feature_extractor)\
        .fold(
            binop=SchemaNodeDict.merge, 
            combine=SchemaNodeDict.merge, 
            initial=SchemaNodeDict(None, [], frozenset()))\
        .compute()


    schema_json = schema_obj.to_json()
    schema_json["$schema"] = "http://json-schema.org/draft-07/schema#"

    print(json.dumps(schema_json, indent=2, sort_keys=True))

    with open(args.output, "w") as outfile:
        json.dump(schema_json, outfile, indent=2, sort_keys=True)