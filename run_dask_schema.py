import simplejson as json
import collections.abc
import argparse
import csv
import functools
import itertools
import numbers

import dask
import dask.bag
# pip install dask[bag] 
from dask.distributed import Client,LocalCluster
# pip install dask[distributed] 

ENUM_LIMIT=10


class SchemaNode(object):
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return "{}({})".format(self.__class__.name, self.name)

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

    def to_json(self):
        json = {}
        json["type"] = "array"
        if len(self.children) > 0:
            #merge children together
            #TODO what if position matters?
            children_merged = self.children[0]
            for child in self.children[1:]:
                children_merged = children_merged.merge(child)
            json["items"] = children_merged.to_json()
        #TODO minlen maxlen
        #TODO uniqueItems
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
    def __init__(self, name, values, datatype):
        assert values == None or isinstance(values, collections.abc.Mapping), "values must be mapping"
        super().__init__(name)
        self.values = values
        self.datatype = datatype

    def to_json(self):
        json = {}

        if self.datatype is not None:
            json["type"] = self.datatype
            #if there a few unique values, its an enum
            #otherwise, its free values and we can't store all
            if self.values is not None:
                if len(self.values) == 1:
                    json["const"] = tuple(self.values)[0]
                else:
                    json["enum"] = sorted((x for x in self.values.keys()))
            elif self.datatype == "string":
                #TODO min_length
                #TODO max_length
                #TODO format 
                #TODO pattern ?
                pass
            elif self.datatype == "integer":
                #TODO maximum / exclusiveMaximum
                #TODO minimum / exclusiveMinimum
                #TODO multipleOf ?
                pass

            #TODO number / integer

        return json

    def merge(self, other):
        if other is None:
            return self
        assert isinstance(other, self.__class__)
        assert self.name == other.name

        if other.datatype != self.datatype:
            child_datatype = None
        else:
            child_datatype = self.datatype
        
        # merge values
        if self.values is None or other.values is None:
            child_values = None
        else:
            child_values = dict()
            for value in self.values:
                if isinstance(value, str):
                    assert len(value) > 0

                if value not in child_values:
                    child_values[value] = 0
                child_values[value] += 1
            for value in other.values:
                if isinstance(value, str):
                    assert len(value) > 0
                    
                if value not in child_values:
                    child_values[value] = 0
                child_values[value] += 1

            #if we now have too many different values, don't be enum
            if len(child_values) > ENUM_LIMIT:
                child_values = None
        
        return self.__class__(self.name, child_values, child_datatype)

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
        return SchemaNodeLeaf(name, {thing:1}, "string")
    elif isinstance(thing, int):
        return SchemaNodeLeaf(name, {thing:1}, "integer")
    elif isinstance(thing, numbers.Number):
        return SchemaNodeLeaf(name, {thing:1}, "number")
    else:
        return SchemaNodeLeaf(name, {thing:1}, None)
    #TODO add number/integer
    #TODO add boolean
    #TODO add null ?

if __name__=="__main__":
    parser = argparse.ArgumentParser(description='JSON to feature CSV')
    parser.add_argument("output")
    parser.add_argument("input", nargs='+')
    parser.add_argument("--blocksize", action="store", default=None, type=str)
    parser.add_argument("--workers", action="store", default="8", type=int)
    parser.add_argument("--visualize", action="store", default=None, type=str)
    args = parser.parse_args()

    cluster = LocalCluster()
    client = Client(cluster)
    cluster.scale(args.workers)

    items = dask.bag.read_text(args.input, blocksize=args.blocksize)\
        .map(json.loads)

    schema_bag = items.map(feature_extractor)\
        .fold(
            binop=SchemaNodeDict.merge, 
            combine=SchemaNodeDict.merge, 
            initial=SchemaNodeDict(None, [], frozenset()))

    if args.visualize:
        #import this here, so if visualize is not used we don't need the requirements
        from dask.dot import dot_graph
        # pip install dask[dot] 
        schema_bag.visualize(args.visualize)

    schema_obj = schema_bag.compute()


    schema_json = schema_obj.to_json()
    schema_json["$schema"] = "http://json-schema.org/draft-07/schema#"

    print(json.dumps(schema_json, indent=2, sort_keys=True))

    with open(args.output, "w") as outfile:
        json.dump(schema_json, outfile, indent=2, sort_keys=True)