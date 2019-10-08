import dask.bag
from json_schema_generator import SchemaNodeLeaf, \
    SchemaNodeDict, Schema, process_to_schema


import random


def test_node_pair():
    foo_a = SchemaNodeLeaf("a", ["a"], "string")
    bar_a = SchemaNodeLeaf("a", ["a"], "string")
    foo = SchemaNodeDict("foo", [foo_a], ())
    bar = SchemaNodeDict("bar", [bar_a], ())
    root = SchemaNodeDict(None, [foo, bar], ())
    schema = Schema(root)

    node_pairs = tuple(schema._ref_node_pairs())
    print(node_pairs)
    assert len(node_pairs) == 1
    assert node_pairs[0] == sorted((bar, foo))


def test_node_components():
    foo_a = SchemaNodeLeaf("a", ["a"], "string")
    bar_a = SchemaNodeLeaf("a", ["a"], "string")
    baz_a = SchemaNodeLeaf("a", ["a"], "string")
    foo_b = SchemaNodeLeaf("b", ["b"], "string")
    bar_b = SchemaNodeLeaf("b", ["b"], "string")
    baz_b = SchemaNodeLeaf("b", ["b"], "string")
    foo = SchemaNodeDict("foo", [foo_a, foo_b], ())
    bar = SchemaNodeDict("bar", [bar_a, bar_b], ())
    baz = SchemaNodeDict("baz", [baz_a, baz_b], ())
    root = SchemaNodeDict(None, [foo, bar, baz], ())
    schema = Schema(root)
    components = schema._ref_components()
    assert len(components) == 4


def test_infer_refs():
    foo_a = SchemaNodeLeaf("a", ["a"], "string")
    bar_a = SchemaNodeLeaf("a", ["a"], "string")
    baz_a = SchemaNodeLeaf("a", ["a"], "string")
    foo_b = SchemaNodeLeaf("b", ["b"], "string")
    bar_b = SchemaNodeLeaf("b", ["b"], "string")
    baz_b = SchemaNodeLeaf("b", ["b"], "string")
    foo = SchemaNodeDict("foo", [foo_a, foo_b], ())
    bar = SchemaNodeDict("bar", [bar_a, bar_b], ())
    baz = SchemaNodeDict("baz", [baz_a, baz_b], ())
    root = SchemaNodeDict(None, [foo, bar, baz], ())
    schema = Schema(root)
    schema.infer_references()
    assert len(schema.definitions) == 1


def test_random_content():
    street_prefix = ("High", "Station", "Hill", "Owl", "Badger", "Civit")
    street_suffix = ("Street", "Lane", "Drive", "Road")
    names = ("Alice", "Bob", "Carol", "David", "Emma", "Fred",  
             "Gini", "Harry", "Irene", "John")

    def random_address(rng):
        return {
            "street": rng.choice(street_prefix)+" "+rng.choice(street_suffix)
        }

    def random_name(rng):
        return rng.choice(names)

    def random_content(rng):
        return {
            "from": {
                "name": random_name(rng),
                "address": random_address(rng)
            },
            "to": {
                "name": random_name(rng),
                "address": random_address(rng)
            }
        }

    # create some content
    rng = random.Random(42)
    source = [random_content(rng) for i in range(25)]
    schema = process_to_schema(
        dask.bag.from_sequence(source), False)
