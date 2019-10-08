import dask.bag
import simplejson as json
import json_schema_generator


def test_types():
    items = dask.bag.from_sequence([{
            "a": True,
            "b": "Hello world", 
            "c": 1, 
            "d": 1.5,
            "e": None,
            "f": [],
            "g": {}
        }])
    schema = json_schema_generator.process_to_schema(items, False).to_json()
    print(json.dumps(schema, indent=2, sort_keys=True))
    assert schema["properties"]["a"]["type"] == "boolean"
    assert schema["properties"]["b"]["type"] == "string"
    assert schema["properties"]["c"]["type"] == "integer"
    assert schema["properties"]["d"]["type"] == "number"
    assert schema["properties"]["e"]["type"] == "null"
    assert schema["properties"]["f"]["type"] == "array"
    assert schema["properties"]["g"]["type"] == "object"


# test equalities better, fine grained, detailed
def test_equal_leaf():
    a1 = json_schema_generator.SchemaNodeLeaf("a", ["foo"], "string")
    a2 = json_schema_generator.SchemaNodeLeaf("a", ["foo"], "string")
    b = json_schema_generator.SchemaNodeLeaf("b", ["foo"], "string")
    c = json_schema_generator.SchemaNodeLeaf("a", ["bar"], "string")
    d1 = json_schema_generator.SchemaNodeLeaf("a", [1], "number")
    d2 = json_schema_generator.SchemaNodeLeaf("a", [1], "integer")

    assert a1 == a1
    assert a1 == a2
    assert a1 != b
    assert a1 != c
    assert a1 != d1
    assert a1 != d2


def test_equal_ref():
    a1 = json_schema_generator.SchemaNodeRef("a", "foo")
    a2 = json_schema_generator.SchemaNodeRef("a", "foo")
    b = json_schema_generator.SchemaNodeRef("a", "bar")
    c = json_schema_generator.SchemaNodeRef("b", "foo")

    assert a1 == a1
    assert a1 == a2
    assert a1 != b
    assert a1 != c


def test_equal_array():
    a1 = json_schema_generator.SchemaNodeArray("a", (
        json_schema_generator.SchemaNodeLeaf(None, ["foo"], "string"),
        json_schema_generator.SchemaNodeLeaf(None, ["bar"], "string")))
    a2 = json_schema_generator.SchemaNodeArray("a", (
        json_schema_generator.SchemaNodeLeaf(None, ["foo"], "string"),
        json_schema_generator.SchemaNodeLeaf(None, ["bar"], "string")))
    b = json_schema_generator.SchemaNodeArray("a", (
        json_schema_generator.SchemaNodeLeaf(None, [1], "integer"),
        json_schema_generator.SchemaNodeLeaf(None, [2], "integer")))
    c = json_schema_generator.SchemaNodeArray("b", (
        json_schema_generator.SchemaNodeLeaf(None, ["foo"], "string"),
        json_schema_generator.SchemaNodeLeaf(None, ["bar"], "string")))
    d = json_schema_generator.SchemaNodeArray("a", (
        json_schema_generator.SchemaNodeLeaf(None, ["foo"], "string"),
        json_schema_generator.SchemaNodeLeaf(None, ["bar"], "string"),
        json_schema_generator.SchemaNodeLeaf(None, ["baz"], "string")))

    assert a1 == a1
    assert a1 == a2
    assert a1 != b
    assert a1 != c
    assert a1 != d


def test_equal_dict():
    a1 = json_schema_generator.SchemaNodeDict("a", (
        json_schema_generator.SchemaNodeLeaf("foo", ["foo"], "string"),
        json_schema_generator.SchemaNodeLeaf("bar", ["bar"], "string")),
        ())
    a2 = json_schema_generator.SchemaNodeDict("a", (
        json_schema_generator.SchemaNodeLeaf("foo", ["foo"], "string"),
        json_schema_generator.SchemaNodeLeaf("bar", ["bar"], "string")),
        ())
    b = json_schema_generator.SchemaNodeDict("a", (
        json_schema_generator.SchemaNodeLeaf("foo", [1], "integer"),
        json_schema_generator.SchemaNodeLeaf("bar", [2], "integer")),
        ())
    c = json_schema_generator.SchemaNodeDict("b", (
        json_schema_generator.SchemaNodeLeaf("foo", ["foo"], "string"),
        json_schema_generator.SchemaNodeLeaf("bar", ["bar"], "string")),
        ())
    d = json_schema_generator.SchemaNodeDict("a", (
        json_schema_generator.SchemaNodeLeaf("foo", ["foo"], "string"),
        json_schema_generator.SchemaNodeLeaf("bar", ["bar"], "string"),
        json_schema_generator.SchemaNodeLeaf("baz", ["baz"], "string")),
        ())

    assert a1 == a1
    assert a1 == a2
    assert a1 != b
    assert a1 != c
    assert a1 != d

    assert len(a1) == 3


def test_combining_types():
    items = dask.bag.from_sequence([{
            "a": True,
            "b": "Hello world", 
            "c": 1, 
            "d": 1.5,
            "e": None,
        }, {
            "b": True,
            "c": "Hello world", 
            "d": 1, 
            "e": 1.5,
            "a": None,
        }])
    schema = json_schema_generator.process_to_schema(items, False).to_json()
    print(json.dumps(schema, indent=2, sort_keys=True))
    # because we are mixing types there cannot be any type validation
    assert "type" not in schema["properties"]["a"]
    assert "type" not in schema["properties"]["b"]
    assert "type" not in schema["properties"]["c"]
    assert "type" not in schema["properties"]["d"]
    assert "type" not in schema["properties"]["e"]


def test_enum():
    items = dask.bag.from_sequence([{
            "a": "alpha"
        }, {
            "a": "ah"
        }])
    schema = json_schema_generator.process_to_schema(items, False).to_json()
    print(json.dumps(schema, indent=2, sort_keys=True))
    assert schema["properties"]["a"]["type"] == "string"
    assert "enum" in schema["properties"]["a"]
    assert ["ah", "alpha"] == schema["properties"]["a"]["enum"]


def test_const():
    items = dask.bag.from_sequence([{
            "a": "alpha"
        }, {
            "a": "alpha"
        }])
    schema = json_schema_generator.process_to_schema(items, False).to_json()
    print(json.dumps(schema, indent=2, sort_keys=True))
    assert schema["properties"]["a"]["type"] == "string"
    assert schema["properties"]["a"]["const"] == "alpha"
