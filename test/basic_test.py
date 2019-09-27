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
    schema = json_schema_generator.process(items, False)
    print(json.dumps(schema, indent=2, sort_keys=True))
    assert schema["properties"]["a"]["type"] == "boolean"
    assert schema["properties"]["b"]["type"] == "string"
    assert schema["properties"]["c"]["type"] == "integer"
    assert schema["properties"]["d"]["type"] == "number"
    assert schema["properties"]["e"]["type"] == "null"
    assert schema["properties"]["f"]["type"] == "array"
    assert schema["properties"]["g"]["type"] == "object"


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
    schema = json_schema_generator.process(items, False)
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
    schema = json_schema_generator.process(items, False)
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
    schema = json_schema_generator.process(items, False)
    print(json.dumps(schema, indent=2, sort_keys=True))
    assert schema["properties"]["a"]["type"] == "string"
    assert schema["properties"]["a"]["const"] == "alpha"
