import dask.bag
import simplejson as json
import json_schema_generator


def test_prototype():
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
