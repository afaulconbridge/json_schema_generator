import dask.bag
import simplejson as json
import json_schema_generator

import random


def test_coocurance():
    rng = random.Random(42)

    items = []
    for _ in range(100):
        item = {}
        for keyset in ("abd", "c"):
            if rng.random() < 0.5:
                for key in keyset:
                    item[key] = rng.randint(0, 100)
        items.append(item)

    items = dask.bag.from_sequence(items)
    schema = json_schema_generator.process_to_schema(items, False).to_json()

    print(json.dumps(schema, indent=2, sort_keys=True))

    schema = json.loads(json.dumps(schema))

    expected_schema = json.loads("""
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "properties": {
    "a": {
      "type": "integer"
    },
    "b": {
      "type": "integer"
    },
    "c": {
      "type": "integer"
    },
    "d": {
      "type": "integer"
    }
  },
  "type": "object",
  "oneOf": [
    {
      "required": ["a","b","d"]
    },
    {
      "not": {
        "anyOf": [
          {
            "required":["a"]
          },
          {
            "required":["b"]
          },
          {
            "required":["d"]
          }
        ]
      }
    }
  ]
}
""")

    assert schema == expected_schema


def test_coocurance_multi():
    rng = random.Random(42)

    items = []
    for _ in range(100):
        item = {}
        for keyset in ("abc", "def"):
            if rng.random() < 0.5:
                for key in keyset:
                    item[key] = rng.randint(0, 100)
        items.append(item)

    items = dask.bag.from_sequence(items)
    schema = json_schema_generator.process_to_schema(items, False).to_json()

    print(json.dumps(schema, indent=2, sort_keys=True))

    schema = json.loads(json.dumps(schema))

    expected_schema = json.loads("""
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "properties": {
    "a": { "type": "integer" },
    "b": { "type": "integer" },
    "c": { "type": "integer" },
    "d": { "type": "integer" },
    "e": { "type": "integer" },
    "f": { "type": "integer" }
  },
  "type": "object",
  "allOf": [
    {
      "oneOf": [
        { "required": ["a","b","c"] },
        { "not": 
          { "anyOf": 
            [
              { "required": ["a"] },
              { "required": ["b"] },
              { "required": ["c"] }
            ]
          }
        }
      ]
    },
    {
      "oneOf": [
        { "required": ["d","e","f"] },
        { 
          "not": 
          { "anyOf": 
            [
              { "required": [ "d" ] },
              { "required": [ "e" ] },
              { "required": [ "f" ] }
            ]
          }
        }
      ]
    }
  ]
}
""")

    assert schema == expected_schema