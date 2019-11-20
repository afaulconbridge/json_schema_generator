

Run tests
=========

To run the unit tests and get a coverage report run:
```
pytest -vv --cov=json_schema_generator
```


Upate requirements.txt
======================

To update `requirements.txt` from `requirements.in` run:
```
pip-compile requirements.in
```


JSON schema
===========

https://json-schema.org/



Examples
========

https://json-schema.org/draft/2019-09/json-schema-core.html#rfc.section.9.2.1

The boolean logic of subschemas part can be used to define combinations of properties.

Two mutually-exclusive options may be represented as:

```jsonlines
{"a":1}
{"b":1}
```

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "properties": {
    "a": {
      "type": "integer"
    },
    "b": {
      "type": "integer"
    }
  },
  "type": "object",
  "oneOf": [
      {
        "required": ["a"]
      },
      {
        "required": ["b"]
      }
  ]
}
```

Two options that co-occur may be respresented as:

```jsonlines
{"a":1,"b":1,"c":1}
{"c":1}
{"a":1,"b":1}
```

```json
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
    }
  },
  "type": "object",
  "oneOf": [
      {
        "required": ["a","b"]
      },
      {
        "required": []
      }
  ]
}
```