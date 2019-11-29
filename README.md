
development
-----------

Install using pip including development extras

```sh
pip install -e .[dev]
```

tests
-----

To run the unit tests and get a coverage report run:
```
pytest -v --cov=json_schema_generator
```

requirements.txt
----------------

To update `requirements.txt` from `requirements.in` run:
```
pip-compile requirements.in
```
