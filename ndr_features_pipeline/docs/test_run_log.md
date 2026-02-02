# Test Run Log

- `pytest -q`
  - Result: failed during collection because the environment is missing `pyspark` and the `ndr` module on `PYTHONPATH`.
- `pytest -q` (rerun)
  - Result: failed during collection for the same missing `pyspark` and `ndr` module dependencies.
- `pytest -q`
  - Result: failed during collection because `pyspark` is missing and the `ndr` module is not on `PYTHONPATH`.
