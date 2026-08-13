"""Databricks lakehouse code: Bronze/Silver transforms and pipeline definitions.

Transformation logic lives in `lakehouse.trades.transforms` as plain PySpark so
it is testable on a laptop. `lakehouse.pipelines.*` holds the declarative shells
that only import cleanly on Databricks Runtime.
"""
