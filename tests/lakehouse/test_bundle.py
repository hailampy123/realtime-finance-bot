"""Static checks on the bundle.

`databricks bundle validate` is the live check (see `make pipeline-validate`);
these run with no network and no auth, so a wrong edition or an accidental
switch to serverless fails in the ordinary test suite.
"""

from __future__ import annotations

from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")


def _load(path: str) -> dict:
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))


def _pipeline() -> dict:
    return _load("resources/trades.pipeline.yml")["resources"]["pipelines"]["trades_bronze_silver"]


def test_targets_the_verified_catalog_and_schema():
    settings = _pipeline()
    assert settings["catalog"] == "fdai"
    assert settings["schema"] == "market"


def test_edition_is_advanced():
    # PRO does not include CDC at all, and AUTO CDC is the whole design.
    assert _pipeline()["edition"] == "ADVANCED"


def test_compute_is_classic_not_serverless():
    # Serverless egress addresses rotate; MSK is reachable only from an
    # IP allowlist, so classic is a networking requirement, not a preference.
    settings = _pipeline()
    assert settings["serverless"] is False
    assert settings["clusters"][0]["node_type_id"] == "m5d.xlarge"


def test_driver_has_at_least_four_cores():
    # An observed failure, not a theoretical one: m5d.large (2 cores) died with
    # "Spark driver failed to start within the startup timeout. This commonly
    # occurs on instances with fewer than 4 CPU cores." The `.large` sizes in
    # every AWS family are 2-core, so downsizing to one for cost reasons breaks
    # the pipeline rather than saving money.
    node = _pipeline()["clusters"][0]["node_type_id"]
    assert not node.endswith(".large"), (
        f"{node} is a 2-core instance; the DLT driver needs at least 4 cores"
    )


def test_pipeline_is_triggered_not_continuous():
    assert _pipeline()["continuous"] is False


def test_session_timezone_is_configured_utc():
    # Belt and braces with the shell's spark.conf.set: a workspace default of
    # anything but UTC would shift every derived event_ts.
    assert _pipeline()["configuration"]["spark.sql.session.timeZone"] == "UTC"


def test_bundle_declares_the_dev_target():
    bundle = _load("databricks.yml")
    assert bundle["bundle"]["name"] == "finance-data-ai"
    assert "dev" in bundle["targets"]
