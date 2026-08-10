from __future__ import annotations

import pytest

from devlab import config
from devlab.config import LOCAL_BOOTSTRAP, Target, TargetError


class StubSettings:
    """Stands in for ingest.settings.Settings so a developer's own .env,
    which is gitignored and may point anywhere, cannot change the result."""

    def __init__(self, bootstrap: str, username: str | None, password: str | None) -> None:
        self.bootstrap_servers = bootstrap
        self.sasl_username = username
        self.sasl_password = password


def stub_settings(monkeypatch, bootstrap="broker:9196", username="u", password="p") -> None:
    monkeypatch.setattr(config, "Settings", lambda: StubSettings(bootstrap, username, password))


def test_local_target_needs_no_credentials():
    target = config.local()
    assert target.bootstrap == LOCAL_BOOTSTRAP
    assert target.uses_sasl is False
    assert "security.protocol" not in target.consumer_config(group="g")


def test_local_ignores_env_pointing_elsewhere(monkeypatch):
    stub_settings(monkeypatch, bootstrap="msk-broker:9196")
    assert config.local().bootstrap == LOCAL_BOOTSTRAP


def test_password_is_not_in_repr():
    target = Target("msk", "broker:9196", "user", "hunter2")
    assert "hunter2" not in repr(target)
    assert "user" in repr(target)


def test_msk_config_carries_scram():
    target = Target("msk", "broker:9196", "user", "secret")
    consumer = target.consumer_config(group="g")
    assert consumer["security.protocol"] == "SASL_SSL"
    assert consumer["sasl.mechanisms"] == "SCRAM-SHA-512"
    assert consumer["sasl.password"] == "secret"


def test_consumer_config_disables_autocommit():
    # Re-running a notebook cell must re-read the same data.
    assert config.local().consumer_config(group="g")["enable.auto.commit"] is False


def test_consumer_config_rejects_a_bad_offset_reset():
    with pytest.raises(ValueError, match="offset_reset"):
        config.local().consumer_config(group="g", offset_reset="beginning")


def test_partial_credentials_do_not_enable_sasl():
    assert Target("msk", "broker:9196", "user", None).uses_sasl is False
    assert Target("msk", "broker:9196", None, "secret").uses_sasl is False


def test_msk_requires_credentials(monkeypatch):
    stub_settings(monkeypatch, username=None, password=None)
    with pytest.raises(TargetError, match="from_terraform"):
        config.msk()


def test_msk_reads_settings(monkeypatch):
    stub_settings(monkeypatch, bootstrap="b-1.msk:9196", username="scram", password="pw")
    target = config.msk()
    assert (target.name, target.bootstrap, target.sasl_username) == ("msk", "b-1.msk:9196", "scram")


def test_resolve_defaults_to_local(monkeypatch):
    monkeypatch.delenv("FDAI_TARGET", raising=False)
    assert config.resolve().name == "local"


def test_resolve_reads_the_env_var(monkeypatch):
    stub_settings(monkeypatch)
    monkeypatch.setenv("FDAI_TARGET", "MSK")
    assert config.resolve().name == "msk"


def test_explicit_name_beats_the_env_var(monkeypatch):
    monkeypatch.setenv("FDAI_TARGET", "msk")
    assert config.resolve("local").name == "local"


def test_resolve_rejects_an_unknown_target(monkeypatch):
    monkeypatch.delenv("FDAI_TARGET", raising=False)
    with pytest.raises(TargetError, match="unknown target"):
        config.resolve("staging")


def test_from_terraform_reports_a_failed_output(monkeypatch):
    class Result:
        returncode = 1
        stdout = ""
        stderr = "No outputs found"

    monkeypatch.setattr(config.subprocess, "run", lambda *a, **k: Result())
    with pytest.raises(TargetError, match="make up"):
        config.from_terraform()


def test_from_terraform_builds_a_target(monkeypatch):
    outputs = {
        "bootstrap_brokers_public": "b-1.msk:9196",
        "sasl_username": "scram-user",
        "sasl_password": "scram-pass",
    }

    class Result:
        returncode = 0
        stderr = ""

        def __init__(self, stdout: str) -> None:
            self.stdout = stdout

    def fake_run(cmd, **kwargs):
        return Result(outputs[cmd[-1]] + "\n")

    monkeypatch.setattr(config.subprocess, "run", fake_run)
    target = config.from_terraform()
    assert target.bootstrap == "b-1.msk:9196"
    assert target.uses_sasl is True


def test_expired_credentials_get_a_distinct_message_from_a_missing_stack(monkeypatch):
    # "Is the stack up? Run make up" is actively wrong advice when the real
    # problem is a stale token in *this* process -- re-running make up from a
    # terminal with valid credentials would just succeed, masking that a
    # Jupyter kernel's environment is what's actually stale.
    class Result:
        returncode = 1
        stdout = ""
        stderr = (
            "Error: validating provider credentials: retrieving caller identity from STS: "
            "operation error STS: GetCallerIdentity, https response error StatusCode: 403, "
            "RequestID: x, api error InvalidClientTokenId: The security token included in "
            "the request is invalid."
        )

    monkeypatch.setattr(config.subprocess, "run", lambda *a, **k: Result())
    with pytest.raises(TargetError, match="credentials are invalid or expired") as excinfo:
        config.from_terraform()
    # The improved message still mentions `make up` (to say it won't help) --
    # what must NOT survive is the old, actively-wrong generic phrasing.
    assert "Is the stack up? Run" not in str(excinfo.value)


def test_a_genuinely_missing_stack_keeps_the_make_up_hint(monkeypatch):
    class Result:
        returncode = 1
        stdout = ""
        stderr = 'Error: Output "cluster_arn" not found'

    monkeypatch.setattr(config.subprocess, "run", lambda *a, **k: Result())
    with pytest.raises(TargetError, match="make up"):
        config.from_terraform()


def test_a_successful_but_empty_bootstrap_output_is_not_treated_as_working(monkeypatch):
    # AWS's GetBootstrapBrokers is fetched by the provider exactly once, at the
    # moment public connectivity finishes, and Terraform caches whatever it got --
    # including empty, if AWS was not ready yet (see infra/envs/dev/outputs.tf).
    # This is a real state on an otherwise-healthy cluster (returncode 0, valid
    # credentials, sasl_username/password resolve fine) and must not silently
    # build a Target that fails confusingly three calls later.
    outputs = {
        "bootstrap_brokers_public": "",
        "sasl_username": "fdai-producer",
        "sasl_password": "secret",
    }

    class Result:
        returncode = 0
        stderr = ""

        def __init__(self, stdout: str) -> None:
            self.stdout = stdout

    def fake_run(cmd, **kwargs):
        return Result(outputs[cmd[-1]] + "\n")

    monkeypatch.setattr(config.subprocess, "run", fake_run)
    with pytest.raises(TargetError, match="cached") as excinfo:
        config.from_terraform()
    # Must not read like the credentials-expired or missing-stack cases --
    # re-running `make up` does not fix this, since bootstrap.sh never writes
    # its own AWS-CLI-polled value back into Terraform's cached output.
    assert "make up` will NOT fix" in str(excinfo.value)
