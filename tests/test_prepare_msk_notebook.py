from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from scripts import prepare_msk_notebook as prepare


def completed(
    args: list[str],
    *,
    returncode: int = 0,
    stdout: str = "",
    stderr: str = "",
) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(args, returncode, stdout, stderr)


def test_named_profile_removes_raw_credentials_that_would_override_it():
    source = {
        "AWS_ACCESS_KEY_ID": "stale-key",
        "AWS_SECRET_ACCESS_KEY": "stale-secret",
        "AWS_SESSION_TOKEN": "stale-token",
        "AWS_SECURITY_TOKEN": "stale-security-token",
        "AWS_PROFILE": "wrong-profile",
        "PATH": "/usr/bin",
    }

    environment = prepare.clean_aws_environment("fdai-sandbox", source)

    assert environment == {
        "AWS_PROFILE": "fdai-sandbox",
        "PATH": "/usr/bin",
    }


def test_expired_sso_profile_is_logged_in_once_and_rechecked(monkeypatch):
    calls: list[list[str]] = []
    sts_attempts = 0

    def fake_run(args: list[str], **kwargs) -> subprocess.CompletedProcess[str]:
        nonlocal sts_attempts
        calls.append(args)
        if args[:3] == ["aws", "sts", "get-caller-identity"]:
            sts_attempts += 1
            if sts_attempts == 1:
                return completed(args, returncode=255, stderr="ExpiredToken")
            return completed(args, stdout="160071257600\n")
        if args[:3] == ["aws", "configure", "get"]:
            return completed(args, stdout="fdai-sso\n")
        if args[:3] == ["aws", "sso", "login"]:
            return completed(args)
        raise AssertionError(f"unexpected command: {args}")

    monkeypatch.setattr(prepare, "_run", fake_run)

    account = prepare.ensure_aws_credentials(
        "fdai-sandbox",
        prepare.clean_aws_environment("fdai-sandbox", {}),
    )

    assert account == "160071257600"
    assert calls.count(["aws", "sso", "login", "--profile", "fdai-sandbox"]) == 1
    assert sts_attempts == 2


def test_missing_stack_stops_before_terraform_apply(monkeypatch, tmp_path):
    calls: list[list[str]] = []

    def fake_run(args: list[str], **kwargs) -> subprocess.CompletedProcess[str]:
        calls.append(args)
        if args[:3] == ["aws", "sts", "get-caller-identity"]:
            return completed(args, stdout="160071257600\n")
        if args[0] == "terraform" and "init" in args:
            return completed(args)
        if args[0] == "terraform" and args[-2:] == ["-raw", "cluster_arn"]:
            return completed(args, returncode=1, stderr="Output not found")
        raise AssertionError(f"unexpected command: {args}")

    monkeypatch.setattr(prepare, "_run", fake_run)

    with pytest.raises(prepare.PreparationError, match="make up"):
        prepare.prepare("fdai-sandbox", tmp_path)

    assert not any("apply" in args for args in calls)


def test_prepare_applies_current_ip_and_verifies_metadata(monkeypatch, tmp_path):
    calls: list[list[str]] = []
    verified: list[Path] = []
    cluster_arn = (
        "arn:aws:kafka:us-east-1:160071257600:cluster/"
        "fdai-kafka/3917b19e-4114-414b-a4be-d7c8c3cbe328-23"
    )

    def fake_run(args: list[str], **kwargs) -> subprocess.CompletedProcess[str]:
        calls.append(args)
        if args[:3] == ["aws", "sts", "get-caller-identity"]:
            return completed(args, stdout="160071257600\n")
        if args[0] == "terraform" and "init" in args:
            return completed(args)
        if args[0] == "terraform" and args[-2:] == ["-raw", "cluster_arn"]:
            return completed(args, stdout=f"{cluster_arn}\n")
        if args[:3] == ["aws", "kafka", "get-bootstrap-brokers"]:
            return completed(args, stdout="b-1-public.live-msk.amazonaws.com:9196\n")
        if args[0] == "terraform" and "apply" in args:
            return completed(args)
        raise AssertionError(f"unexpected command: {args}")

    monkeypatch.setattr(prepare, "_run", fake_run)
    monkeypatch.setattr(prepare, "_current_public_ip", lambda: "171.243.48.245")
    monkeypatch.setattr(prepare, "_verify_access", lambda dev_dir: verified.append(dev_dir))

    original_environment = os.environ.copy()
    prepare.prepare("fdai-sandbox", tmp_path)

    apply = next(args for args in calls if args[0] == "terraform" and "apply" in args)
    assert "-var=msk_public_access=true" in apply
    assert "-var=msk_restrict_acls=true" in apply
    assert '-var=operator_cidrs=["171.243.48.245/32"]' in apply
    assert verified == [tmp_path]
    assert os.environ == original_environment
