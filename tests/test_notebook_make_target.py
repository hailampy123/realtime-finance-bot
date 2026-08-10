from __future__ import annotations

import os
import subprocess


def dry_run(target: str, *, aws_profile: str | None = None) -> str:
    environment = os.environ.copy()
    for name in (
        "AWS_PROFILE",
        "FDAI_AWS_PROFILE",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_SECURITY_TOKEN",
    ):
        environment.pop(name, None)
    if aws_profile:
        environment["AWS_PROFILE"] = aws_profile

    result = subprocess.run(
        ["make", "-n", "notebook", f"TARGET={target}"],
        capture_output=True,
        text=True,
        check=False,
        env=environment,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout


def test_local_notebook_launch_has_no_aws_preflight():
    output = dry_run("local")

    assert "prepare_msk_notebook" not in output
    assert "FDAI_TARGET=local" in output
    assert "AWS_PROFILE=" not in output


def test_msk_notebook_launch_prepares_access_and_inherits_default_profile():
    output = dry_run("msk")

    assert "python -m scripts.prepare_msk_notebook --profile fdai-sandbox" in output
    assert "AWS_PROFILE=fdai-sandbox" in output
    assert "FDAI_TARGET=terraform" in output


def test_existing_aws_profile_beats_the_project_default():
    output = dry_run("msk", aws_profile="team-sandbox")

    assert "--profile team-sandbox" in output
    assert "AWS_PROFILE=team-sandbox" in output
    assert "fdai-sandbox" not in output
