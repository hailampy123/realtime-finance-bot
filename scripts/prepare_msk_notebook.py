"""Prepare Terraform-managed MSK access before launching a notebook."""

from __future__ import annotations

import argparse
import ipaddress
import os
import subprocess
import sys
import urllib.request
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DEV_DIR = PROJECT_ROOT / "infra" / "envs" / "dev"
RAW_AWS_CREDENTIAL_VARIABLES = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_SECURITY_TOKEN",
)


class PreparationError(RuntimeError):
    """The MSK notebook preflight could not establish safe access."""


def clean_aws_environment(
    profile: str,
    source: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Select one named profile without stale raw credentials taking precedence."""
    environment = dict(os.environ if source is None else source)
    for name in RAW_AWS_CREDENTIAL_VARIABLES:
        environment.pop(name, None)
    environment["AWS_PROFILE"] = profile
    return environment


def _run(
    args: list[str],
    *,
    env: Mapping[str, str] | None = None,
    capture_output: bool = True,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        capture_output=capture_output,
        text=True,
        check=False,
        env=env,
    )


def _successful_output(result: subprocess.CompletedProcess[str]) -> str | None:
    value = result.stdout.strip()
    return value if result.returncode == 0 and value not in ("", "None") else None


def ensure_aws_credentials(profile: str, environment: Mapping[str, str]) -> str:
    """Return the AWS account, refreshing an expired SSO session once if possible."""
    identity_command = [
        "aws",
        "sts",
        "get-caller-identity",
        "--query",
        "Account",
        "--output",
        "text",
    ]
    identity = _run(identity_command, env=dict(environment))
    account = _successful_output(identity)
    if account:
        return account

    sso_session = _run(
        ["aws", "configure", "get", "sso_session", "--profile", profile],
        env=dict(environment),
    )
    sso_start_url = _run(
        ["aws", "configure", "get", "sso_start_url", "--profile", profile],
        env=dict(environment),
    )
    if not (_successful_output(sso_session) or _successful_output(sso_start_url)):
        raise PreparationError(
            f"AWS profile {profile!r} is invalid or expired and is not configured for "
            "SSO, so it cannot be refreshed automatically."
        )

    login = _run(
        ["aws", "sso", "login", "--profile", profile],
        env=dict(environment),
        capture_output=False,
    )
    if login.returncode != 0:
        raise PreparationError(f"AWS SSO login failed for profile {profile!r}")

    identity = _run(identity_command, env=dict(environment))
    account = _successful_output(identity)
    if not account:
        raise PreparationError(
            f"AWS profile {profile!r} is still invalid after SSO login: {identity.stderr.strip()}"
        )
    return account


def _current_public_ip() -> str:
    with urllib.request.urlopen("https://checkip.amazonaws.com", timeout=15) as response:
        content: bytes = response.read()
        return content.decode("ascii").strip()


def _required_output(
    args: list[str],
    *,
    environment: Mapping[str, str],
    error: str,
) -> str:
    result = _run(args, env=dict(environment))
    value = _successful_output(result)
    if not value:
        detail = result.stderr.strip()
        raise PreparationError(f"{error}{f': {detail}' if detail else ''}")
    return value


def _verify_access(dev_dir: Path) -> None:
    import devlab

    target = devlab.from_terraform(dev_dir)
    topic_names = {topic.name for topic in devlab.topics(target)}
    if "md.trades.v1" not in topic_names:
        raise PreparationError(
            "MSK is reachable but md.trades.v1 is missing; run `make up` to finish "
            "topic provisioning."
        )


@contextmanager
def _temporary_environment(environment: Mapping[str, str]) -> Iterator[None]:
    previous = os.environ.copy()
    os.environ.clear()
    os.environ.update(environment)
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(previous)


def prepare(profile: str, dev_dir: Path = DEFAULT_DEV_DIR) -> None:
    """Refresh the current laptop `/32` through Terraform and verify Kafka access."""
    environment = clean_aws_environment(profile)
    dev_dir = dev_dir.resolve()

    with _temporary_environment(environment):
        account = ensure_aws_credentials(profile, environment)
        print(f"AWS profile {profile!r}: account {account}")

        init = _run(
            ["terraform", f"-chdir={dev_dir}", "init", "-input=false", "-reconfigure"],
            env=environment,
        )
        if init.returncode != 0:
            raise PreparationError(f"Terraform initialization failed: {init.stderr.strip()}")

        cluster_arn = _required_output(
            [
                "terraform",
                f"-chdir={dev_dir}",
                "output",
                "-raw",
                "cluster_arn",
            ],
            environment=environment,
            error="No deployed MSK stack was found; run `make up` once",
        )
        try:
            region = cluster_arn.split(":", maxsplit=4)[3]
        except IndexError as exc:
            raise PreparationError(
                f"Terraform returned an invalid MSK cluster ARN: {cluster_arn!r}"
            ) from exc

        _required_output(
            [
                "aws",
                "kafka",
                "get-bootstrap-brokers",
                "--cluster-arn",
                cluster_arn,
                "--region",
                region,
                "--query",
                "BootstrapBrokerStringPublicSaslScram",
                "--output",
                "text",
            ],
            environment=environment,
            error="The deployed MSK cluster has no public SASL/SCRAM endpoint",
        )

        try:
            operator_ip = ipaddress.ip_address(_current_public_ip())
        except ValueError as exc:
            raise PreparationError("Could not determine a valid public IP address") from exc
        if operator_ip.version != 4:
            raise PreparationError(
                f"MSK operator access currently requires an IPv4 address, got {operator_ip}"
            )

        operator_cidrs = f'["{operator_ip}/32"]'
        apply = _run(
            [
                "terraform",
                f"-chdir={dev_dir}",
                "apply",
                "-input=false",
                "-auto-approve",
                "-var=msk_public_access=true",
                "-var=msk_restrict_acls=true",
                f"-var=operator_cidrs={operator_cidrs}",
            ],
            env=environment,
        )
        if apply.returncode != 0:
            raise PreparationError(f"Terraform access refresh failed: {apply.stderr.strip()}")

        _verify_access(dev_dir)
        print(f"MSK notebook access ready for {operator_ip}/32")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--dev-dir", type=Path, default=DEFAULT_DEV_DIR)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        prepare(args.profile, args.dev_dir)
    except PreparationError as exc:
        print(f"MSK notebook preflight failed: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
