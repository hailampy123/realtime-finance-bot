"""Where to connect, and with what credentials.

Two brokers matter during development: the single-node compose broker on
localhost, and the real MSK cluster that `make up` provisions. Notebooks should
not know which one they are talking to, so they call `resolve()` and read
`FDAI_TARGET` from the environment.

MSK credentials are not duplicated here. `ingest.settings.Settings` already
reads `INGEST_*` from `.env`, so `msk()` reuses it; `from_terraform()` exists
because the broker DNS changes on every `make up`, which makes a hardcoded
`.env` entry actively misleading rather than merely stale.
"""

from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass, field
from typing import Any

from ingest.settings import Settings

LOCAL_BOOTSTRAP = "localhost:9092"
DEFAULT_TARGET = "local"
DEFAULT_TERRAFORM_DIR = "infra/envs/dev"
TERRAFORM_TIMEOUT_S = 60.0


class TargetError(RuntimeError):
    """Raised when a target cannot be built. The message must say what to run next."""


@dataclass(frozen=True, slots=True)
class Target:
    """A resolved broker endpoint.

    The password is excluded from `repr` on purpose: notebooks echo the value of
    the last expression in a cell, and a `Target` is exactly the sort of thing
    you evaluate bare to check what you are pointed at.
    """

    name: str
    bootstrap: str
    sasl_username: str | None = None
    sasl_password: str | None = field(default=None, repr=False)

    @property
    def uses_sasl(self) -> bool:
        return bool(self.sasl_username and self.sasl_password)

    def _auth(self) -> dict[str, Any]:
        if not self.uses_sasl:
            return {}
        return {
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "SCRAM-SHA-512",
            "sasl.username": self.sasl_username,
            "sasl.password": self.sasl_password,
        }

    def admin_config(self) -> dict[str, Any]:
        return {"bootstrap.servers": self.bootstrap} | self._auth()

    def consumer_config(self, *, group: str, offset_reset: str = "latest") -> dict[str, Any]:
        """Config for an ad-hoc read.

        Auto-commit is off: re-running a notebook cell should re-read the same
        data, not silently advance an offset that a later cell depends on.
        """
        if offset_reset not in ("earliest", "latest"):
            raise ValueError(f"offset_reset must be 'earliest' or 'latest', got {offset_reset!r}")
        return {
            "bootstrap.servers": self.bootstrap,
            "group.id": group,
            "auto.offset.reset": offset_reset,
            "enable.auto.commit": False,
        } | self._auth()


def local() -> Target:
    """The compose broker.

    Deliberately ignores `.env`: if your `.env` points at MSK, asking for
    `local` should give you local rather than a surprise round-trip to AWS.
    """
    return Target(name="local", bootstrap=LOCAL_BOOTSTRAP)


def msk() -> Target:
    """MSK, from `INGEST_*` env vars or `.env` (see docs/SETUP.md §5a)."""
    settings = Settings()
    if not (settings.sasl_username and settings.sasl_password):
        raise TargetError(
            "target 'msk' needs INGEST_SASL_USERNAME and INGEST_SASL_PASSWORD "
            f"(bootstrap currently {settings.bootstrap_servers!r}). Either set them in .env, "
            "or use devlab.from_terraform() to read them straight from the stack outputs."
        )
    return Target(
        name="msk",
        bootstrap=settings.bootstrap_servers,
        sasl_username=settings.sasl_username,
        sasl_password=settings.sasl_password,
    )


def _terraform_output(chdir: str, name: str) -> str:
    try:
        result = subprocess.run(
            ["terraform", f"-chdir={chdir}", "output", "-raw", name],
            capture_output=True,
            text=True,
            timeout=TERRAFORM_TIMEOUT_S,
            check=False,
        )
    except FileNotFoundError as exc:
        raise TargetError("terraform is not on PATH; see docs/SETUP.md §1") from exc
    except subprocess.TimeoutExpired as exc:
        raise TargetError(
            f"`terraform output {name}` timed out after {TERRAFORM_TIMEOUT_S}s"
        ) from exc
    if result.returncode != 0:
        raise TargetError(
            f"`terraform -chdir={chdir} output -raw {name}` failed. Is the stack up? "
            f"Run `make up` first.\n{result.stderr.strip()}"
        )
    return result.stdout.strip()


def from_terraform(chdir: str = DEFAULT_TERRAFORM_DIR) -> Target:
    """Read the live MSK endpoint and SCRAM credentials from the dev stack.

    Slower than `msk()` (three subprocesses) but always correct — broker DNS is
    regenerated on every `make up`, so this is the only source that cannot go
    stale. Cache the result in a variable rather than calling it per cell.
    """
    return Target(
        name="msk",
        bootstrap=_terraform_output(chdir, "bootstrap_brokers_public"),
        sasl_username=_terraform_output(chdir, "sasl_username"),
        sasl_password=_terraform_output(chdir, "sasl_password"),
    )


def resolve(name: str | None = None) -> Target:
    """Pick a target by name, defaulting to `$FDAI_TARGET`, then `local`."""
    chosen = (name or os.environ.get("FDAI_TARGET") or DEFAULT_TARGET).strip().lower()
    if chosen == "local":
        return local()
    if chosen == "msk":
        return msk()
    if chosen == "terraform":
        return from_terraform()
    raise TargetError(f"unknown target {chosen!r}; expected 'local', 'msk', or 'terraform'")
