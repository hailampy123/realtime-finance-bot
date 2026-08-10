"""Settings must find .env at the repo root regardless of the caller's cwd.

Jupyter sets a notebook kernel's cwd to the notebook's own folder, not the
repo root -- the same problem devlab/config.py already solved for
DEFAULT_TERRAFORM_DIR. A relative env_file would silently read whatever .env
happens to sit in that folder instead of the real one, with no error at all --
worse than a loud failure, since nothing raises and the settings just look
wrong.
"""

from __future__ import annotations

from pathlib import Path

from ingest import settings as settings_module
from ingest.settings import Settings


def test_default_env_file_is_pinned_to_the_repo_root():
    repo_root = Path(__file__).resolve().parent.parent.parent
    assert settings_module._ENV_FILE == repo_root / ".env"


def test_settings_ignores_a_env_file_in_the_callers_cwd(tmp_path, monkeypatch):
    decoy_dir = tmp_path / "notebooks"
    decoy_dir.mkdir()
    (decoy_dir / ".env").write_text("INGEST_BOOTSTRAP_SERVERS=DECOY_VALUE\n")
    monkeypatch.chdir(decoy_dir)

    assert Settings().bootstrap_servers != "DECOY_VALUE"
