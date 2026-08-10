from __future__ import annotations

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

# Absolute, not ".env": a Jupyter kernel's cwd is the notebook's own folder,
# not the repo root, so a relative path here would silently read whatever
# .env happens to sit there instead of the real one -- see
# devlab/config.py's DEFAULT_TERRAFORM_DIR for the same problem, same fix.
_ENV_FILE = Path(__file__).resolve().parent.parent / ".env"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="INGEST_", env_file=_ENV_FILE)

    bootstrap_servers: str = "127.0.0.1:9092"
    sasl_username: str | None = None
    sasl_password: str | None = None
    universe_path: Path = Path("config/universe.yaml")
    venues: list[str] = ["binance", "coinbase"]
    queue_maxsize: int = 20_000
    trades_topic: str = "md.trades.v1"
