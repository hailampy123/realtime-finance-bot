from __future__ import annotations

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

# Absolute for the same reason ingest/settings.py is: a process whose cwd is
# not the repo root would otherwise silently read a different .env.
_ENV_FILE = Path(__file__).resolve().parent.parent / ".env"


class NativeSettings(BaseSettings):
    """Config for the AWS-native producer.

    Separate from ingest.Settings, and a separate env prefix, so that neither
    path can be started with the other's configuration by accident -- there is
    no bootstrap_servers here to leave pointing at a dead broker.
    """

    model_config = SettingsConfigDict(env_prefix="FDAI_NATIVE_", env_file=_ENV_FILE)

    stream_name: str = "fdai-native-md-trades-v1"
    universe_path: Path = Path("config/universe.yaml")
    venues: list[str] = ["binance", "coinbase"]
    queue_maxsize: int = 20_000
    trades_topic: str = "md.trades.v1"
