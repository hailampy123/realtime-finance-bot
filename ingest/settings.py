from __future__ import annotations

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="INGEST_", env_file=".env")

    bootstrap_servers: str = "localhost:9092"
    sasl_username: str | None = None
    sasl_password: str | None = None
    universe_path: Path = Path("config/universe.yaml")
    venues: list[str] = ["binance", "coinbase"]
    queue_maxsize: int = 20_000
    trades_topic: str = "md.trades.v1"
