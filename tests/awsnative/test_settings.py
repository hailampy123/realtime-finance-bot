from __future__ import annotations

from awsnative.settings import NativeSettings


def test_legacy_ingest_dotenv_keys_do_not_block_native_settings(tmp_path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "INGEST_BOOTSTRAP_SERVERS=legacy-broker.example:9196\n"
        "INGEST_SASL_USERNAME=legacy-user\n"
        "FDAI_NATIVE_STREAM_NAME=test-native-stream\n",
        encoding="utf-8",
    )

    settings = NativeSettings(_env_file=env_file)

    assert settings.stream_name == "test-native-stream"
    assert settings.venues == ["binance", "coinbase"]
