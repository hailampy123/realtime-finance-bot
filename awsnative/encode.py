"""Trade -> JSON, the AWS-native wire format.

The Kafka path sends bare Avro datums plus four Kafka headers. Kinesis records
have no headers, so the envelope has to change regardless; given that, JSON is
the better choice here because Firehose converts JSON to Parquet natively
against a Glue table schema. That removes a transform Lambda from the hot path.

trade.v1.avsc remains the single source of truth. Nothing in this module reads
the schema at runtime -- the guarantee is a contract test
(tests/awsnative/test_encode.py) asserting this output validates against it,
so drift fails CI instead of failing delivery.
"""

from __future__ import annotations

import json
from typing import Any

from ingest.core.codec import TRADE_SCHEMA_VERSION
from ingest.core.models import Trade

# Compact: no whitespace after separators. At ~350 bytes a record and a per-GB
# Kinesis and Firehose bill, pretty-printing is a line item.
_SEPARATORS = (",", ":")


def trade_to_dict(trade: Trade) -> dict[str, Any]:
    """The record as a plain dict, with schema_version added.

    Trade.to_avro() already stringifies the two StrEnums, so this is that dict
    plus the one field Kinesis's lack of headers forces into the body.
    """
    record = trade.to_avro()
    record["schema_version"] = TRADE_SCHEMA_VERSION
    return record


def encode_trade(trade: Trade) -> bytes:
    """UTF-8 JSON, one document, no trailing newline.

    Firehose's OpenX JSON deserializer reads exactly one JSON document per
    Kinesis record, so this must not emit newline-delimited batches.
    """
    return json.dumps(trade_to_dict(trade), separators=_SEPARATORS).encode()
