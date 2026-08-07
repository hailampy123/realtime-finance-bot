from scripts.create_topics import TOPIC_SPECS, to_new_topic


def test_every_spec_matches_the_design():
    by_name = {spec.name: spec for spec in TOPIC_SPECS}
    assert by_name["md.trades.v1"].partitions == 6
    assert by_name["md.trades.v1"].retention_ms == 24 * 3600 * 1000
    assert by_name["md.book.top.v1"].retention_ms == 6 * 3600 * 1000
    assert by_name["md.book.depth.v1"].retention_ms == 2 * 3600 * 1000
    assert by_name["md.bars.v1"].retention_ms == 48 * 3600 * 1000
    assert by_name["news.articles.v1"].retention_ms == 7 * 24 * 3600 * 1000


def test_every_data_topic_has_a_dead_letter_queue():
    names = {spec.name for spec in TOPIC_SPECS}
    for topic in ("md.trades.v1", "md.book.top.v1", "news.articles.v1"):
        assert f"_dlq.{topic}" in names


def test_new_topic_carries_retention_and_compression():
    spec = next(s for s in TOPIC_SPECS if s.name == "md.trades.v1")
    topic = to_new_topic(spec, replication_factor=2)
    assert topic.num_partitions == 6
    assert topic.config["retention.ms"] == str(24 * 3600 * 1000)
    assert topic.config["compression.type"] == "zstd"


def test_dlq_topics_are_single_partition():
    for spec in TOPIC_SPECS:
        if spec.name.startswith("_dlq."):
            assert spec.partitions == 1
