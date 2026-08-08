from __future__ import annotations

from confluent_kafka.admin import AclOperation, AclPermissionType, ResourcePatternType, ResourceType

from scripts.create_acls import (
    CLUSTER_RESOURCE_NAME,
    DEFAULT_PRINCIPAL,
    build_config,
    wanted_bindings,
)


def test_every_resource_type_a_client_touches_is_covered():
    # A missing one does not fail at grant time; it fails later as an opaque
    # authorization error from whichever client happened to need it first.
    covered = {b.restype for b in wanted_bindings(DEFAULT_PRINCIPAL)}
    assert covered == {
        ResourceType.TOPIC,
        ResourceType.GROUP,
        ResourceType.BROKER,
        ResourceType.TRANSACTIONAL_ID,
    }


def test_the_cluster_binding_uses_kafkas_canonical_name():
    # librdkafka spells CLUSTER as BROKER, and the resource has one legal
    # name. Get either wrong and the grant silently covers nothing.
    (cluster,) = [b for b in wanted_bindings(DEFAULT_PRINCIPAL) if b.restype == ResourceType.BROKER]
    assert cluster.name == CLUSTER_RESOURCE_NAME == "kafka-cluster"


def test_every_binding_allows_rather_than_denies():
    for binding in wanted_bindings(DEFAULT_PRINCIPAL):
        assert binding.permission_type == AclPermissionType.ALLOW
        assert binding.operation == AclOperation.ALL
        assert binding.resource_pattern_type == ResourcePatternType.LITERAL


def test_the_default_principal_is_any_authenticated_user():
    # Authentication is the access control here, not authorization — see the
    # module docstring and docs/SETUP.md.
    assert DEFAULT_PRINCIPAL == "User:*"
    assert all(b.principal == "User:*" for b in wanted_bindings(DEFAULT_PRINCIPAL))


def test_the_principal_can_be_narrowed():
    bindings = wanted_bindings("User:fdai-producer")
    assert {b.principal for b in bindings} == {"User:fdai-producer"}


def test_bindings_are_distinct():
    bindings = wanted_bindings(DEFAULT_PRINCIPAL)
    assert len(set(bindings)) == len(bindings)


def test_credentials_enable_scram():
    config = build_config("b-1:9096", "user", "pw")
    assert config["security.protocol"] == "SASL_SSL"
    assert config["sasl.mechanisms"] == "SCRAM-SHA-512"


def test_no_credentials_means_plaintext():
    # The local compose broker has no auth; the same script must work there.
    assert build_config("localhost:9092", None, None) == {"bootstrap.servers": "localhost:9092"}


def test_partial_credentials_do_not_half_configure_sasl():
    assert "sasl.username" not in build_config("b-1:9096", "user", None)
    assert "sasl.username" not in build_config("b-1:9096", None, "pw")
