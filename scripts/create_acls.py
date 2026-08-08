"""Grant Kafka ACLs, then prove they took. Idempotent.

Why this exists, and why it runs where it does:

MSK refuses to enable public access unless the broker configuration sets
`allow.everyone.if.no.acl.found=false`. That single setting flips Kafka's
authorizer to deny-by-default, which locks out *every* SCRAM principal --
including the one that would create the ACLs, since CreateAcls itself needs
Alter on the cluster. A cluster tightened before its ACLs exist cannot be
recovered from the client side at all; the only way out is to loosen the
configuration again.

So the ACLs must exist first, and they can only be written from inside the
VPC: before public access is on, the in-VPC endpoint is the only one that
resolves. The producer host is the one thing in the VPC, which is why
scripts/bootstrap.sh runs this over SSH rather than from your laptop.

The grant is `User:*` -- any authenticated principal -- with full access. That
is deliberate and matches what docs/SETUP.md already states: SASL/SCRAM plus
the security-group IP allowlist is the access control here, not per-topic
authorization. Narrowing it would mean provisioning an ACL for every new topic
and consumer group, where a missing one surfaces as a confusing runtime auth
error rather than a clear failure.
"""

from __future__ import annotations

import argparse
import sys
from typing import Any

from confluent_kafka.admin import (
    AclBinding,
    AclBindingFilter,
    AclOperation,
    AclPermissionType,
    AdminClient,
    ResourcePatternType,
    ResourceType,
)

from ingest.settings import Settings

DEFAULT_PRINCIPAL = "User:*"
ANY_HOST = "*"

# The cluster resource has a fixed canonical name in the Kafka protocol.
# librdkafka calls resource type 4 BROKER; Kafka itself calls it CLUSTER.
CLUSTER_RESOURCE_NAME = "kafka-cluster"


def wanted_bindings(principal: str) -> list[AclBinding]:
    """Full access on every resource type a producer or consumer can touch.

    TRANSACTIONAL_ID is included even though nothing uses transactions yet:
    the producer runs with enable.idempotence, and an idempotent producer that
    is later made transactional would otherwise fail at an unrelated time.
    """
    return [
        AclBinding(
            ResourceType.TOPIC,
            "*",
            ResourcePatternType.LITERAL,
            principal,
            ANY_HOST,
            AclOperation.ALL,
            AclPermissionType.ALLOW,
        ),
        AclBinding(
            ResourceType.GROUP,
            "*",
            ResourcePatternType.LITERAL,
            principal,
            ANY_HOST,
            AclOperation.ALL,
            AclPermissionType.ALLOW,
        ),
        AclBinding(
            ResourceType.BROKER,
            CLUSTER_RESOURCE_NAME,
            ResourcePatternType.LITERAL,
            principal,
            ANY_HOST,
            AclOperation.ALL,
            AclPermissionType.ALLOW,
        ),
        AclBinding(
            ResourceType.TRANSACTIONAL_ID,
            "*",
            ResourcePatternType.LITERAL,
            principal,
            ANY_HOST,
            AclOperation.ALL,
            AclPermissionType.ALLOW,
        ),
    ]


def build_config(bootstrap: str, username: str | None, password: str | None) -> dict[str, Any]:
    config: dict[str, Any] = {"bootstrap.servers": bootstrap}
    if username and password:
        config |= {
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "SCRAM-SHA-512",
            "sasl.username": username,
            "sasl.password": password,
        }
    return config


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    # All three default to the INGEST_* settings rather than being required,
    # so the producer host can run this as `docker run --env-file .env ...`
    # with no credentials on the command line -- bootstrap.sh invokes it over
    # SSH, and argv is world-readable in `ps` for as long as the call runs.
    parser.add_argument("--bootstrap")
    parser.add_argument("--username")
    parser.add_argument("--password")
    parser.add_argument("--principal", default=DEFAULT_PRINCIPAL)
    parser.add_argument("--timeout", type=float, default=60.0)
    args = parser.parse_args()

    settings = Settings()
    bootstrap = args.bootstrap or settings.bootstrap_servers
    username = args.username or settings.sasl_username
    password = args.password or settings.sasl_password

    print(f"granting ACLs for {args.principal} on {bootstrap}")
    admin = AdminClient(build_config(bootstrap, username, password))
    bindings = wanted_bindings(args.principal)

    failed = 0
    for binding, future in admin.create_acls(bindings, request_timeout=args.timeout).items():
        try:
            future.result(timeout=args.timeout)
            print(f"granted {binding.restype.name} {binding.name} -> {binding.principal}")
        except Exception as exc:
            print(f"FAILED {binding.restype.name} {binding.name}: {exc}", file=sys.stderr)
            failed += 1
    if failed:
        return 1

    # Read them back. bootstrap.sh gates the deny-by-default flip on this exit
    # code, so "the create call returned OK" is not good enough -- the point is
    # to be certain before the cluster stops trusting unauthenticated intent.
    existing = admin.describe_acls(
        AclBindingFilter(
            ResourceType.ANY,
            None,
            ResourcePatternType.ANY,
            None,
            None,
            AclOperation.ANY,
            AclPermissionType.ANY,
        ),
        request_timeout=args.timeout,
    ).result(timeout=args.timeout)

    missing = [b for b in bindings if b not in set(existing)]
    if missing:
        for binding in missing:
            print(f"MISSING after create: {binding}", file=sys.stderr)
        return 1

    print(f"VERIFIED: {len(bindings)} ACLs present for {args.principal}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
