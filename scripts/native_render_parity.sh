#!/usr/bin/env bash
# Prove that Terraform and Python render the merge SQL to the same bytes.
#
# awsnative/sql holds one copy of each transform, read by two things that
# cannot see each other: Terraform's templatefile(), which bakes the statements
# into the Step Functions definition and is therefore what actually runs, and
# awsnative/render.py, which the offline tests and manual runs use.
#
# "One definition, two consumers" is a claim until something checks it. The two
# renderers agree on ${...} syntax but not on everything around it -- file()
# does not strip trailing newlines and read_text() can be made to -- so this
# drifts silently the moment someone adds a .strip() for tidiness. The symptom
# would be tests passing against SQL that is not the SQL in production.
#
# Offline: no AWS credentials, no providers, no backend. Safe in CI.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

cat > "$TMP/main.tf" <<'HCL'
variable "sql_dir" { type = string }
locals {
  valid_expr = file("${var.sql_dir}/fragments/valid_trade.sql")
  dirty_cte = templatefile("${var.sql_dir}/fragments/dirty_from_bronze.sql", {
    database = "parity_check", lookback_days = 1
  })
  merge_silver = templatefile("${var.sql_dir}/merge_silver_trades.sql", {
    database = "parity_check", lookback_days = 1, valid_expr = local.valid_expr
  })
  merge_quarantine = templatefile("${var.sql_dir}/merge_silver_quarantine.sql", {
    database = "parity_check", lookback_days = 1, valid_expr = local.valid_expr
  })
  merge_gold = templatefile("${var.sql_dir}/merge_gold_bars_1m.sql", {
    database = "parity_check", dirty_cte = local.dirty_cte
  })
  merge_perp_context = templatefile("${var.sql_dir}/merge_silver_perp_context.sql", {
    database = "parity_check", lookback_days = 1
  })
  merge_macro = templatefile("${var.sql_dir}/merge_silver_macro.sql", {
    database = "parity_check"
  })
}
# One map, because a Terraform output name may not contain a dot and these are
# keyed by filename to line up with what render.merge_statements() and
# render.enrichment_statements() return.
output "rendered" {
  value = {
    "merge_silver_trades.sql"       = local.merge_silver
    "merge_silver_quarantine.sql"   = local.merge_quarantine
    "merge_gold_bars_1m.sql"        = local.merge_gold
    "merge_silver_perp_context.sql" = local.merge_perp_context
    "merge_silver_macro.sql"        = local.merge_macro
  }
}
HCL

terraform -chdir="$TMP" init -input=false >/dev/null
terraform -chdir="$TMP" apply -auto-approve -input=false \
  -var="sql_dir=$ROOT/awsnative/sql" >/dev/null
terraform -chdir="$TMP" output -json > "$TMP/rendered.json"

cd "$ROOT"
uv run --group awsnative python - "$TMP/rendered.json" <<'PY'
import difflib
import json
import sys

from awsnative.render import enrichment_statements, merge_statements

terraform = json.load(open(sys.argv[1]))["rendered"]["value"]
python = {
    **merge_statements("parity_check", lookback_days=1),
    **enrichment_statements("parity_check", lookback_days=1),
}

if set(terraform) != set(python):
    print(f"different statement sets: terraform {sorted(terraform)} vs python {sorted(python)}")
    sys.exit(1)

drifted = 0
for name in sorted(terraform):
    tf, py = terraform[name], python[name]
    if tf == py:
        print(f"  {name:32} identical ({len(tf)} chars)")
        continue
    drifted += 1
    print(f"  {name:32} DRIFTED (terraform {len(tf)}, python {len(py)})")
    diff = difflib.unified_diff(
        tf.splitlines(), py.splitlines(), "terraform", "python", lineterm=""
    )
    for line in list(diff)[:30]:
        print(f"      {line}")

if drifted:
    print(f"\n{drifted} statement(s) drifted: the SQL in the state machine is not the SQL under test.")
    sys.exit(1)
print("\nRenderers agree.")
PY
