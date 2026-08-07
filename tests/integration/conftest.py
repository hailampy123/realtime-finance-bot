import os
from pathlib import Path

import pytest

# pytest_collection_modifyitems fires once per session with every collected
# item, regardless of which conftest.py defines it -- it is not scoped to
# this directory automatically. Filter explicitly so `make check` (which
# collects the whole tests/ tree) only skips tests that live under here.
INTEGRATION_DIR = Path(__file__).parent


def pytest_collection_modifyitems(config, items):
    if os.environ.get("RUN_INTEGRATION") == "1":
        return
    skip = pytest.mark.skip(reason="set RUN_INTEGRATION=1 with docker compose running")
    for item in items:
        if INTEGRATION_DIR in item.path.parents:
            item.add_marker(skip)
