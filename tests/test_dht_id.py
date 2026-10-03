import asyncio
import sys
import types
from pathlib import Path

import pytest

pytest.importorskip("hivemind")

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src" / "eclipse_hivemind" / "nodes"))

from hivemind.dht.routing import DHTID  # noqa: E402
from shared import dht  # noqa: E402


class FakeDHT:
    """Runs a run_coroutine callback against a node holding a fixed DHTID."""

    def __init__(self, node_id: DHTID) -> None:
        self._node = types.SimpleNamespace(node_id=node_id)

    def run_coroutine(self, callback) -> object:
        return asyncio.run(callback(self, self._node))


def test_unset_source_leaves_the_id_to_hivemind(monkeypatch) -> None:
    monkeypatch.delenv("PARAM_DHT_ID_SOURCE", raising=False)

    assert dht.dht_id() is None


def test_empty_source_counts_as_unset(monkeypatch) -> None:
    monkeypatch.setenv("PARAM_DHT_ID_SOURCE", "")

    assert dht.dht_id() is None


def test_same_source_gives_the_same_id(monkeypatch) -> None:
    monkeypatch.setenv("PARAM_DHT_ID_SOURCE", "adversary-0")
    first = dht.dht_id()
    monkeypatch.setenv("PARAM_DHT_ID_SOURCE", "adversary-0")

    assert first == dht.dht_id()


def test_different_sources_give_different_ids(monkeypatch) -> None:
    monkeypatch.setenv("PARAM_DHT_ID_SOURCE", "adversary-0")
    first = dht.dht_id()
    monkeypatch.setenv("PARAM_DHT_ID_SOURCE", "adversary-1")

    assert first != dht.dht_id()


def test_resolved_dht_id_reads_the_running_node_id() -> None:
    node_id = DHTID.generate(source=b"adversary-0")

    resolved = dht.resolved_dht_id(FakeDHT(node_id))

    assert resolved == f"{int(node_id):040x}"
    assert len(resolved) == 40
    assert int(resolved, 16) == int(node_id)
