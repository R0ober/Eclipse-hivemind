"""Ensure a completed run cannot pass the pilot gate with invalid observations."""

import importlib.util
import json
from pathlib import Path

import pytest


@pytest.fixture
def pilot(tmp_path, monkeypatch):
    path = Path(__file__).resolve().parents[1] / "scripts" / "verify_enforced_groups.py"
    spec = importlib.util.spec_from_file_location("verify_enforced_groups", path)
    verifier = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(verifier)
    monkeypatch.setattr(verifier, "ROOT", tmp_path)
    directory = tmp_path / "outputs" / "pilot"
    directory.mkdir(parents=True)
    (directory / "config.json").write_text(json.dumps({"node_types": {"adversarial": {"count": 4}}}))
    groups = [["normal-0", *[f"adversarial-{i}" for i in range(4)]],
              [f"normal-{i}" for i in range(1, 6)]]
    records = []
    for group in groups:
        for node in group:
            records.append({"node": {"node_id": node}, "event": {
                "event_type": "node_started", "data": {"hivemind_address": f"/p2p/id-{node}"}}})
            records.append({"node": {"node_id": node}, "event": {
                "event_type": "eval_metrics", "data": {"round": 20}}})
            for index in range(20):
                records.append({"node": {"node_id": node}, "event": {
                    "event_type": "averaging_completed", "data": {
                        "round": index, "success": True, "fallback": False,
                        "group_members": [f"id-{member}" for member in group]}}})

    def check():
        (directory / "events.jsonl").write_text("\n".join(json.dumps(record) for record in records))
        return verifier.verify_run("pilot")

    return records, check


def test_complete_successful_pilot_passes(pilot):
    _, check = pilot
    assert check()["complete"] is True


@pytest.mark.parametrize("fault", ["cross_group", "missing", "failed", "duplicate"])
def test_invalid_membership_or_coverage_fails_the_gate(pilot, fault):
    records, check = pilot
    index = next(i for i, record in enumerate(records)
                 if record["event"]["event_type"] == "averaging_completed")
    data = records[index]["event"]["data"]
    if fault == "cross_group":
        data["group_members"][-1] = "id-normal-1"
    elif fault == "missing":
        records.pop(index)
    elif fault == "failed":
        data["success"] = False
    else:
        records.append(records[index])
    with pytest.raises(ValueError):
        check()
