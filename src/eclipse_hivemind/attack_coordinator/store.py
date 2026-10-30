"""Durable state and audit events for the coordinated-attacker control channel."""

from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from pathlib import Path


class CoordinationStore:
    """Store one plan and a sequence of short-lived leader claims per run."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._db = sqlite3.connect(path, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode = DELETE")
        self._db.execute("PRAGMA synchronous = FULL")
        self._db.executescript("""
            CREATE TABLE IF NOT EXISTS plans (
                experiment_id TEXT PRIMARY KEY,
                victim_peer_id TEXT NOT NULL,
                recruiter_peer_ids_json TEXT NOT NULL,
                created_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS claims (
                claim_id TEXT PRIMARY KEY,
                experiment_id TEXT NOT NULL,
                leader_peer_id TEXT NOT NULL,
                status TEXT NOT NULL,
                group_id TEXT,
                expires_at REAL NOT NULL,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS events (
                ordinal INTEGER PRIMARY KEY,
                experiment_id TEXT NOT NULL,
                occurred_at REAL NOT NULL,
                event_type TEXT NOT NULL,
                data_json TEXT NOT NULL
            );
        """)
        self._db.commit()

    def close(self) -> None:
        self._db.close()

    def _event(self, experiment_id: str, event_type: str, **data) -> None:
        self._db.execute(
            "INSERT INTO events (experiment_id, occurred_at, event_type, data_json) VALUES (?, ?, ?, ?)",
            (experiment_id, time.time(), event_type, json.dumps(data, sort_keys=True)),
        )

    def configure_plan(self, experiment_id: str, victim_peer_id: str, recruiter_peer_ids: list[str]) -> dict:
        recruiters = tuple(sorted(set(recruiter_peer_ids)))
        if len(recruiters) != 3 or victim_peer_id in recruiters:
            raise ValueError("a plan requires one distinct Victim and exactly three distinct recruiters")
        with self._lock, self._db:
            current = self._db.execute("SELECT * FROM plans WHERE experiment_id = ?", (experiment_id,)).fetchone()
            requested = (victim_peer_id, json.dumps(recruiters))
            if current is None:
                self._db.execute(
                    "INSERT INTO plans VALUES (?, ?, ?, ?)",
                    (experiment_id, *requested, time.time()),
                )
                self._event(experiment_id, "plan_configured", victim_peer_id=victim_peer_id,
                            recruiter_peer_ids=list(recruiters))
            elif (current["victim_peer_id"], current["recruiter_peer_ids_json"]) != requested:
                raise ValueError("plan already configured with different peers")
        return self.plan(experiment_id)

    def plan(self, experiment_id: str) -> dict:
        with self._lock:
            row = self._db.execute("SELECT * FROM plans WHERE experiment_id = ?", (experiment_id,)).fetchone()
            if row is None:
                raise KeyError(experiment_id)
            return {"victim_peer_id": row["victim_peer_id"],
                    "recruiter_peer_ids": json.loads(row["recruiter_peer_ids_json"])}

    def _expire_claims(self, experiment_id: str, now: float) -> None:
        rows = self._db.execute(
            "SELECT claim_id FROM claims WHERE experiment_id = ? AND status = 'active' AND expires_at <= ?",
            (experiment_id, now),
        ).fetchall()
        for row in rows:
            self._db.execute("UPDATE claims SET status = 'expired', updated_at = ? WHERE claim_id = ?",
                             (now, row["claim_id"]))
            self._event(experiment_id, "claim_expired", claim_id=row["claim_id"])

    def claim(self, experiment_id: str, leader_peer_id: str, requester_peer_id: str, expires_at: float) -> dict:
        if expires_at <= time.time():
            raise ValueError("claim expiry must be in the future")
        with self._lock, self._db:
            plan = self.plan(experiment_id)
            if requester_peer_id != plan["victim_peer_id"] or leader_peer_id not in plan["recruiter_peer_ids"]:
                self._event(experiment_id, "claim_rejected", leader_peer_id=leader_peer_id,
                            requester_peer_id=requester_peer_id, reason="peer_not_in_plan")
                return {"accepted": False, "reason": "peer_not_in_plan"}
            now = time.time()
            self._expire_claims(experiment_id, now)
            active = self._db.execute(
                "SELECT * FROM claims WHERE experiment_id = ? AND status = 'active'", (experiment_id,)
            ).fetchone()
            if active is not None:
                accepted = active["leader_peer_id"] == leader_peer_id
                self._event(experiment_id, "claim_reused" if accepted else "claim_rejected",
                            claim_id=active["claim_id"], leader_peer_id=leader_peer_id,
                            requester_peer_id=requester_peer_id,
                            reason=None if accepted else "another_leader_has_active_claim")
                return {"accepted": accepted, "claim_id": active["claim_id"],
                        "leader_peer_id": active["leader_peer_id"], "expires_at": active["expires_at"],
                        "reason": None if accepted else "another_leader_has_active_claim"}
            claim_id = str(uuid.uuid4())
            self._db.execute(
                "INSERT INTO claims VALUES (?, ?, ?, 'active', NULL, ?, ?, ?)",
                (claim_id, experiment_id, leader_peer_id, expires_at, now, now),
            )
            self._event(experiment_id, "claim_accepted", claim_id=claim_id,
                        leader_peer_id=leader_peer_id, requester_peer_id=requester_peer_id, expires_at=expires_at)
            return {"accepted": True, "claim_id": claim_id, "leader_peer_id": leader_peer_id,
                    "expires_at": expires_at, "reason": None}

    def active_claim(self, experiment_id: str) -> dict | None:
        with self._lock, self._db:
            self._expire_claims(experiment_id, time.time())
            row = self._db.execute(
                "SELECT * FROM claims WHERE experiment_id = ? AND status = 'active'", (experiment_id,)
            ).fetchone()
            if row is None:
                return None
            return {"claim_id": row["claim_id"], "leader_peer_id": row["leader_peer_id"],
                    "expires_at": row["expires_at"], "status": row["status"]}

    def finish(self, experiment_id: str, claim_id: str, outcome: str, group_id: str | None = None,
               reason: str | None = None) -> dict:
        if outcome not in {"completed", "aborted"}:
            raise ValueError("outcome must be completed or aborted")
        with self._lock, self._db:
            row = self._db.execute("SELECT * FROM claims WHERE claim_id = ? AND experiment_id = ?",
                                   (claim_id, experiment_id)).fetchone()
            if row is None:
                raise KeyError(claim_id)
            if row["status"] == "active":
                self._db.execute("UPDATE claims SET status = ?, group_id = ?, updated_at = ? WHERE claim_id = ?",
                                 (outcome, group_id, time.time(), claim_id))
                self._event(experiment_id, f"claim_{outcome}", claim_id=claim_id, group_id=group_id, reason=reason)
            return {"claim_id": claim_id, "status": outcome if row["status"] == "active" else row["status"]}
