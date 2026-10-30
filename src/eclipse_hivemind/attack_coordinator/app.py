"""HTTP API for the isolated attacker coordination service."""

from __future__ import annotations

import os
import tempfile
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .store import CoordinationStore


class PlanRequest(BaseModel):
    victim_peer_id: str = Field(min_length=1)
    recruiter_peer_ids: list[str]


class ClaimRequest(BaseModel):
    leader_peer_id: str = Field(min_length=1)
    requester_peer_id: str = Field(min_length=1)
    expires_at: float


class FinishRequest(BaseModel):
    outcome: str
    group_id: str | None = None
    reason: str | None = None


def create_app(store: CoordinationStore | None = None) -> FastAPI:
    default_path = Path(tempfile.gettempdir()) / "eclipse-attack-coordinator.sqlite3"
    event_store = store or CoordinationStore(Path(os.environ.get("COORDINATION_DB_PATH", default_path)))

    @asynccontextmanager
    async def lifespan(_application):
        print("ATTACK_COORDINATOR_READY=1", flush=True)
        try:
            yield
        finally:
            event_store.close()

    app = FastAPI(title="Eclipse Attack Coordinator", version="1.0", lifespan=lifespan)

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.put("/api/v1/experiments/{experiment_id}/plan")
    def configure_plan(experiment_id: str, request: PlanRequest) -> dict:
        try:
            return event_store.configure_plan(experiment_id, request.victim_peer_id, request.recruiter_peer_ids)
        except ValueError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error

    @app.get("/api/v1/experiments/{experiment_id}/claim")
    def get_claim(experiment_id: str) -> dict:
        try:
            return {"claim": event_store.active_claim(experiment_id)}
        except KeyError as error:
            raise HTTPException(status_code=404, detail="unknown experiment plan") from error

    @app.post("/api/v1/experiments/{experiment_id}/claims")
    def claim(experiment_id: str, request: ClaimRequest) -> dict:
        try:
            return event_store.claim(experiment_id, request.leader_peer_id, request.requester_peer_id, request.expires_at)
        except KeyError as error:
            raise HTTPException(status_code=404, detail="unknown experiment plan") from error
        except ValueError as error:
            raise HTTPException(status_code=400, detail=str(error)) from error

    @app.post("/api/v1/experiments/{experiment_id}/claims/{claim_id}")
    def finish(experiment_id: str, claim_id: str, request: FinishRequest) -> dict:
        try:
            return event_store.finish(experiment_id, claim_id, request.outcome, request.group_id, request.reason)
        except KeyError as error:
            raise HTTPException(status_code=404, detail="unknown claim") from error
        except ValueError as error:
            raise HTTPException(status_code=400, detail=str(error)) from error

    return app


app = create_app()
