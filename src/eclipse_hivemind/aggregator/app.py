"""FastAPI application for the first aggregator API slice."""

from __future__ import annotations

import os

from fastapi import FastAPI, HTTPException, status

from .models import (
    EventAcknowledgement,
    EventBatch,
    ExperimentRegistration,
    ExperimentSummary,
)
from .store import (
    EventConflict,
    ExperimentAlreadyExists,
    InMemoryEventStore,
    UnknownExperiment,
    UnknownNode,
)


def create_app(
    store: InMemoryEventStore | None = None,
    initial_registration: ExperimentRegistration | None = None,
) -> FastAPI:
    event_store = store or InMemoryEventStore()
    if initial_registration is not None:
        event_store.register(initial_registration)
    application = FastAPI(title="Eclipse Hivemind Aggregator", version="1.0")

    @application.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @application.post("/api/v1/experiments/{experiment_id}/events",response_model=EventAcknowledgement,status_code=status.HTTP_202_ACCEPTED)
    def submit_events(experiment_id: str, batch: EventBatch) -> EventAcknowledgement:
        if batch.experiment_id != experiment_id:
            raise HTTPException(status_code=400, detail="experiment ID does not match URL")
        try:
            accepted, duplicates, last_sequence = event_store.submit(
                experiment_id,
                batch.node.node_id,
                batch.node.node_type,
                batch.node.node_index,
                batch.events,
            )
        except UnknownExperiment as error:
            raise HTTPException(status_code=404, detail="unknown experiment") from error
        except UnknownNode as error:
            raise HTTPException(status_code=404, detail="unknown node") from error
        except EventConflict as error:
            raise HTTPException(status_code=409, detail="event ID conflict") from error
        return EventAcknowledgement(
            accepted=accepted,
            duplicates=duplicates,
            last_sequence=last_sequence,
        )

    @application.get("/api/v1/experiments/{experiment_id}/summary", response_model=ExperimentSummary)
    def get_summary(experiment_id: str) -> ExperimentSummary:
        try:
            event_count, node_count = event_store.summary(experiment_id)
        except UnknownExperiment as error:
            raise HTTPException(status_code=404, detail="unknown experiment") from error
        return ExperimentSummary(
            experiment_id=experiment_id,
            event_count=event_count,
            node_count=node_count,
        )

    return application


def _registration_from_environment() -> ExperimentRegistration | None:
    manifest = os.environ.get("EXPERIMENT_MANIFEST")
    if not manifest:
        return None
    return ExperimentRegistration.model_validate_json(manifest)


app = create_app(initial_registration=_registration_from_environment())
