"""Request and response models for the aggregator's first API version."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class EventType(StrEnum):
    NODE_STARTED = "node_started"
    RESOURCE_SAMPLE = "resource_sample"
    TRAINING_METRICS = "training_metrics"
    EVAL_METRICS = "eval_metrics"
    AVERAGING_STARTED = "averaging_started"
    AVERAGING_COMPLETED = "averaging_completed"
    NODE_FINISHED = "node_finished"
    NODE_ERROR = "node_error"


class NodeIdentity(BaseModel):
    model_config = ConfigDict(extra="forbid")

    node_id: str = Field(min_length=1)
    node_type: str = Field(min_length=1)
    node_index: int = Field(ge=0)


class ExperimentNode(NodeIdentity):

    start_group: str = Field(default="all", min_length=1)


class ExperimentRegistration(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: int = Field(default=1, strict=True)
    experiment_id: str = Field(min_length=1)
    experiment_name: str = Field(min_length=1)
    rounds: int = Field(gt=0, strict=True)
    nodes: list[ExperimentNode] = Field(min_length=1)


class Event(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(min_length=1)
    sequence: int = Field(ge=0, strict=True)
    event_type: EventType
    occurred_at: datetime
    data: dict[str, Any]


class EventBatch(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: int = Field(default=1, strict=True)
    experiment_id: str = Field(min_length=1)
    node: NodeIdentity
    events: list[Event] = Field(min_length=1)


class EventAcknowledgement(BaseModel):
    accepted: int = Field(ge=0)
    duplicates: int = Field(ge=0)
    rejected: int = Field(default=0, ge=0)
    last_sequence: int | None = Field(default=None, ge=0)


class ExperimentSummary(BaseModel):
    experiment_id: str
    event_count: int
    node_count: int


class StartBarrier(BaseModel):
    """Whether every node in one node's start group has reported node_started yet."""

    experiment_id: str
    node_id: str
    start_group: str
    ready: bool
    expected: int
    started: int
    started_nodes: list[str]
    pending_nodes: list[str]
