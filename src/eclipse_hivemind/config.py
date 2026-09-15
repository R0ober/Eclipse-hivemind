import yaml
from enum import Enum
from pathlib import Path
from typing import Any
from typing import Literal

from pydantic import BaseModel, ValidationError, Field, StrictInt, AnyUrl, IPvAnyAddress, ConfigDict, model_validator

SUPPORTED_SCHEMA_VERSIONS = (1,)


class ConfigError(Exception):
    """Base class for configuration problems."""


class ConfigSyntaxError(ConfigError):
    """The file was not valid YAML (layer 1)."""

class ConfigValidationError(ConfigError):
    """The configuration violated schema or semantic rules (layers 2 and 3)."""

class BootstrapPolicy(str, Enum):
    SEED_ONLY = "seed_only"
    FULL = "full"

class Experiment(BaseModel):
    name: str =Field(min_length=1)  
    seed: StrictInt    
    rounds: StrictInt = Field(gt=0)  

class Aggregator(BaseModel):
    endpoint: AnyUrl | IPvAnyAddress


class BootstrapSeeds(BaseModel):
    node_type: str | None = None
    count: StrictInt = Field(default=1, ge=1)


class Bootstrap(BaseModel):
    seeds: BootstrapSeeds = Field(default_factory=BootstrapSeeds)
    policy: BootstrapPolicy = BootstrapPolicy.SEED_ONLY


class NodeType(BaseModel):
    count: StrictInt =Field(ge=0)       
    image: str  =Field(min_length=1)     
    parameters: dict[str, Any] = Field(default_factory=dict)


class ExperimentConfig(BaseModel):
    """Top-level resolved config. This is the public interface."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[*SUPPORTED_SCHEMA_VERSIONS]  

    experiment: Experiment
    aggregator: Aggregator
    node_types: dict[str, NodeType]
    bootstrap: Bootstrap = Field(default_factory=Bootstrap)  

# Runs before validation of fields above  
    @model_validator(mode="before")
    @classmethod
    def _default_seed_node_type(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data

        node_types = data.get("node_types")
        if not isinstance(node_types, dict):
            return data

        bootstrap = data.get("bootstrap")
        if bootstrap is None:
            bootstrap = {}
            data["bootstrap"] = bootstrap

        seeds = bootstrap.get("seeds")
        if seeds is None:
            seeds = {}
            bootstrap["seeds"] = seeds

        #  ADR 0002: If seed node_type is missing and exactly 1 node type exists, default to it
        if not seeds.get("node_type") and len(node_types) == 1:
            seeds["node_type"] = next(iter(node_types.keys()))

        return data

# Runs after validation 
    @model_validator(mode="after")
    def _check_semantics(self) -> "ExperimentConfig":
        if self.total_nodes() == 0:
            raise ValueError("at least one node type must have count > 0")

        seed_node_type = self.bootstrap.seeds.node_type
        if seed_node_type not in self.node_types:
            raise ValueError(
                f"bootstrap seed node_type '{seed_node_type}' not found in node_types"
            )
        seed_count = self.bootstrap.seeds.count
        available_count = self.node_types[seed_node_type].count
        if available_count < seed_count:
            raise ValueError(
                f"bootstrap seed count ({seed_count}) exceeds available nodes "
                f"for node_type '{seed_node_type}' ({available_count})"
            )

        return self
    
    def total_nodes(self) -> int:
        return sum(nt.count for nt in self.node_types.values())

    def node_types_with_nodes(self) -> dict[str, NodeType]:
        """Returns only node types that have a count > 0."""
        return {name: nt for name, nt in self.node_types.items() if nt.count > 0}

def parse_config(text: str) -> ExperimentConfig:
    """Full pipeline: text -> validated ExperimentConfig, or raises ConfigError."""
    raw = load_yaml(text)  # layer 1
    try: 
        return ExperimentConfig.model_validate(raw)
    except ValidationError as e:
        raise ConfigValidationError(f"Invalid configuration:\n{e}") from e


def parse_config_file(path: str | Path) -> ExperimentConfig:
    """Load and validate a YAML configuration file."""
    try:
        text = Path(path).read_text(encoding="utf-8")
    except OSError as e:
        raise ConfigError(f"Could not read configuration file '{path}': {e}") from e
    return parse_config(text)

def load_yaml(text: str) -> dict:
    """Parse YAML text into a dict. Raises ConfigSyntaxError on bad YAML."""
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ConfigSyntaxError(f"could not parse YAML: {exc}") from exc

    if not isinstance(data,dict):
        raise ConfigSyntaxError
    return data
