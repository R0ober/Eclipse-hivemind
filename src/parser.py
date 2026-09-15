import yaml
import argparse
from enum import Enum
from typing import Any

from pydantic import BaseModel, ValidationError, Field, StrictInt, AnyUrl, IPvAnyAddress


class ConfigError(Exception):
    """Base class for configuration problems."""


class ConfigSyntaxError(ConfigError):
    """The file was not valid YAML (layer 1)."""

class BootstrapPolicy(str, Enum):
    SEED_ONLY = "seed_only"
    FULL = "full"

class Experiment(BaseModel):
    name: str =Field(min_length=1)  
    seed: StrictInt    
    rounds: StrictInt = Field(gt=0)  

class Aggregator(BaseModel):
    endpoint: AnyUrl | IPvAnyAddress


class Bootstrap(BaseModel):
    seed_node_type: str | None = None
    seed_count: StrictInt = Field(default=1, ge=1)
    policy: BootstrapPolicy = BootstrapPolicy.SEED_ONLY


class NodeType(BaseModel):
    count: StrictInt =Field(ge=0)       
    image: str  =Field(min_length=1)     
    parameters: dict[str, Any] = Field(default_factory=dict)




def load_yaml(text: str) -> dict:
    """Parse YAML text into a dict. Raises ConfigSyntaxError on bad YAML."""
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ConfigSyntaxError(f"could not parse YAML: {exc}") from exc

    if not isinstance(data,dict):
        raise ConfigSyntaxError
    return data



def main():

    p = argparse.ArgumentParser()
    p.add_argument("--config-path",type=str,required=True,help="Path to YAML file")

    args = p.parse_args()

    with open(args.config_path, "r", encoding="utf-8") as f:
        raw = f.read()

    config_dict = load_yaml(raw)
    print(config_dict)

if __name__ == "__main__":
    main()
