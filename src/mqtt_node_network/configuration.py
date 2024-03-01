from __future__ import annotations
from pathlib import Path
from typing import Union
import os

from dotenv import load_dotenv

from mqtt_node_network.node import MQTTBrokerConfig

load_dotenv("config/.secrets")


def load_config(filepath: Union[str, Path]) -> dict:
    if isinstance(filepath, str):
        filepath = Path(filepath)

    if not Path(filepath).exists():
        raise FileNotFoundError(f"File not found: {filepath}")

    # if extension is .json
    if filepath.suffix == ".json":
        import json

        with open(filepath, "r") as file:
            return json.load(file)

    # if extension is .yaml
    if filepath.suffix == ".yaml":
        import yaml

        with open(filepath, "r") as file:
            return yaml.safe_load(file)
    # if extension is .toml
    if filepath.suffix == ".toml":
        try:
            import tomllib
        except ImportError:
            import tomli as tomllib

        with open(filepath, "rb") as file:
            return tomllib.load(file)

    # else load as binary
    with open(filepath, "rb") as file:
        return file.read()


config = load_config("config/application.toml")

broker_config = MQTTBrokerConfig(
    hostname=config["mqtt"]["broker"].get("hostname", "localhost"),
    port=config["mqtt"]["broker"].get("port", 1883),
    keepalive=config["mqtt"]["broker"].get("keepalive", 60),
    username=os.getenv("MQTT_BROKER_USERNAME"),
    password=os.getenv("MQTT_BROKER_PASSWORD"),
)

logger_config = load_config("config/logger.yaml")
