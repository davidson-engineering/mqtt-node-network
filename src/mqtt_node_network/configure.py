from __future__ import annotations
from pathlib import Path
from typing import Union
import os
from logging.config import dictConfig

from dotenv import load_dotenv

from mqtt_node_network.node import MQTTBrokerConfig

CONFIG_FILEPATH = "config/config.toml"


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


def start_prometheus_server(port=8000):
    from prometheus_client import start_http_server

    start_http_server(port)


def build_config(filepath: str = CONFIG_FILEPATH) -> dict:
    # Define default configuration
    from mqtt_node_network.config_default import config_defaults

    config_local = load_config(filepath)
    # Merge the two configurations, with the local configuration taking precedence
    return config_defaults | config_local


config = build_config()

load_dotenv(config["secrets_filepath"])

PROMETHEUS_ENABLE = config["mqtt"]["node_network"]["enable_prometheus_server"]
PROMETHEUS_PORT = config["mqtt"]["node_network"]["prometheus_port"]

if PROMETHEUS_ENABLE:
    start_prometheus_server(PROMETHEUS_PORT)

broker_config = MQTTBrokerConfig(
    hostname=config["mqtt"]["broker"]["hostname"],
    port=config["mqtt"]["broker"]["port"],
    keepalive=config["mqtt"]["broker"]["keepalive"],
    username=os.getenv("MQTT_BROKER_USERNAME"),
    password=os.getenv("MQTT_BROKER_PASSWORD"),
)

logger_config = load_config("config/logger.yaml")


def setup_logging(logger_config):
    from pathlib import Path

    Path.mkdir(Path("logs"), exist_ok=True)
    return dictConfig(logger_config)


setup_logging(logger_config)
