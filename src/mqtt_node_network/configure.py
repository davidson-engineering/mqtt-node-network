from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Union
import os
from logging.config import dictConfig

from dotenv import load_dotenv

FILEPATH_CONFIG_DEFAULT = "config/config.toml"
PORT_PROMETHEUS_DEFAULT = 8000
FILEPATH_SECRETS_DEFAULT = ".env"
FILEPATH_LOGGING_CONFIG_DEFAULT = "config/logger.yaml"


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


def start_prometheus_server(port=None) -> None:
    from prometheus_client import start_http_server

    port = port or PORT_PROMETHEUS_DEFAULT

    start_http_server(port)


def merge_dicts_recursive(dict1, dict2):
    """
    Recursively merge two dictionaries. dict2 takes precedence over dict1.

    Args:
    - dict1: The first dictionary.
    - dict2: The second dictionary.

    Returns:
    - Merged dictionary.
    """
    merged_dict = dict1.copy()

    for key, value in dict2.items():
        if (
            key in merged_dict
            and isinstance(merged_dict[key], dict)
            and isinstance(value, dict)
        ):
            merged_dict[key] = merge_dicts_recursive(merged_dict[key], value)
        else:
            merged_dict[key] = value

    return merged_dict


def build_config(config: Union[str, dict] = None) -> dict:
    # Define default configuration
    from mqtt_node_network.config_default import config_defaults

    config = config or FILEPATH_CONFIG_DEFAULT

    if isinstance(config, dict):
        config_local = config
    else:
        config_local = load_config(config)
    # Merge the two configurations, with the local configuration taking precedence
    return merge_dicts_recursive(config_defaults, config_local)


def setup_logging(logger_config: Union[str, dict] = None):
    logger_config = logger_config or FILEPATH_LOGGING_CONFIG_DEFAULT
    # if logger_config is a dictionary, use it as is
    if isinstance(logger_config, dict):
        return dictConfig(logger_config)
    # else if logger_config is a string, load it as a file
    logger_config = load_config(logger_config)
    # Create logs directory if it doesn't exist
    Path.mkdir(Path("logs"), exist_ok=True)
    dictConfig(logger_config)


@dataclass
class MQTTBrokerConfig:
    username: str
    password: str
    keepalive: int
    hostname: str
    port: int
    timeout: int
    reconnect_attempts: int


@dataclass
class LatencyMonitoringConfig:
    enabled: bool = False  # Whether to enable latency monitoring
    request_topic: str = "request"
    response_topic: str = "response"
    qos: int = 1  # MQTT Quality of Service level for latency monitoring
    interval: int = 10  # How often to check latency (in seconds)
    log_enabled: bool = False


def load_secrets(filepath: Union[str, Path] = None, secrets: list[str] = None) -> dict:
    filepath = filepath or FILEPATH_SECRETS_DEFAULT
    if isinstance(filepath, str):
        filepath = Path(filepath)

    if not Path(filepath).exists():
        raise FileNotFoundError(f"File not found: {filepath}")

    load_dotenv(filepath)

    if secrets is None:
        secrets = []

    for secret in secrets:
        if secret not in os.environ:
            raise KeyError(f"Secret not found: {secret}")

    secrets = {secret: os.getenv(secret) for secret in secrets}

    return secrets
