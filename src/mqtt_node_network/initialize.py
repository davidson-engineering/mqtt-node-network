from logging.config import dictConfig
from pathlib import Path
from typing import Union
from mqtt_node_network import (
    MQTTBrokerConfig,
)
import logging
from config_loader import load_configs


def start_prometheus_server(port=8000) -> None:
    from prometheus_client import start_http_server

    start_http_server(port)


def get_nested_value(config, target_key):
    # Base case: if the target key is in the current dictionary, return its value
    if target_key in config:
        return config[target_key]

    # Recursive case: check for the target key in any nested dictionaries
    for key, value in config.items():
        if isinstance(value, dict):
            result = get_nested_value(value, target_key)
            if result is not None:
                return result

    # Return None if the target key is not found
    return None


def initialize_config(
    config: str = None,
    secrets: str = None,
    logging_config: Union[dict, str] = None,
) -> tuple[dict, logging.Logger]:
    """
    Initialize the configuration and logger.

    Args:
    - config: The configuration file path or list of configuration file paths.
    - secrets: The secrets file path. Will default to ".env" if not provided.

    """

    config_filename = config
    config = load_configs(config, secrets_filepath=secrets)

    # If a logger configuration is provided, set up logging
    if logging_config:
        if isinstance(logging_config, str):
            logging_config = load_configs(logging_config)
        Path.mkdir(Path("logs"), exist_ok=True)
        dictConfig(logging_config)

    mqtt_config = get_nested_value(config, "mqtt")

    PROMETHEUS_ENABLE = mqtt_config["node_network"].get("enable_prometheus_server")
    PROMETHEUS_PORT = mqtt_config["node_network"].get("prometheus_port")

    if PROMETHEUS_ENABLE:
        start_prometheus_server(PROMETHEUS_PORT)

    mqtt_config["broker"] = MQTTBrokerConfig(
        username=mqtt_config["broker"].get("username", "mqtt"),
        password=mqtt_config["broker"].get("password", ""),
        hostname=mqtt_config["broker"].get("hostname", "localhost"),
        port=mqtt_config["broker"].get("port", 1883),
        keepalive=mqtt_config["broker"].get("keepalive", 60),
        timeout=mqtt_config["broker"].get("timeout", 5),
        reconnect_attempts=mqtt_config["broker"].get("reconnect_attempts", 10),
    )
    return mqtt_config
