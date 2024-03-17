from typing import Union
from mqtt_node_network.configure import (
    start_prometheus_server,
    setup_logging,
    build_config,
)
import logging


def initialize(
    config: Union[str, dict] = None,
    secrets: Union[str, dict] = None,
    logger: Union[str, dict] = None,
) -> tuple[dict, logging.Logger]:
    from mqtt_node_network.configure import MQTTBrokerConfig
    from mqtt_node_network.configure import load_secrets

    config = build_config(config)
    secrets = secrets or config.get("secrets_filepath")
    secrets = load_secrets(secrets, ["MQTT_BROKER_USERNAME", "MQTT_BROKER_PASSWORD"])
    logger = setup_logging(logger)

    PROMETHEUS_ENABLE = config["mqtt"]["node_network"].get("enable_prometheus_server")
    PROMETHEUS_PORT = config["mqtt"]["node_network"].get("prometheus_port")

    if PROMETHEUS_ENABLE:
        start_prometheus_server(PROMETHEUS_PORT)

    config["mqtt"]["broker"] = MQTTBrokerConfig(
        username=secrets.get("MQTT_BROKER_USERNAME"),
        password=secrets.get("MQTT_BROKER_PASSWORD"),
        hostname=config["mqtt"]["broker"].get("hostname", "localhost"),
        port=config["mqtt"]["broker"].get("port", 1883),
        keepalive=config["mqtt"]["broker"].get("keepalive", 60),
        timeout=config["mqtt"]["broker"].get("timeout", 5),
        reconnect_attempts=config["mqtt"]["broker"].get("reconnect_attempts", 10),
    )

    return config, logger
