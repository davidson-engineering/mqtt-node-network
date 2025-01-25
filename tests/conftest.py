import time
import pytest

from mqtt_node_network.configuration import MQTTBrokerConfig


@pytest.fixture(scope="session")
def mqtt_test_client():
    from mqtt_node_network.node import MQTTNode

    broker_config = MQTTBrokerConfig(
        username="rw",
        password="readwrite",
        keepalive=60,
        hostname="test.mosquitto.org",
        port=1884,
        timeout=1,
        reconnect_attempts=5,
        clean_session=1,
    )

    client = MQTTNode.from_config_file(
        config_file="tests/config-test.toml",
        secrets_file="tests/test.env",
        broker_config=broker_config,
        node_id="",
        subscribe_config=None,
    )
    client.connect()

    yield client
