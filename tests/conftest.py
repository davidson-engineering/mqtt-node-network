import time
import pytest


@pytest.fixture(scope="session")
def mqtt_client():
    from mqtt_node_network.initialize import initialize_config
    from mqtt_node_network.node import MQTTNode

    config = initialize_config(
        config="tests/config-test.toml", secrets="tests/test.env"
    )["mqtt"]

    BROKER_CONFIG = config["broker"]

    client = MQTTNode(broker_config=BROKER_CONFIG).connect()

    timeout = 10

    while not client.is_connected() or timeout < 0:
        time.sleep(1)
        timeout -= 1
    if timeout < 0:
        raise TimeoutError("Client connection timeout")
    yield client
