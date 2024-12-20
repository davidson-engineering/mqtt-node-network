import time
import random
import threading

from mqtt_node_network.configuration import initialize_config
from mqtt_node_network.node import MQTTNode
from mqtt_node_network.metrics_node import MQTTClient


def publish_forever():
    """Publish random temperature data to the broker every PUBLISH_PERIOD seconds."""
    # Initialize the configuration
    config = initialize_config(
        config="config/node-config.toml", logging_config="config/logging.yaml"
    )

    client = MQTTNode(node_id="publisher", **config).connect()

    while True:
        payload = random.random()
        client.publish_every(
            topic=f"{client.node_id}/temperature/IR/0", payload=payload, interval=1
        )


def subscribe_forever():
    """Subscribe to the broker and print messages to the console."""
    config = initialize_config(
        config="config/client-config.toml", logging_config="config/logging.yaml"
    )
    buffer = []
    client = MQTTClient(
        name="subscriber",
        buffer=buffer,
        **config,
    ).connect()
    client.subscribe(topic=SUBSCRIBE_TOPICS, qos=QOS)

    while True:
        time.sleep(1)


def publisher_subscriber_threaded():
    """Publish and subscribe in separate threads."""
    publish_thread = threading.Thread(target=publish_forever)
    subscribe_thread = threading.Thread(target=subscribe_forever)
    publish_thread.start()
    subscribe_thread.start()
    publish_thread.join()
    subscribe_thread.join()


def create_node():
    """Publish random temperature data to the broker every PUBLISH_PERIOD seconds."""
    client = MQTTNode(
        node_id=None,
        name=None,
        broker_config=BROKER_CONFIG,
        latency_config=config["mqtt"]["client"]["metrics"]["latency"],
    ).connect()

    while True:
        time.sleep(1)


def create_node_swarm(num_nodes):
    """Publish random temperature data to the broker every PUBLISH_PERIOD seconds."""
    threads = []
    for _ in range(num_nodes):
        thread = threading.Thread(target=create_node)
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()


if __name__ == "__main__":
    # publisher_subscriber_threaded()
    create_node_swarm(10)
