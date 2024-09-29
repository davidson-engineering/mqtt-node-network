import time
import random
import signal
import sys
from mqtt_node_network.initialize import initialize_config
from mqtt_node_network.node import MQTTNode
from mqtt_node_network.client import MQTTClient

# Initialize the configuration
config = initialize_config(
    config="config/config.toml", logging_config="config/logging.yaml"
)

BROKER_CONFIG = config["broker"]
QOS = config["client"]["subscribe_qos"]
PUBLISH_PERIOD = config["client"]["publish_period"]
publish_topic = config["client"]["publish_topic"]
subscribe_topics = config["client"]["subscribe_topics"]

# Flag for stopping the infinite loop
running = True


def signal_handler(sig, frame):
    """Handle termination signals for graceful shutdown."""
    global running
    print("Shutting down...")
    running = False


def publish_forever():
    """Publish random temperature data to the broker every PUBLISH_PERIOD seconds."""
    client = MQTTNode(broker_config=BROKER_CONFIG).connect()

    while running:
        payload = random.random()
        client.publish(
            topic=f"{client.node_id}/{publish_topic}/temperature/IR/0", payload=payload
        )
        time.sleep(PUBLISH_PERIOD)


def subscribe_forever():
    """Subscribe to the broker and print messages to the console."""
    buffer = []
    client = MQTTClient(
        broker_config=BROKER_CONFIG,
        buffer=buffer,
        topic_structure=config["node_network"]["topic_structure"],
    ).connect()
    client.subscribe(topic=subscribe_topics, qos=QOS)

    while running:
        time.sleep(1)


if __name__ == "__main__":
    # Handle Ctrl+C to stop the infinite loop
    signal.signal(signal.SIGINT, signal_handler)

    mode = "publish"  # Switch between 'publish' or 'subscribe'

    if mode == "publish":
        publish_forever()
    elif mode == "subscribe":
        subscribe_forever()

    print("Exiting.")
