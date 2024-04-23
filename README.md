# mqtt-node-network
### A node and client to help implement an mqqt network

```python
import threading

SUBSCRIBER_NODE_ID = "node_subscriber_0"
PUBLISHER_NODE_ID = "node_publisher_0"
GATHER_PERIOD = 1
SUBSCRIBE_TOPICS = ["+/metrics"]
PUBLISH_TOPIC = f"{PUBLISHER_NODE_ID}/metrics"
QOS = 0

BROKER_CONFIG = MQTTBrokerConfig(
    hostname="localhost,
    port=1883,
    keepalive=60,
    username="user",
    password="super_secret_password",
)

def publish_forever():
    import json
    import time
    import random

    from mqtt_node_network.node import MQTTNode

    node = MQTTNode(broker_config=BROKER_CONFIG, node_id=PUBLISHER_NODE_ID).connect()

    while True:
        data = {
            "measurement": "test_measure",
            "fields": {"random_data": random.random()},
            "time": time.time(),
        }
        payload = json.dumps(data)
        node.publish(topic=PUBLISH_TOPIC, payload=payload)
        time.sleep(GATHER_PERIOD)

def subscribe_forever():
    from mqtt_node_network.client import MQTTClient

    buffer = []
    client = MQTTClient(
        broker_config=BROKER_CONFIG, node_id=SUBSCRIBER_NODE_ID, buffer=buffer
    ).connect()
    client.subscribe(topics=SUBSCRIBE_TOPICS, qos=QOS)
    client.loop_forever()


threading.Thread(target=publish_forever).start()
threading.Thread(target=subscribe_forever).start()

```
