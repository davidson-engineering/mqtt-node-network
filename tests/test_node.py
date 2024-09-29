def test_subscribe_to_topic():

    from mqtt_node_network.initialize import initialize_config
    from mqtt_node_network.node import MQTTNode

    SUBSCRIBE_TOPICS = ["+/metrics/#"]
    QOS = 0

    config = initialize_config(config="config/config.toml", secrets=".env")

    BROKER_CONFIG = config["mqtt"]["broker"]

    client = MQTTNode(broker_config=BROKER_CONFIG).connect()

    client.subscribe(topic=SUBSCRIBE_TOPICS, qos=QOS)
    assert client.is_connected()
    client.subscribe(topic=SUBSCRIBE_TOPICS, qos=QOS)
    client.restore_subscriptions()
    assert len(client.subscriptions) == 1

    SUBSCRIBE_STRING = SUBSCRIBE_TOPICS[0]
    client.subscribe(topic=SUBSCRIBE_STRING, qos=QOS)
    assert len(client.subscriptions) == 1

    SUBSCRIBE_TUPLE = (SUBSCRIBE_STRING, QOS)
    client.subscribe(topic=SUBSCRIBE_TUPLE)
    assert len(client.subscriptions) == 1

    SUBSCRIBE_TOPICS = [SUBSCRIBE_TOPICS[0], SUBSCRIBE_TOPICS[0]]
    client.subscribe(topic=SUBSCRIBE_TOPICS, qos=QOS)
    assert len(client.subscriptions) == 1

    SUBSCRIBE_TOPICS = ["topic1/#", "topic2/#"]
    client.subscribe(topic=SUBSCRIBE_TOPICS, qos=QOS)
    assert len(client.subscriptions) == 3

    client.client.disconnect()
    assert not client.is_connected()
