def test_subscribe_to_topic(mqtt_client):

    SUBSCRIBE_TOPICS = ["+/#"]
    QOS = 0

    mqtt_client.subscribe(topic=SUBSCRIBE_TOPICS, qos=QOS)
    assert mqtt_client.is_connected()
    mqtt_client.subscribe(topic=SUBSCRIBE_TOPICS, qos=QOS)
    mqtt_client.restore_subscriptions()
    assert len(mqtt_client.subscriptions) == 1

    SUBSCRIBE_STRING = SUBSCRIBE_TOPICS[0]
    mqtt_client.subscribe(topic=SUBSCRIBE_STRING, qos=QOS)
    assert len(mqtt_client.subscriptions) == 1

    SUBSCRIBE_TUPLE = (SUBSCRIBE_STRING, QOS)
    mqtt_client.subscribe(topic=SUBSCRIBE_TUPLE)
    assert len(mqtt_client.subscriptions) == 1

    SUBSCRIBE_TOPICS = [SUBSCRIBE_TOPICS[0], SUBSCRIBE_TOPICS[0]]
    mqtt_client.subscribe(topic=SUBSCRIBE_TOPICS, qos=QOS)
    assert len(mqtt_client.subscriptions) == 1

    SUBSCRIBE_TOPICS = ["topic1/#", "topic2/#"]
    mqtt_client.subscribe(topic=SUBSCRIBE_TOPICS, qos=QOS)
    assert len(mqtt_client.subscriptions) == 3

    mqtt_client.client.disconnect()
    assert not mqtt_client.is_connected()
