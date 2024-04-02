config_defaults = {
    "mqtt": {
        "broker": {
            "hostname": "localhost",
            "port": 1883,
            "keepalive": 60,
        },
        "node_network": {
            "enable_prometheus_server": False,
            "prometheus_port": 8000,
        },
    },
    "secrets_filepath": ".env",
}
