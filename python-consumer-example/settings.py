"""Load Kafka settings from environment (see .env.example)."""

from __future__ import annotations

import os
import uuid

from dotenv import load_dotenv


def _required(name: str) -> str:
    value = os.environ.get(name)
    if value is None or str(value).strip() == "":
        raise ValueError(
            f'Missing required environment variable "{name}". '
            "Copy .env.example to .env and set it."
        )
    return str(value).strip()


def _optional(name: str, default: str) -> str:
    value = os.environ.get(name)
    if value is None or str(value).strip() == "":
        return default
    return str(value).strip()


def load_settings() -> tuple[str, dict, str]:
    """
    Returns (topic, kafka_conf_dict, group_id_for_logging).

    group_id is embedded in kafka_conf_dict under 'group.id'.
    """
    load_dotenv()

    username = _required("KAFKA_USERNAME")
    password = _required("KAFKA_PASSWORD")

    topic = _optional("KAFKA_TOPIC", "solana.transactions.proto")
    bootstrap = _optional(
        "KAFKA_BOOTSTRAP_SERVERS",
        "rpk0.bitquery.io:9092,rpk1.bitquery.io:9092,rpk2.bitquery.io:9092",
    )
    auto_offset = _optional("KAFKA_AUTO_OFFSET_RESET", "latest")
    if auto_offset not in ("latest", "earliest"):
        raise ValueError(
            'KAFKA_AUTO_OFFSET_RESET must be "latest" or "earliest".'
        )

    explicit_group = os.environ.get("KAFKA_GROUP_ID")
    if explicit_group and str(explicit_group).strip():
        group_id = str(explicit_group).strip()
    else:
        group_id = f"{username}-group-{uuid.uuid4().hex}"

    conf = {
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "session.timeout.ms": 30_000,
        "security.protocol": "SASL_PLAINTEXT",
        "ssl.endpoint.identification.algorithm": "none",
        "sasl.mechanisms": "SCRAM-SHA-512",
        "sasl.username": username,
        "sasl.password": password,
        "auto.offset.reset": auto_offset,
        "enable.auto.commit": False,
    }

    return topic, conf, group_id
