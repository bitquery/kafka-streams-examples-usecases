#!/usr/bin/env python3
"""
Bitquery Kafka consumer (Python): solana.transactions.proto via confluent_kafka + protobuf.

Stdout: decoded protobuf only (recursive field tree). Logs go to stderr.
"""

from __future__ import annotations

import logging
import signal
import sys
import threading

from confluent_kafka import Consumer, KafkaError, KafkaException
from google.protobuf.message import DecodeError
from solana import parsed_idl_block_message_pb2

from protobuf_print import print_protobuf_message
from settings import load_settings

logger = logging.getLogger(__name__)
shutdown_event = threading.Event()


def process_payload(raw: bytes) -> None:
    block = parsed_idl_block_message_pb2.ParsedIdlBlockMessage()
    block.ParseFromString(raw)
    print_protobuf_message(block, indent=0, encoding="base58")


def _handle_signal(signum: int, _frame) -> None:
    logger.info("Received signal %s, shutting down…", signum)
    shutdown_event.set()


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stderr,
    )

    try:
        topic, conf, group_id = load_settings()
    except ValueError as e:
        print(str(e), file=sys.stderr)
        return 1

    logger.info("Subscribing to topic=%s group.id=%s", topic, group_id)

    consumer = Consumer(conf)
    consumer.subscribe([topic])

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    processed = 0
    try:
        while not shutdown_event.is_set():
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())
            try:
                value = msg.value()
                if value is None:
                    continue
                process_payload(value)
                processed += 1
            except DecodeError as err:
                logger.error("Protobuf decode error: %s", err)
            except Exception:
                logger.exception("Failed to process message")
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt")
    finally:
        logger.info("Closing consumer (processed %s messages)", processed)
        consumer.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
