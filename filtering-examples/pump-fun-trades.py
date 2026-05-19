"""Filter Solana DEX trades for a fixed program id (Pump fun example)."""

import datetime
import logging
import signal
import threading
import uuid

import base58
from confluent_kafka import Consumer, KafkaError, KafkaException
from google.protobuf.message import DecodeError
from solana import dex_block_message_pb2

import config

# Kafka consumer configuration
group_id_suffix = uuid.uuid4().hex
conf = {
    "bootstrap.servers": "rpk0.bitquery.io:9092,rpk1.bitquery.io:9092,rpk2.bitquery.io:9092",
    "group.id": f"{config.solana_username}-group-{group_id_suffix}",
    "session.timeout.ms": 45000,
    "heartbeat.interval.ms": 15000,
    "security.protocol": "SASL_PLAINTEXT",
    "ssl.endpoint.identification.algorithm": "none",
    "sasl.mechanisms": "SCRAM-SHA-512",
    "sasl.username": config.solana_username,
    "sasl.password": config.solana_password,
    "auto.offset.reset": "latest",
    "enable.auto.commit": False,
}

consumer = Consumer(conf)
topic = "solana.dextrades.proto"

TARGET_PROGRAM_ADDRESS = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

shutdown_event = threading.Event()
processed_count = 0

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def process_message(buffer):
    """Decode DexParsedBlockMessage and print matching trades."""
    try:
        tx_block = dex_block_message_pb2.DexParsedBlockMessage()
        tx_block.ParseFromString(buffer)

        timestamp = datetime.datetime.now(datetime.timezone.utc)
        target_program_bytes = base58.b58decode(TARGET_PROGRAM_ADDRESS)

        if hasattr(tx_block, "Transactions") and tx_block.Transactions:
            for tx in tx_block.Transactions:
                if hasattr(tx, "Trades") and tx.Trades:
                    for trade in tx.Trades:
                        if (
                            hasattr(trade, "Dex")
                            and trade.Dex.ProgramAddress == target_program_bytes
                        ):
                            signature_str = base58.b58encode(tx.Signature).decode()
                            print(
                                f" MATCH: Transaction Signature: {signature_str} | "
                                f"Block: {tx_block.Header.Slot} | Time: {timestamp}"
                            )
                            break

    except DecodeError as err:
        logger.error("Protobuf decoding error: %s", err)
    except Exception as err:
        logger.error("Error processing message: %s", err)


def signal_handler(_signum, _frame):
    logger.info("Shutdown requested...")
    shutdown_event.set()


def main():
    global processed_count

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    consumer.subscribe([topic])

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
                process_message(msg.value())
                processed_count += 1
                logger.debug("offset=%s count=%s", msg.offset(), processed_count)
            except Exception as err:
                logger.exception("Failed to process message: %s", err)

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    except Exception as e:
        logger.exception("Error in main polling loop: %s", e)
    finally:
        shutdown_event.set()
        consumer.close()
        logger.info("Shutdown complete. Total messages processed: %s", processed_count)


if __name__ == "__main__":
    main()
