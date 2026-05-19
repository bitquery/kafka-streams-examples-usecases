"""BSC: filter broadcasted transactions for Four.meme TokenCreate events on a contract."""

import logging
import signal
import sys
import uuid

from confluent_kafka import Consumer, KafkaError, KafkaException
from google.protobuf.message import DecodeError
from evm import parsed_abi_block_message_pb2

import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

group_id_suffix = uuid.uuid4().hex

conf = {
    "bootstrap.servers": "rpk0.bitquery.io:9092,rpk1.bitquery.io:9092,rpk2.bitquery.io:9092",
    "group.id": f"{config.bsc_username}-group-{group_id_suffix}",
    "session.timeout.ms": 30000,
    "security.protocol": "SASL_PLAINTEXT",
    "ssl.endpoint.identification.algorithm": "none",
    "sasl.mechanisms": "SCRAM-SHA-512",
    "sasl.username": config.bsc_username,
    "sasl.password": config.bsc_password,
    "auto.offset.reset": "latest",
    "enable.auto.commit": False,
}

topic = "bsc.broadcasted.transactions.proto"
consumer = Consumer(conf)


def process_message(msg) -> None:
    try:
        block_msg = parsed_abi_block_message_pb2.ParsedAbiBlockMessage()
        block_msg.ParseFromString(msg.value())

        target_contract = "0x5c952063c7fc8610ffdb798152d69f0b9550762b"

        for transaction in block_msg.Transactions:
            transaction_to = (
                "0x" + transaction.TransactionHeader.To.hex()
                if transaction.TransactionHeader.To
                else ""
            )
            if transaction_to.lower() != target_contract.lower():
                continue

            for call in transaction.Calls:
                for log in call.Logs:
                    if not log.HasField("Parsed"):
                        continue
                    if not log.Parsed.HasField("Signature"):
                        continue
                    if log.Parsed.Signature.Name != "TokenCreate":
                        continue

                    print(" TokenCreate Event Found!")
                    print(f"  Transaction Hash: {transaction.TransactionHeader.Hash.hex()}")
                    print(f"  Transaction To: {transaction_to}")
                    print(f"  Event Signature: {log.Parsed.Signature.Signature}")
                    print(f"  Contract Address: {log.Header.Address.hex()}")

                    if log.Arguments:
                        print("  Event Arguments:")
                        for arg in log.Arguments:
                            if arg.Value.Bytes:
                                print(f"    {arg.Name}: {arg.Value.Bytes.hex()}")
                            elif arg.Value.String:
                                print(f"    {arg.Name}: {arg.Value.String}")
                            else:
                                print(f"    {arg.Name}: {arg.Value}")
                    print("-" * 60)

    except DecodeError as e:
        logger.error("Protobuf decoding error: %s", e)
    except Exception as e:
        logger.error("Unexpected error: %s", e)


def main() -> None:
    consumer.subscribe([topic])
    logger.info("Subscribed to %s (group.id=%s)", topic, conf["group.id"])

    def _stop(_sig, _frm):
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())
            process_message(msg)
    except KeyboardInterrupt:
        logger.info("Stopping...")
    finally:
        consumer.close()


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:
        logger.exception("%s", e)
        sys.exit(1)
