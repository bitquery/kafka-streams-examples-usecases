# Filter BSC transactions for Uniswap-style PairCreated logs (human-readable protobuf print).

import uuid

import base58
from confluent_kafka import Consumer, KafkaError, KafkaException
from google.protobuf.descriptor import FieldDescriptor
from google.protobuf.message import DecodeError
from evm import parsed_abi_block_message_pb2

import config

group_id_suffix = uuid.uuid4().hex
print(f"Group ID Suffix: {group_id_suffix}")
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

consumer = Consumer(conf)
topic = "bsc.transactions.proto"
consumer.subscribe([topic])


def convert_bytes(value, encoding="base58"):
    if encoding == "base58":
        return base58.b58encode(value).decode()
    return "0x" + value.hex()


def print_protobuf_message(msg, indent=0, encoding="base58"):
    prefix = " " * indent
    for field in msg.DESCRIPTOR.fields:
        value = getattr(msg, field.name)

        if field.label == FieldDescriptor.LABEL_REPEATED:
            if not value:
                continue
            print(f"{prefix}{field.name} (repeated):")
            for idx, item in enumerate(value):
                if field.type == FieldDescriptor.TYPE_MESSAGE:
                    print(f"{prefix}  [{idx}]:")
                    print_protobuf_message(item, indent + 4, encoding)
                elif field.type == FieldDescriptor.TYPE_BYTES:
                    print(f"{prefix}  [{idx}]: {convert_bytes(item, encoding)}")
                else:
                    print(f"{prefix}  [{idx}]: {item}")

        elif field.type == FieldDescriptor.TYPE_MESSAGE:
            if msg.HasField(field.name):
                print(f"{prefix}{field.name}:")
                print_protobuf_message(value, indent + 4, encoding)

        elif field.type == FieldDescriptor.TYPE_BYTES:
            print(f"{prefix}{field.name}: {convert_bytes(value, encoding)}")

        elif field.containing_oneof:
            if msg.WhichOneof(field.containing_oneof.name) == field.name:
                print(f"{prefix}{field.name} (oneof): {value}")

        else:
            print(f"{prefix}{field.name}: {value}")


def process_message(message):
    try:
        buffer = message.value()
        tx_block = parsed_abi_block_message_pb2.ParsedAbiBlockMessage()
        tx_block.ParseFromString(buffer)

        for transaction in tx_block.Transactions:
            for call in transaction.Calls:
                for log in call.Logs:
                    if not log.HasField("Parsed"):
                        continue
                    if not log.Parsed.HasField("Signature"):
                        continue
                    if log.Parsed.Signature.Name != "PairCreated":
                        continue
                    print("\nNew Message received (PairCreated found):\n")
                    print_protobuf_message(transaction, encoding="hex")
                    return

    except DecodeError as err:
        print(f"Protobuf decoding error: {err}")
    except Exception as err:
        print(f"Error processing message: {err}")


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
    print("Stopping consumer...")

finally:
    consumer.close()
