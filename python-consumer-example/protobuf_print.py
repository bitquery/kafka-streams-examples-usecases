"""Recursive protobuf pretty-print (descriptor-based), stdout only for payload."""

from __future__ import annotations

import base58
from google.protobuf.descriptor import FieldDescriptor


def _field_is_repeated(field) -> bool:
    """
    Repeated-field check compatible with protobuf's UPB/c++ backend descriptors.

    Newer protobuf versions remove `.label` on `google._upb._message.FieldDescriptor`
    and expect `field.is_repeated()` instead — see protobuf release notes /
    Descriptor API changes ([protobuf.dev/news](https://protobuf.dev/news/2025-07-14/)).

    Older code like Bitquery's `streaming-protobuf-python` snippet still uses `.label`,
    which breaks after upgrading protobuf.
    """
    is_rep_fn = getattr(field, "is_repeated", None)
    if callable(is_rep_fn):
        return bool(is_rep_fn())
    label = getattr(field, "label", None)
    if label is not None:
        return label == FieldDescriptor.LABEL_REPEATED
    return False


def convert_bytes(value: bytes, encoding: str = "base58") -> str:
    if encoding == "base58":
        return base58.b58encode(value).decode("ascii")
    return value.hex()


def print_protobuf_message(msg, indent: int = 0, encoding: str = "base58") -> None:
    """Walk protobuf message fields by descriptor; skips empty repeated fields."""
    prefix = " " * indent

    for field in msg.DESCRIPTOR.fields:
        if field.containing_oneof is not None:
            if msg.WhichOneof(field.containing_oneof.name) != field.name:
                continue

        value = getattr(msg, field.name)
        tag = f"{field.name} (oneof)" if field.containing_oneof is not None else field.name

        if _field_is_repeated(field):
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
            if not msg.HasField(field.name):
                continue
            print(f"{prefix}{tag}:")
            print_protobuf_message(value, indent + 4, encoding)

        elif field.type == FieldDescriptor.TYPE_BYTES:
            print(f"{prefix}{tag}: {convert_bytes(value, encoding)}")

        else:
            print(f"{prefix}{tag}: {value}")
