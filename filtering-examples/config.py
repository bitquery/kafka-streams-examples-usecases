"""
Load Kafka credentials from environment variables.

Never commit real credentials. Copy `.env.example` to `.env` locally.
The repo root `.gitignore` already ignores `.env`.
"""

from __future__ import annotations

import os
import sys

from dotenv import load_dotenv

load_dotenv()


def _required(key: str) -> str:
    value = os.environ.get(key)
    if value is None or not str(value).strip():
        print(
            f'Error: missing required environment variable "{key}".\n'
            "Copy filtering-examples/.env.example to filtering-examples/.env "
            "and set your Bitquery Kafka credentials (see README.md).",
            file=sys.stderr,
        )
        raise SystemExit(1)
    return str(value).strip()


def _pair_or_fallback(
    alt_user_key: str, alt_pass_key: str, fallback_user: str, fallback_pass: str
) -> tuple[str, str]:
    au = os.environ.get(alt_user_key)
    ap = os.environ.get(alt_pass_key)
    if au or ap:
        if not au or not ap:
            print(
                f'Error: set both "{alt_user_key}" and "{alt_pass_key}", or neither.',
                file=sys.stderr,
            )
            raise SystemExit(1)
        return au.strip(), ap.strip()
    return fallback_user, fallback_pass


_u = _required("KAFKA_USERNAME")
_p = _required("KAFKA_PASSWORD")

solana_username, solana_password = _pair_or_fallback(
    "KAFKA_USERNAME_SOLANA", "KAFKA_PASSWORD_SOLANA", _u, _p
)
bsc_username, bsc_password = _pair_or_fallback(
    "KAFKA_USERNAME_BSC", "KAFKA_PASSWORD_BSC", _u, _p
)
