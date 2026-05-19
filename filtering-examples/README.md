# Filtering examples (Python + Kafka protobuf)

Small **Python** scripts that consume **Bitquery Kafka** topics, decode **Protocol Buffers** with `bitquery-pb2-kafka-package`, and **filter** for specific programs, events, or instruction patterns. They are **not** production pipelines—only illustrations you can copy from.

| Script                                                     | Chain / topic (default)                  | What it does                                                                                               |
| ---------------------------------------------------------- | ---------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| <a href="./pump-fun-trades.py" target="_blank" rel="noopener noreferrer"><code>pump-fun-trades.py</code></a>               | Solana `solana.dextrades.proto`          | Prints txs where a DEX trade’s program id matches Pump.fun’s program.                                      |
| <a href="./pure-transfers.py" target="_blank" rel="noopener noreferrer"><code>pure-transfers.py</code></a>                 | Solana `solana.transactions.proto`       | Classifies parsed-idl transactions (swap, multi-transfer, vote, burn, HumidiFi, etc.) and logs signatures. |
| <a href="./four-meme-token-create.py" target="_blank" rel="noopener noreferrer"><code>four-meme-token-create.py</code></a> | BSC `bsc.broadcasted.transactions.proto` | Watches a fixed contract for `TokenCreate` logs (Four.meme–style factory).                                 |
| <a href="./bsc-pairCreated.py" target="_blank" rel="noopener noreferrer"><code>bsc-pairCreated.py</code></a>               | BSC `bsc.transactions.proto`             | Prints full protobuf for transactions that emit `PairCreated`.                                             |

## Prerequisites

- **Python 3.10+** (tested with the dependency stack below).
- **Bitquery Kafka username and password** for the streams you read. These are **subscription / stream credentials**, not IDE API keys. Request them from **Bitquery support / sales** via the **<a href="https://bitquery.io/forms/api" target="_blank" rel="noopener noreferrer">API request form</a>** (same as elsewhere in this repo).  
  Use a local **`.env`** file (never commit real values). This repository’s root <a href="../.gitignore" target="_blank" rel="noopener noreferrer"><code>.gitignore</code></a> ignores `.env`.
- Your subscription must **include the topic** each script uses (see table above).

## Setup (self-contained in this folder)

Run all commands **from this directory** so `import config` resolves and relative paths stay obvious.

```bash
cd filtering-examples

python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate

pip install -r requirements.txt
cp .env.example .env
# Edit .env: set KAFKA_USERNAME and KAFKA_PASSWORD (see Prerequisites — obtain via Bitquery form)
```

**Credentials:** **`KAFKA_USERNAME`** / **`KAFKA_PASSWORD`** are **stream credentials**, not IDE keys. Get them from Bitquery support / sales using the **<a href="https://bitquery.io/forms/api" target="_blank" rel="noopener noreferrer">API request form</a>**.

### Environment variables

| Variable                                          | Required | Purpose                                                                 |
| ------------------------------------------------- | -------- | ----------------------------------------------------------------------- |
| `KAFKA_USERNAME`                                  | Yes      | Kafka login from Bitquery (request via <a href="https://bitquery.io/forms/api" target="_blank" rel="noopener noreferrer">form</a>; not an IDE API key). |
| `KAFKA_PASSWORD`                                  | Yes      | Matching password for that Kafka user.                                 |
| `KAFKA_USERNAME_SOLANA` / `KAFKA_PASSWORD_SOLANA` | No       | Override for Solana-only scripts (`pump-fun-trades`, `pure-transfers`). |
| `KAFKA_USERNAME_BSC` / `KAFKA_PASSWORD_BSC`       | No       | Override for BSC scripts (`four-meme-token-create`, `bsc-pairCreated`). |

If you omit the `_SOLANA` / `_BSC` pairs, the generic `KAFKA_USERNAME` / `KAFKA_PASSWORD` are used for every script (typical when one Kafka user has access to both chains).

## Run an example

```bash
# Solana — DEX trades filter
python pump-fun-trades.py

# Solana — parsed transaction heuristics
python pure-transfers.py

# BSC — TokenCreate on a fixed factory (edit contract in the file if needed)
python four-meme-token-create.py

# BSC — PairCreated (verbose protobuf dump to stdout)
python bsc-pairCreated.py
```

Stop with **Ctrl+C**.

## Security & this public repo

- **Do not commit** `.env` or any file with real passwords. The template is <a href="./.env.example" target="_blank" rel="noopener noreferrer"><code>.env.example</code></a> only.
- **Kafka credentials** are issued for **streams**, not the IDE — use the **<a href="https://bitquery.io/forms/api" target="_blank" rel="noopener noreferrer">API request form</a>** (or Bitquery support); never paste live passwords into issues or docs.
- A previous version of one script contained **personal infra notes** in comments; those have been removed.
- If you fork or paste these scripts, rotate credentials that were ever exposed.

## Dependencies

Declared in <a href="./requirements.txt" target="_blank" rel="noopener noreferrer"><code>requirements.txt</code></a>: `confluent-kafka`, `bitquery-pb2-kafka-package`, `protobuf`, `base58`, `python-dotenv`. Same family as <a href="../python-consumer-example/" target="_blank" rel="noopener noreferrer"><code>python-consumer-example</code></a>.

## Protobuf layouts

Authoritative `.proto` sources: <a href="https://github.com/bitquery/streaming_protobuf" target="_blank" rel="noopener noreferrer">bitquery/streaming_protobuf</a>. Background on brokers, SASL, and topics: <a href="https://docs.bitquery.io/docs/streams/kafka-streaming-concepts/" target="_blank" rel="noopener noreferrer">Kafka streaming concepts</a>.

## TLS (optional)

These samples use **`SASL_PLAINTEXT`** on port **9092** by default. For **`SASL_SSL`** on **9093**, follow Bitquery’s docs and extend the `conf` dict in each script (see the main repo <a href="../README.md" target="_blank" rel="noopener noreferrer"><code>README.md</code></a> and <a href="../python-consumer-example/" target="_blank" rel="noopener noreferrer"><code>python-consumer-example</code></a>).
