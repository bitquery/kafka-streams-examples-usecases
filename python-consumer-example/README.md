# Python — Bitquery Kafka consumer (`solana.transactions.proto`)

Minimal consumer using **`confluent_kafka`**, Bitquery’s **`bitquery-pb2-kafka-package`** (Solana **`ParsedIdlBlockMessage`**), **`base58`**, and **`.env`** for credentials.

**Official tutorial:** <a href="https://docs.bitquery.io/docs/streams/protobuf/kafka-protobuf-python/" target="_blank" rel="noopener noreferrer">Python — Solana shreds / protobuf from Kafka</a>

**Parent hub:** <a href="https://docs.bitquery.io/docs/category/kafka-streams/" target="_blank" rel="noopener noreferrer">Kafka streams documentation</a>

---

## Behaviour

| Stream | Content |
|--------|---------|
| **Stdout** | Decoded protobuf only — recursive field tree (descriptor order). **`bytes`** → **base58**. |
| **Stderr** | **`logging`** — subscribe notice, decode errors, graceful shutdown. |

Default topic: **`solana.transactions.proto`** (`KAFKA_TOPIC` overrides). Kafka metadata is **not** printed on stdout so output stays pipe-friendly.

---

## Requirements

- **Python 3.10+** (3.9 may work; prefer an active **3.x LTS**).
- Network access to Bitquery Kafka brokers.
- **Username and password** for **Kafka streams** (not IDE API keys). Request from Bitquery support / sales via the **<a href="https://bitquery.io/forms/api" target="_blank" rel="noopener noreferrer">API request form</a>**.

---

## Quick start

From the repository root:

```bash
cd python-consumer-example
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
# Edit .env: set KAFKA_USERNAME and KAFKA_PASSWORD (obtain stream credentials via https://bitquery.io/forms/api — not IDE keys)
python consumer.py
```

Without activating the venv: **`.venv/bin/python consumer.py`**

---

## Encryption in transit (TLS / SSL)

**This sample defaults to non-TLS Kafka:** **`security.protocol=SASL_PLAINTEXT`** + **SCRAM-SHA-512** on **port 9092**. The connection is **authenticated** (SASL) but the **Kafka wire is not wrapped in TLS**, so **no TLS version** (1.2 vs 1.3) is negotiated on that socket.

**Official TLS reference:** Bitquery documents **`SASL_SSL`** on **`9093`** with PEM paths in **<a href="https://docs.bitquery.io/docs/streams/kafka-streaming-concepts/#ssl-connection-sasl_ssl-" target="_blank" rel="noopener noreferrer">Kafka streams concepts — SSL connection (SASL_SSL)</a>**.

**Certificates:** Download **`server.cer.pem`**, **`client.cer.pem`**, and **`client.key.pem`** from Bitquery’s reference repo **<a href="https://github.com/bitquery/kafka-consumer-example" target="_blank" rel="noopener noreferrer">`bitquery/kafka-consumer-example`</a>** (repo root). Overview and **`curl`** one-liners: **<a href="../README.md#optional-tls-sasl_ssl-port-9093" target="_blank" rel="noopener noreferrer">root `README.md` — Optional: TLS</a>**.

**To switch this consumer to TLS**, extend **`settings.py`** so the **`conf`** dict matches Bitquery’s snippet (bootstrap **`9093`**, **`security.protocol=SASL_SSL`**, SCRAM credentials unchanged). Point **`ssl.*.location`** at wherever you saved those files (example below assumes they sit **next to `consumer.py`** — adjust paths as needed):

```python
# Align keys with Bitquery docs.
"bootstrap.servers": "rpk0.bitquery.io:9093,rpk1.bitquery.io:9093,rpk2.bitquery.io:9093",
"security.protocol": "SASL_SSL",
"sasl.mechanism": "SCRAM-SHA-512",
"sasl.username": "...",
"sasl.password": "...",
"ssl.ca.location": "server.cer.pem",
"ssl.certificate.location": "client.cer.pem",
"ssl.key.location": "client.key.pem",
"ssl.endpoint.identification.algorithm": "none",
```

**TLS protocol version** is **not** a single `sslVersion` field in **`confluent_kafka`** — OpenSSL inside librdkafka negotiates with the broker (typically **TLS 1.2+**).

See also Bitquery’s **Java + keystore** walkthrough for another packaging of the same idea: <a href="https://github.com/bitquery/kafka-consumer-example/tree/main/Java" target="_blank" rel="noopener noreferrer">`kafka-consumer-example` / `Java`</a>.

---

## Configuration

See **`.env.example`** for:

- **`KAFKA_TOPIC`**, **`KAFKA_BOOTSTRAP_SERVERS`**, **`KAFKA_GROUP_ID`**, **`KAFKA_AUTO_OFFSET_RESET`** (`latest` / `earliest`).

Kafka client settings include **`SASL_PLAINTEXT`**, **SCRAM-SHA-512**, and **`enable.auto.commit=false`**.

### Changing topic or chain

Updating **`KAFKA_TOPIC`** alone is **not** enough: import and decode the **correct generated message type** for that topic in **`consumer.py`** (same schema Bitquery publishes for that stream).

---

## Protobuf runtime note

**`requirements.txt`** pins **`protobuf>=6.30.2`** so generated code and runtime stay aligned.

Older snippets that use **`field.label == LABEL_REPEATED`** can fail on newer protobuf (**`FieldDescriptor` has no attribute `label`**). This repo’s **`protobuf_print.py`** prefers **`field.is_repeated()`** when available, with a fallback for older descriptors — reducing copy-paste breakage when users upgrade **`protobuf`**.

---

## Credentials and repository hygiene

- **Never commit** **`.env`**. Only **`.env.example`** is meant for git—copy it to **`.env`** locally and add your secrets there.
- **`KAFKA_USERNAME`** / **`KAFKA_PASSWORD`** are **Kafka stream** credentials, **not** IDE API keys. Request access via Bitquery’s **<a href="https://bitquery.io/forms/api" target="_blank" rel="noopener noreferrer">API request form</a>** (support / sales), as in the root **<a href="../README.md" target="_blank" rel="noopener noreferrer">repository README</a>** (Security callout).

---

## Licence / terms

Educational sample — follow **Bitquery’s terms** for stream and API access.
