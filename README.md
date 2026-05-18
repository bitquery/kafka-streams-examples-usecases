# Bitquery Kafka — examples and use cases

**Canonical runnable samples** for [Bitquery Kafka streams](https://docs.bitquery.io/docs/category/kafka-streams/). This repository is maintained for **documentation, DevRel, and support**: one place to clone, one set of conventions, and links from the Kafka section of [docs.bitquery.io](https://docs.bitquery.io).

It **replaces a scattered layout** across multiple small GitHub repos with a **single public “front door”** for **Python**, **Go**, and **Node.js** baseline consumers, plus a dedicated [`usecases/`](./usecases/) area for larger recipes (filtering, scale patterns, integrations).

> **Secrets:** This repo is **public**. Never commit real credentials. Use **`.env`** locally (gitignored) and ship only **`.env.example`** templates.

---

## Repository layout

| Folder                                                   | Role                                                                                                                                    |
| -------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| [`python-consumer-example/`](./python-consumer-example/) | Baseline consumer with **`confluent_kafka`** + Bitquery Solana protobuf package.                                                        |
| [`js-consumer-example/`](./js-consumer-example/)         | Baseline consumer with **KafkaJS** + **`bitquery-protobuf-schema`** (runs with **`node`** / **`npm`**).                                 |
| [`go-consumer-example/`](./go-consumer-example/)         | Baseline consumer with **`confluent-kafka-go`** + **`github.com/bitquery/streaming_protobuf/v2`**.                                      |
| [`usecases/`](./usecases/)                               | Scenario-sized projects **vendored** as standalone snapshots (no nested git remotes). See [`usecases/README.md`](./usecases/README.md). |

Each baseline folder is **self-contained** (no cross-imports between language folders).

---

## Conventions (all baseline consumers)

These rules keep samples consistent when linked from docs and when users pipe output:

| Topic                | Convention                                                                                                                                                                                                                                                                                                                                                              |
| -------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Transport & auth** | **`SASL_PLAINTEXT`** + **SCRAM-SHA-512** on **`9092`** by default (tutorial posture). **Optional TLS:** **`SASL_SSL`** on **`9093`** + PEM files — see **[Optional: TLS (SASL_SSL)](#optional-tls-sasl_ssl-port-9093)** below and Bitquery’s **[SSL connection (SASL_SSL)](https://docs.bitquery.io/docs/streams/kafka-streaming-concepts/#ssl-connection-sasl_ssl-)**. |
| **Credentials**      | **`KAFKA_USERNAME`**, **`KAFKA_PASSWORD`** via environment or **`.env`** (see each folder’s **`.env.example`**).                                                                                                                                                                                                                                                        |
| **Stdout**           | **Decoded protobuf payload only** (human-readable tree; **`bytes`** as Solana-style **base58**). No Kafka partition/offset dumps on stdout.                                                                                                                                                                                                                             |
| **Stderr**           | Logs, connection errors, shutdown.                                                                                                                                                                                                                                                                                                                                      |
| **Offsets**          | **`enable.auto.commit` / equivalent disabled** unless a README states otherwise — matches tutorial posture; restarts may re-read messages depending on group id and retention.                                                                                                                                                                                          |

---

## Optional: TLS (`SASL_SSL`, port `9093`)

The baseline consumers in this repo default to **`SASL_PLAINTEXT`** on **`9092`**. For **encrypted** Kafka (TLS + SASL), Bitquery documents **`SASL_SSL`** on **`9093`** in **[Kafka streams concepts — SSL connection (SASL_SSL)](https://docs.bitquery.io/docs/streams/kafka-streaming-concepts/#ssl-connection-sasl_ssl-)**.

You need three PEM files (same filenames as in that doc):

| File                 | Typical client setting                                         |
| -------------------- | -------------------------------------------------------------- |
| **`server.cer.pem`** | Trust / CA (**`ssl.ca.location`** in librdkafka-style configs) |
| **`client.cer.pem`** | Client certificate (**`ssl.certificate.location`**)            |
| **`client.key.pem`** | Client private key (**`ssl.key.location`**)                    |

**Where to get them:** Bitquery publishes copies at the repo root of **[`bitquery/kafka-consumer-example`](https://github.com/bitquery/kafka-consumer-example)**:

- [`server.cer.pem`](https://github.com/bitquery/kafka-consumer-example/blob/main/server.cer.pem)
- [`client.cer.pem`](https://github.com/bitquery/kafka-consumer-example/blob/main/client.cer.pem)
- [`client.key.pem`](https://github.com/bitquery/kafka-consumer-example/blob/main/client.key.pem)

Download them.

Save them **outside git** or **gitignored paths** — especially **`client.key.pem`**. Point your consumer config at the paths where you saved them. Bootstrap brokers for TLS are typically **`rpk0.bitquery.io:9093,rpk1.bitquery.io:9093,rpk2.bitquery.io:9093`**.

Per-language TLS snippets live in each **`python-consumer-example`**, **`js-consumer-example`**, and **`go-consumer-example`** README under **“Encryption in transit (TLS / SSL)”**.

---

## Documentation links

Point readers here from the Kafka docs hub and language pages:

- [Kafka streams — category hub](https://docs.bitquery.io/docs/category/kafka-streams/)
- [Go — Kafka protobuf streams](https://docs.bitquery.io/docs/streams/protobuf/kafka-protobuf-go/)
- [Python — Solana shreds from Kafka](https://docs.bitquery.io/docs/streams/protobuf/kafka-protobuf-python/)
- [JavaScript — Solana Kafka shred stream](https://docs.bitquery.io/docs/streams/protobuf/kafka-protobuf-js/)
- [Filtering Kafka streams](https://docs.bitquery.io/docs/streams/protobuf/filtering-kafka-streams/)
- [Kafka streams concepts — SASL_SSL / certificates](https://docs.bitquery.io/docs/streams/kafka-streaming-concepts/#ssl-connection-sasl_ssl-)

---

## Topics commonly used in samples

Authoritative topic names for **your** account come from Bitquery provisioning and docs. Typical tutorial topics:

| Topic (example)             | Notes                                                                   |
| --------------------------- | ----------------------------------------------------------------------- |
| `solana.transactions.proto` | Solana transaction-level protobuf stream (default in baseline folders). |
| `solana.dextrades.proto`    | Solana DEX protobuf stream.                                             |
| `solana.tokens.proto`       | Solana token-related protobuf stream.                                   |

Changing **`KAFKA_TOPIC`** requires **matching decode types** for that topic’s schema — not only env vars.

---

## Roadmap

1. Baseline consumers (**Python**, **Node**, **Go**) — **current**.
2. **`usecases/`** — **current:** BSC sniper, Binance wallet monitoring, Solana wallet tracker, liquidity drain detector (snapshots; refresh manually from upstream when needed — see [`usecases/README.md`](./usecases/README.md)).
3. **Docs** — update Kafka doc pages to link to **stable paths** in this repo (per-language quick start + troubleshooting).

---

## Contributing

- Keep examples **small and runnable**.
- Prefer **`.env.example`** over checked-in secrets.
- Avoid breaking the **stdout / stderr** split without updating all baselines and docs.

---

_Discovery: Bitquery, Kafka, protobuf, blockchain streaming, Solana, real-time data._
