# Bitquery Kafka — examples and use cases

Examples for consuming [Bitquery Kafka streams](https://docs.bitquery.io/docs/category/kafka-streams/) (protobuf over Kafka): **Python**, **Go**, and **Node.js** baseline consumers and scenario-sized projects under [`usecases/`](./usecases/). See [docs.bitquery.io](https://docs.bitquery.io) for Kafka concepts, authentication, and topic details.

Shared conventions apply across baseline consumers (default transport settings, **`KAFKA_USERNAME` / `KAFKA_PASSWORD`** via **`.env`**, protobuf on stdout / logs on stderr).

> **Security:** Treat this repository as **public**. Never commit secrets. Use a local **`.env`** (typically gitignored) and ship only **`.env.example`** templates.

---

## Repository layout

| Folder                                                   | Role                                                                                                                                                 |
| -------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`python-consumer-example/`](./python-consumer-example/) | Kafka consumer using **`confluent_kafka`** + Bitquery Solana protobuf helpers.                                                                       |
| [`js-consumer-example/`](./js-consumer-example/)         | Kafka consumer using **KafkaJS** + **`bitquery-protobuf-schema`**.                                                                                   |
| [`go-consumer-example/`](./go-consumer-example/)         | Kafka consumer using **`confluent-kafka-go`** + **`github.com/bitquery/streaming_protobuf/v2`**.                                                     |
| [`usecases/`](./usecases/)                               | Additional examples keyed to specific chains or workflows. Each subfolder includes its own README. See [`usecases/README.md`](./usecases/README.md). |

Each baseline consumer folder is **self-contained** (no cross-imports between language folders).

---

## Packages

Baseline consumers use Bitquery-published helpers for protobuf decoding:

| Language       | Package                                                                                                                                                |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Python**     | [`bitquery-pb2-kafka-package`](https://pypi.org/project/bitquery-pb2-kafka-package/) (PyPI)                                                            |
| **JavaScript** | [`bitquery-protobuf-schema`](https://www.npmjs.com/package/bitquery-protobuf-schema) (npm)                                                             |
| **Go**         | [`github.com/bitquery/streaming_protobuf/v2`](https://pkg.go.dev/github.com/bitquery/streaming_protobuf/v2) — import paths depend on chain and message |

---

## Sample payloads (`kafka-data-sample`)

JSON mirror payloads for inspecting topic structure offline — **[bitquery/kafka-data-sample](https://github.com/bitquery/kafka-data-sample)**. Live Kafka streams use **Protobuf**; those samples document fields and nesting before you decode binary messages.

---

## Conventions (baseline consumers)

These defaults keep behavior consistent across languages and make stdout easy to pipe or parse:

| Topic                | Convention                                                                                                                                                                                                                                                                                                            |
| -------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Transport & auth** | **`SASL_PLAINTEXT`** + **SCRAM-SHA-512** on **`9092`**. Optional **TLS**: **`SASL_SSL`** on **`9093`** and PEM paths — **[Optional TLS](#optional-tls-sasl_ssl-port-9093)** and Bitquery’s **[SSL connection (SASL_SSL)](https://docs.bitquery.io/docs/streams/kafka-streaming-concepts/#ssl-connection-sasl_ssl-)**. |
| **Credentials**      | **`KAFKA_USERNAME`**, **`KAFKA_PASSWORD`** from the environment or **`.env`** (see each **`/.env.example`**).                                                                                                                                                                                                         |
| **Stdout**           | Decoded protobuf only (readable field tree; **`bytes`** encoded as Solana-style **base58**). Omit partition/offset noise.                                                                                                                                                                                             |
| **Stderr**           | Logging, errors, lifecycle messages.                                                                                                                                                                                                                                                                                  |
| **Offsets**          | **`enable.auto.commit`** (and equivalents) disabled unless documented otherwise — restarts may re-read depending on consumer group and retention.                                                                                                                                                                     |

---

## Optional: TLS (`SASL_SSL`, port `9093`)

Baseline consumers use **`SASL_PLAINTEXT`** on **`9092`** by default. For TLS + SASL, follow Bitquery’s **[Kafka streams concepts — SSL connection (SASL_SSL)](https://docs.bitquery.io/docs/streams/kafka-streaming-concepts/#ssl-connection-sasl_ssl-)**.

Use these PEM filenames (aligned with Bitquery docs):

| File                 | Typical librdkafka-style key   |
| -------------------- | ------------------------------ |
| **`server.cer.pem`** | **`ssl.ca.location`**          |
| **`client.cer.pem`** | **`ssl.certificate.location`** |
| **`client.key.pem`** | **`ssl.key.location`**         |

Official copies alongside sample clients: **[bitquery/kafka-consumer-example](https://github.com/bitquery/kafka-consumer-example)** (`server.cer.pem`, `client.cer.pem`, `client.key.pem` at repo root). Example fetch:

```bash
curl -fsSLO https://raw.githubusercontent.com/bitquery/kafka-consumer-example/main/server.cer.pem
curl -fsSLO https://raw.githubusercontent.com/bitquery/kafka-consumer-example/main/client.cer.pem
curl -fsSLO https://raw.githubusercontent.com/bitquery/kafka-consumer-example/main/client.key.pem
```

Store keys **outside revision control**. Protect **`client.key.pem`** as you would any private key. TLS bootstrap brokers are typically **`rpk0.bitquery.io:9093,rpk1.bitquery.io:9093,rpk2.bitquery.io:9093`**.

Language-specific **`SASL_SSL`** snippets appear under **“Encryption in transit (TLS / SSL)”** in **`python-consumer-example/`**, **`js-consumer-example/`**, and **`go-consumer-example/`**.

---

## Documentation

- [Kafka streams — category hub](https://docs.bitquery.io/docs/category/kafka-streams/)
- [Go — Kafka protobuf streams](https://docs.bitquery.io/docs/streams/protobuf/kafka-protobuf-go/)
- [Python — Solana shreds from Kafka](https://docs.bitquery.io/docs/streams/protobuf/kafka-protobuf-python/)
- [JavaScript — Solana Kafka shred stream](https://docs.bitquery.io/docs/streams/protobuf/kafka-protobuf-js/)
- [Filtering Kafka streams](https://docs.bitquery.io/docs/streams/protobuf/filtering-kafka-streams/)
- [Kafka streams concepts — SASL_SSL / certificates](https://docs.bitquery.io/docs/streams/kafka-streaming-concepts/#ssl-connection-sasl_ssl-)
- [Kafka topic JSON samples (`kafka-data-sample`)](https://github.com/bitquery/kafka-data-sample)

---

## Kafka topics (examples) and protobuf types

Which topics you may consume depends on **Bitquery provisioning** for your account. Topic names follow **`chain`.`variant`.`stream`.proto** patterns (including **`broadcasted`** mempool-style variants where listed).

Match **`KAFKA_TOPIC`** (or subscription config) to the **protobuf message type** your decoder expects — swapping topic alone without the right generated types will fail decode.

### Trading (`trading` namespace)

Multi-chain streams (not tied to a single chain prefix). Both use the **same Kafka credentials** as your subscription.

| Topic                | Description                                                                                                                                                                                                                                                                                                                          |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **`trading.prices`** | Multi-chain **Price Index** streams. See the **[Crypto Price APIs](https://docs.bitquery.io/docs/category/crypto-price-apis/)** documentation for usage.                                                                                                                                                                             |
| **`trading.trades`** | Multi-chain Real-time Trader focused **DEX trades**, aligned with the **[Crypto Trades API](https://docs.bitquery.io/docs/category/crypto-trades-api/)**. Message layout is defined in **[`market/trades.proto`](https://github.com/bitquery/streaming_protobuf/blob/main/market/trades.proto)** in Bitquery **Streaming Protobuf**. |

Example JSON mirrors: **[kafka-data-sample](https://github.com/bitquery/kafka-data-sample)** (`trading_prices_sample.json`, `trading_trades.json`).

### Bitcoin

| Topic                    | Top-level protobuf message                                                    |
| ------------------------ | ----------------------------------------------------------------------------- |
| `btc.transactions.proto` | _(Bitcoin transactions — decode using Bitquery Bitcoin protobuf definitions)_ |

### Ethereum (`eth`)

| Topic                                | Top-level protobuf message                      |
| ------------------------------------ | ----------------------------------------------- |
| `eth.transactions.proto`             | **`ParsedAbiBlockMessage`**                     |
| `eth.tokens.proto`                   | **`TokenBlockMessage`**                         |
| `eth.dextrades.proto`                | **`DexBlockMessage`**                           |
| `eth.raw.proto`                      | **`BlockMessage`** (raw block data)             |
| `eth.broadcasted.transactions.proto` | **`ParsedAbiBlockMessage`**                     |
| `eth.broadcasted.tokens.proto`       | **`TokenBlockMessage`**                         |
| `eth.broadcasted.dextrades.proto`    | **`DexBlockMessage`**                           |
| `eth.broadcasted.raw.proto`          | **`BlockMessage`** (raw broadcasted block data) |

### Solana (`solana`)

| Topic                       | Top-level protobuf message  |
| --------------------------- | --------------------------- |
| `solana.transactions.proto` | **`ParsedIdlBlockMessage`** |
| `solana.tokens.proto`       | **`TokenBlockMessage`**     |
| `solana.dextrades.proto`    | **`DexParsedBlockMessage`** |

### Tron (`tron`)

| Topic                                 | Notes                       |
| ------------------------------------- | --------------------------- |
| `tron.raw.proto`                      | Raw block data              |
| `tron.transactions.proto`             | Transactions                |
| `tron.tokens.proto`                   | Token transfers             |
| `tron.dextrades.proto`                | DEX trades                  |
| `tron.broadcasted.raw.proto`          | Raw broadcasted block data  |
| `tron.broadcasted.transactions.proto` | Broadcasted transactions    |
| `tron.broadcasted.tokens.proto`       | Broadcasted token transfers |
| `tron.broadcasted.dextrades.proto`    | Broadcasted DEX trades      |

For Tron message type names and imports, use the **streaming protobuf** definitions aligned with each topic ([schemas overview](https://github.com/bitquery/streaming_protobuf)).

Authoritative naming and additions beyond this list appear in Bitquery docs (for example the [Kafka streams concepts](https://docs.bitquery.io/docs/streams/kafka-streaming-concepts/) topic lists).

---

## Repository contents

| Area                                    | Status                                                               |
| --------------------------------------- | -------------------------------------------------------------------- |
| Baseline consumers (Python / Node / Go) | Provided                                                             |
| [`usecases/`](./usecases/)              | Scenario examples — see [`usecases/README.md`](./usecases/README.md) |

---

## Contributing

- Keep examples runnable and narrowly scoped.
- Never commit secrets; prefer **`.env.example`** templates.
- If you change stdout/stderr behavior in one baseline, align the others and update READMEs accordingly.
