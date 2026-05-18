# Bitquery Kafka Streams — Examples and Use Cases

Examples for consuming <a href="https://docs.bitquery.io/docs/category/kafka-streams/" target="_blank" rel="noopener noreferrer">Bitquery Kafka streams</a> (protobuf over Kafka): **Python**, **Go**, and **Node.js** baseline consumers and scenario-sized projects under <a href="./usecases/" target="_blank" rel="noopener noreferrer">`usecases/`</a>. See <a href="https://docs.bitquery.io" target="_blank" rel="noopener noreferrer">docs.bitquery.io</a> for Kafka concepts, authentication, and topic details.

Shared conventions apply across baseline consumers (default transport settings, **`KAFKA_USERNAME` / `KAFKA_PASSWORD`** via **`.env`**, protobuf on stdout / logs on stderr).

> **Security:** Treat this repository as **public**. Never commit secrets. Use a local **`.env`** (typically gitignored) and ship only **`.env.example`** templates.

---

## Repository layout

| Folder                                                   | Role                                                                                                                                                 |
| -------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| <a href="./python-consumer-example/" target="_blank" rel="noopener noreferrer">`python-consumer-example/`</a> | Kafka consumer using **`confluent_kafka`** + Bitquery Solana protobuf helpers.                                                                       |
| <a href="./js-consumer-example/" target="_blank" rel="noopener noreferrer">`js-consumer-example/`</a>         | Kafka consumer using **KafkaJS** + **`bitquery-protobuf-schema`**.                                                                                   |
| <a href="./go-consumer-example/" target="_blank" rel="noopener noreferrer">`go-consumer-example/`</a>         | Kafka consumer using **`confluent-kafka-go`** + **`github.com/bitquery/streaming_protobuf/v2`**.                                                     |
| <a href="./usecases/" target="_blank" rel="noopener noreferrer">`usecases/`</a>                               | Additional examples keyed to specific chains or workflows. Each subfolder includes its own README. See <a href="./usecases/README.md" target="_blank" rel="noopener noreferrer">`usecases/README.md`</a>. |

Each baseline consumer folder is **self-contained** (no cross-imports between language folders).

---

## Packages

Baseline consumers use Bitquery-published helpers for protobuf decoding:

| Language       | Package                                                                                                                                                |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Python**     | <a href="https://pypi.org/project/bitquery-pb2-kafka-package/" target="_blank" rel="noopener noreferrer">`bitquery-pb2-kafka-package`</a> (PyPI)                                                            |
| **JavaScript** | <a href="https://www.npmjs.com/package/bitquery-protobuf-schema" target="_blank" rel="noopener noreferrer">`bitquery-protobuf-schema`</a> (npm)                                                             |
| **Go**         | <a href="https://pkg.go.dev/github.com/bitquery/streaming_protobuf/v2" target="_blank" rel="noopener noreferrer">`github.com/bitquery/streaming_protobuf/v2`</a> — import paths depend on chain and message |

---

## Sample payloads (`kafka-data-sample`)

JSON mirror payloads for inspecting topic structure offline — **<a href="https://github.com/bitquery/kafka-data-sample" target="_blank" rel="noopener noreferrer">bitquery/kafka-data-sample</a>**. Live Kafka streams use **Protobuf**; those samples document fields and nesting before you decode binary messages.

---

## Conventions (baseline consumers)

These defaults keep behavior consistent across languages and make stdout easy to pipe or parse:

| Topic                | Convention                                                                                                                                                                                                                                                                                                            |
| -------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Transport & auth** | **`SASL_PLAINTEXT`** + **SCRAM-SHA-512** on **`9092`**. Optional **TLS**: **`SASL_SSL`** on **`9093`** and PEM paths — **<a href="#optional-tls-sasl_ssl-port-9093" target="_blank" rel="noopener noreferrer">Optional TLS</a>** and Bitquery’s **<a href="https://docs.bitquery.io/docs/streams/kafka-streaming-concepts/#ssl-connection-sasl_ssl-" target="_blank" rel="noopener noreferrer">SSL connection (SASL_SSL)</a>**. |
| **Credentials**      | **`KAFKA_USERNAME`**, **`KAFKA_PASSWORD`** from the environment or **`.env`** (see each **`/.env.example`**).                                                                                                                                                                                                         |
| **Stdout**           | Decoded protobuf only (readable field tree; **`bytes`** encoded as Solana-style **base58**). Omit partition/offset noise.                                                                                                                                                                                             |
| **Stderr**           | Logging, errors, lifecycle messages.                                                                                                                                                                                                                                                                                  |
| **Offsets**          | **`enable.auto.commit`** (and equivalents) disabled unless documented otherwise — restarts may re-read depending on consumer group and retention.                                                                                                                                                                     |

---

## Optional: TLS (`SASL_SSL`, port `9093`)

Baseline consumers use **`SASL_PLAINTEXT`** on **`9092`** by default. For TLS + SASL, follow Bitquery’s **<a href="https://docs.bitquery.io/docs/streams/kafka-streaming-concepts/#ssl-connection-sasl_ssl-" target="_blank" rel="noopener noreferrer">Kafka streams concepts — SSL connection (SASL_SSL)</a>**.

Use these PEM filenames (aligned with Bitquery docs):

| File                 | Typical librdkafka-style key   |
| -------------------- | ------------------------------ |
| **`server.cer.pem`** | **`ssl.ca.location`**          |
| **`client.cer.pem`** | **`ssl.certificate.location`** |
| **`client.key.pem`** | **`ssl.key.location`**         |

Official copies alongside sample clients: **<a href="https://github.com/bitquery/kafka-consumer-example" target="_blank" rel="noopener noreferrer">bitquery/kafka-consumer-example</a>** (`server.cer.pem`, `client.cer.pem`, `client.key.pem` at repo root). Example fetch:

```bash
curl -fsSLO https://raw.githubusercontent.com/bitquery/kafka-consumer-example/main/server.cer.pem
curl -fsSLO https://raw.githubusercontent.com/bitquery/kafka-consumer-example/main/client.cer.pem
curl -fsSLO https://raw.githubusercontent.com/bitquery/kafka-consumer-example/main/client.key.pem
```

Store keys **outside revision control**. Protect **`client.key.pem`** as you would any private key. TLS bootstrap brokers are typically **`rpk0.bitquery.io:9093,rpk1.bitquery.io:9093,rpk2.bitquery.io:9093`**.

Language-specific **`SASL_SSL`** snippets appear under **“Encryption in transit (TLS / SSL)”** in **`python-consumer-example/`**, **`js-consumer-example/`**, and **`go-consumer-example/`**.

---

## Documentation

- <a href="https://docs.bitquery.io/docs/category/kafka-streams/" target="_blank" rel="noopener noreferrer">Kafka streams — category hub</a>
- <a href="https://docs.bitquery.io/docs/streams/protobuf/kafka-protobuf-go/" target="_blank" rel="noopener noreferrer">Go — Kafka protobuf streams</a>
- <a href="https://docs.bitquery.io/docs/streams/protobuf/kafka-protobuf-python/" target="_blank" rel="noopener noreferrer">Python — Solana shreds from Kafka</a>
- <a href="https://docs.bitquery.io/docs/streams/protobuf/kafka-protobuf-js/" target="_blank" rel="noopener noreferrer">JavaScript — Solana Kafka shred stream</a>
- <a href="https://docs.bitquery.io/docs/streams/protobuf/filtering-kafka-streams/" target="_blank" rel="noopener noreferrer">Filtering Kafka streams</a>
- <a href="https://docs.bitquery.io/docs/streams/kafka-streaming-concepts/#ssl-connection-sasl_ssl-" target="_blank" rel="noopener noreferrer">Kafka streams concepts — SASL_SSL / certificates</a>
- <a href="https://github.com/bitquery/kafka-data-sample" target="_blank" rel="noopener noreferrer">Kafka topic JSON samples (`kafka-data-sample`)</a>

---

## Kafka topics (examples) and protobuf types

Which topics you may consume depends on **Bitquery provisioning** for your account. Topic names follow **`chain`.`variant`.`stream`.proto** patterns (including **`broadcasted`** mempool-style variants where listed).

Match **`KAFKA_TOPIC`** (or subscription config) to the **protobuf message type** your decoder expects — swapping topic alone without the right generated types will fail decode.

### Trading (`trading` namespace)

Multi-chain streams (not tied to a single chain prefix). Both use the **same Kafka credentials** as your subscription.

| Topic                | Description                                                                                                                                                                                                                                                                                                                          |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **`trading.prices`** | Multi-chain **Price Index** streams. See the **<a href="https://docs.bitquery.io/docs/category/crypto-price-apis/" target="_blank" rel="noopener noreferrer">Crypto Price APIs</a>** documentation for usage.                                                                                                                                                                             |
| **`trading.trades`** | Multi-chain Real-time Trader focused **DEX trades**, aligned with the **<a href="https://docs.bitquery.io/docs/category/crypto-trades-api/" target="_blank" rel="noopener noreferrer">Crypto Trades API</a>**. Message layout is defined in **<a href="https://github.com/bitquery/streaming_protobuf/blob/main/market/trades.proto" target="_blank" rel="noopener noreferrer">`market/trades.proto`</a>** in Bitquery **Streaming Protobuf**. |

Example JSON mirrors: **<a href="https://github.com/bitquery/kafka-data-sample" target="_blank" rel="noopener noreferrer">kafka-data-sample</a>** (`trading_prices_sample.json`, `trading_trades.json`).

### Bitcoin

| Topic                    | Top-level protobuf message                                                    |
| ------------------------ | ----------------------------------------------------------------------------- |
| `btc.transactions.proto` | _(Bitcoin transactions — decode using Bitquery Bitcoin protobuf definitions)_ |

### Ethereum (`eth`)

These shapes apply to **committed** blocks. For **mempool / broadcasted** variants on EVM chains, use **`*.broadcasted.transactions.proto`** → **`ParsedAbiBlockMessage`**, **`*.broadcasted.tokens.proto`** → **`TokenBlockMessage`**, **`*.broadcasted.dextrades.proto`** → **`DexBlockMessage`**, **`*.broadcasted.raw.proto`** → **`BlockMessage`** (see <a href="https://docs.bitquery.io/docs/streams/kafka-streaming-concepts/" target="_blank" rel="noopener noreferrer">Kafka streams concepts — EVM chains</a>).

| Topic                                | Top-level protobuf message                                                                                                                                                                                                                                      |
| ------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `eth.transactions.proto`             | **`ParsedAbiBlockMessage`**                                                                                                                                                                                                                                     |
| `eth.tokens.proto`                   | **`TokenBlockMessage`**                                                                                                                                                                                                                                         |
| `eth.dextrades.proto`                | **`DexBlockMessage`**                                                                                                                                                                                                                                           |
| `eth.dexpools.proto`                 | DEX pool stream — **<a href="https://docs.bitquery.io/docs/cubes/evm-dexpool/" target="_blank" rel="noopener noreferrer">DEXPools Cube documentation</a>**                                                                                                      |
| `eth.raw.proto`                      | **`BlockMessage`** (raw block data)                                                                                                                                                                                                                             |
| `eth.broadcasted.transactions.proto` | **`ParsedAbiBlockMessage`**                                                                                                                                                                                                                                     |
| `eth.broadcasted.tokens.proto`       | **`TokenBlockMessage`**                                                                                                                                                                                                                                         |
| `eth.broadcasted.dextrades.proto`    | **`DexBlockMessage`**                                                                                                                                                                                                                                           |
| `eth.broadcasted.raw.proto`          | **`BlockMessage`** (raw broadcasted block data)                                                                                                                                                                                                                 |

### BNB Chain (`bsc`)

| Topic                     | Top-level protobuf message                                                                                                                                                                                                                                      |
| ------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `bsc.transactions.proto`  | **`ParsedAbiBlockMessage`**                                                                                                                                                                                                                                     |
| `bsc.tokens.proto`       | **`TokenBlockMessage`**                                                                                                                                                                                                                                         |
| `bsc.dextrades.proto`    | **`DexBlockMessage`**                                                                                                                                                                                                                                           |
| `bsc.dexpools.proto`     | DEX pool stream — **<a href="https://docs.bitquery.io/docs/cubes/evm-dexpool/" target="_blank" rel="noopener noreferrer">DEXPools Cube documentation</a>**                                                                                                      |

_Add **`broadcasted`** in the topic name (e.g. `bsc.broadcasted.transactions.proto`) for mempool-level EVM variants — same protobuf mapping as in the Ethereum table._

### Base (`base`)

| Topic                      | Top-level protobuf message                                                                                                                                                                                                                                      |
| -------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `base.transactions.proto`  | **`ParsedAbiBlockMessage`**                                                                                                                                                                                                                                     |
| `base.tokens.proto`        | **`TokenBlockMessage`**                                                                                                                                                                                                                                         |
| `base.dextrades.proto`     | **`DexBlockMessage`**                                                                                                                                                                                                                                           |
| `base.dexpools.proto`      | DEX pool stream — **<a href="https://docs.bitquery.io/docs/cubes/evm-dexpool/" target="_blank" rel="noopener noreferrer">DEXPools Cube documentation</a>**                                                                                                      |

_Add **`broadcasted`** segment after the chain prefix for mempool-level streams where Bitquery exposes them._

### Polygon (`matic`)

| Topic                                  | Top-level protobuf message                                                                                                                                                                                                                                      |
| -------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `matic.transactions.proto`             | **`ParsedAbiBlockMessage`**                                                                                                                                                                                                                                     |
| `matic.tokens.proto`                   | **`TokenBlockMessage`**                                                                                                                                                                                                                                         |
| `matic.dextrades.proto`                | **`DexBlockMessage`**                                                                                                                                                                                                                                           |
| `matic.dexpools.proto`                 | DEX pool stream — **<a href="https://docs.bitquery.io/docs/cubes/evm-dexpool/" target="_blank" rel="noopener noreferrer">DEXPools Cube documentation</a>**                                                                                                      |
| `matic.predictions.proto`              | Prediction markets stream — decode using **<a href="https://github.com/bitquery/streaming_protobuf" target="_blank" rel="noopener noreferrer">Bitquery Streaming Protobuf</a>** for Polygon prediction topics                                                      |
| `matic.broadcasted.predictions.proto` | Prediction markets (**broadcasted**) — same schema source as **`matic.predictions.proto`**                                                                                                                                                                       |

_Add **`broadcasted`** variants for standard EVM **`transactions` / `tokens` / `dextrades` / `raw`** topics where provisioned._

### Optimism (`optimism`)

| Topic                         | Top-level protobuf message                                                                                                                                                                                                                                      |
| ----------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `optimism.transactions.proto` | **`ParsedAbiBlockMessage`**                                                                                                                                                                                                                                     |
| `optimism.tokens.proto`       | **`TokenBlockMessage`**                                                                                                                                                                                                                                         |
| `optimism.dextrades.proto`    | **`DexBlockMessage`**                                                                                                                                                                                                                                           |

_Add **`broadcasted`** variants where Bitquery exposes them for Optimism._

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

For Tron message type names and imports, use the **streaming protobuf** definitions aligned with each topic (<a href="https://github.com/bitquery/streaming_protobuf" target="_blank" rel="noopener noreferrer">schemas overview</a>).

Authoritative naming and additions beyond this list appear in Bitquery docs (for example the <a href="https://docs.bitquery.io/docs/streams/kafka-streaming-concepts/" target="_blank" rel="noopener noreferrer">Kafka streams concepts</a> topic lists).

---

## Repository contents

| Area                                    | Status                                                               |
| --------------------------------------- | -------------------------------------------------------------------- |
| Baseline consumers (Python / Node / Go) | Provided                                                             |
| <a href="./usecases/" target="_blank" rel="noopener noreferrer">`usecases/`</a>              | Scenario examples — see <a href="./usecases/README.md" target="_blank" rel="noopener noreferrer">`usecases/README.md`</a> |

---

## Contributing

- Keep examples runnable and narrowly scoped.
- Never commit secrets; prefer **`.env.example`** templates.
- If you change stdout/stderr behavior in one baseline, align the others and update READMEs accordingly.
