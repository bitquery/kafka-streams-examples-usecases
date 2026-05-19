# Go — Bitquery Kafka consumer (`solana.transactions.proto`)

Minimal **Go 1.23** consumer using **`confluent-kafka-go`** and Bitquery’s official **`streaming_protobuf/v2`** module (**`ParsedIdlBlockMessage`** and related types).

**Official tutorial:** <a href="https://docs.bitquery.io/docs/streams/protobuf/kafka-protobuf-go/" target="_blank" rel="noopener noreferrer">Go — Kafka protobuf streams</a>

**Reference implementation (broader scope):** <a href="https://github.com/bitquery/stream_protobuf_example" target="_blank" rel="noopener noreferrer">`bitquery/stream_protobuf_example`</a> (YAML, worker pools, etc.). This folder stays **minimal**: single goroutine, **`.env`** secrets, parity with Python/Node baselines.

**Parent hub:** <a href="https://docs.bitquery.io/docs/category/kafka-streams/" target="_blank" rel="noopener noreferrer">Kafka streams documentation</a>

---

## Behaviour

| Stream | Content |
|--------|---------|
| **Stdout** | Decoded protobuf — **`protoreflect.Range`** tree; **`bytes`** as **base58**. |
| **Stderr** | **`log`** — subscribe line, Kafka errors, shutdown. |

No partition/offset prefixes on stdout.

**Protobuf in Go:** there is no separate “protobuf-only” pip package — types come from **`github.com/bitquery/streaming_protobuf/v2/solana/messages`** (<a href="https://pkg.go.dev/github.com/bitquery/streaming_protobuf/v2/solana/messages" target="_blank" rel="noopener noreferrer">`pkg.go.dev`</a>).

---

## Requirements

1. **<a href="https://go.dev/dl/" target="_blank" rel="noopener noreferrer">Go 1.23+</a>** — **`go version`** must satisfy **`go.mod`** / **`toolchain`**.
2. **Kafka stream credentials** (**`KAFKA_USERNAME`** / **`KAFKA_PASSWORD`** in **`.env`**) — not IDE API keys; request from Bitquery via the **<a href="https://bitquery.io/forms/api" target="_blank" rel="noopener noreferrer">API request form</a>**.
3. **C toolchain + librdkafka** (CGO — required by **`confluent-kafka-go`**):
   - **macOS:** `brew install librdkafka pkg-config`
   - **Debian/Ubuntu:** `sudo apt-get install -y librdkafka-dev pkg-config gcc`

Windows-only setups often use **WSL2** or follow Confluent’s Windows notes for **`librdkafka`**.

---

## Quick start

From the repository root:

```bash
cd go-consumer-example
go mod tidy
cp .env.example .env
# Edit .env: set KAFKA_USERNAME and KAFKA_PASSWORD (obtain via https://bitquery.io/forms/api — not IDE keys)
go run .
```

**First clone or fresh machine:** **`go mod tidy`** downloads modules and fills **`go.sum`**. If you see **`missing go.sum entry`**, run **`go mod tidy`** again (needs network).

---

## Encryption in transit (TLS / SSL)

**This sample defaults to non-TLS Kafka:** **`security.protocol=SASL_PLAINTEXT`** + **SCRAM-SHA-512** on **port 9092**. SASL authenticates you; the **Kafka bytes are not TLS-encrypted**, so **no TLS version** applies to that mode.

**Official TLS reference:** **<a href="https://docs.bitquery.io/docs/streams/kafka-streaming-concepts/#ssl-connection-sasl_ssl-" target="_blank" rel="noopener noreferrer">Kafka streams concepts — SSL connection (SASL_SSL)</a>** lists **`9093`**, **`SASL_SSL`**, and the PEM keys (**`ssl.ca.location`**, **`ssl.certificate.location`**, **`ssl.key.location`**, **`ssl.endpoint.identification.algorithm`**).

**Certificates:** Download **`server.cer.pem`**, **`client.cer.pem`**, and **`client.key.pem`** from **<a href="https://github.com/bitquery/kafka-consumer-example" target="_blank" rel="noopener noreferrer">`bitquery/kafka-consumer-example`</a>** (repo root). See **<a href="../README.md#optional-tls-sasl_ssl-port-9093" target="_blank" rel="noopener noreferrer">root `README.md` — Optional: TLS</a>**.

**To switch this consumer to TLS**, extend the **`kafka.ConfigMap`** in **`main.go`** along Bitquery’s lines. Adjust **`ssl.*.location`** to wherever you saved the PEMs (example: same directory you run **`go run`** from):

```go
&kafka.ConfigMap{
	"bootstrap.servers":                     "rpk0.bitquery.io:9093,rpk1.bitquery.io:9093,rpk2.bitquery.io:9093",
	"security.protocol":                   "SASL_SSL",
	"sasl.mechanism":                      "SCRAM-SHA-512",
	"sasl.username":                       username,
	"sasl.password":                       password,
	"ssl.ca.location":                     "server.cer.pem",
	"ssl.certificate.location":            "client.cer.pem",
	"ssl.key.location":                    "client.key.pem",
	"ssl.endpoint.identification.algorithm": "none",
}
```

**TLS protocol version** is negotiated by **OpenSSL inside librdkafka** (no single **`ssl.version`** drop-down in **`confluent-kafka-go`**).

Same cert concepts as Bitquery’s **Java keystore** example, different file format: <a href="https://github.com/bitquery/kafka-consumer-example/tree/main/Java" target="_blank" rel="noopener noreferrer">`kafka-consumer-example` / `Java`</a>.

---

## Configuration

Same env contract as other baseline folders — see **`.env.example`**:

**`KAFKA_TOPIC`**, **`KAFKA_BOOTSTRAP_SERVERS`**, **`KAFKA_GROUP_ID`**, **`KAFKA_AUTO_OFFSET_RESET`**.

Client: **`SASL_PLAINTEXT`**, **SCRAM-SHA-512**, **`enable.auto.commit=false`**.

### Changing topic

Env alone is insufficient: switch **`solanapb`** import / decode type in **`main.go`** (and printer if needed) to the message type Bitquery publishes on that topic.

### Env vs YAML

Bitquery’s larger Go samples sometimes use **`config.yml`**. This repo standardises on **`.env`** for secrets and parity across languages; rationale is summarized in the **root** <a href="../README.md#conventions-baseline-consumers" target="_blank" rel="noopener noreferrer">`README.md`</a>.

---

## Credentials and repository hygiene

- **Never commit** **`.env`**. Only **`.env.example`** is meant for git—copy it to **`.env`** locally and add your secrets there.
- **`KAFKA_USERNAME`** / **`KAFKA_PASSWORD`** are **Kafka stream** credentials, **not** IDE API keys. Request access via Bitquery’s **<a href="https://bitquery.io/forms/api" target="_blank" rel="noopener noreferrer">API request form</a>** (support / sales), as in the root **<a href="../README.md" target="_blank" rel="noopener noreferrer">repository README</a>** (Security callout).

---

## Licence / terms

Educational sample — follow Bitquery’s terms for stream access.
