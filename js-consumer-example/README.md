# Node.js — Bitquery Kafka consumer (`solana.transactions.proto`)

CLI-style **Node.js** app (**not** a browser bundle): **KafkaJS**, **LZ4** (`kafkajs-lz4`), **`bitquery-protobuf-schema`** for runtime protobuf loading, **`.env`** for credentials.

**Official tutorial:** <a href="https://docs.bitquery.io/docs/streams/protobuf/kafka-protobuf-js/" target="_blank" rel="noopener noreferrer">JavaScript — Solana Kafka shred stream</a>  
(Bitquery titles this “Javascript”; the implementation is **Node + CommonJS**.)

**Parent hub:** <a href="https://docs.bitquery.io/docs/category/kafka-streams/" target="_blank" rel="noopener noreferrer">Kafka streams documentation</a>

---

## Behaviour

| Stream | Content |
|--------|---------|
| **Stdout** | Decoded protobuf only — recursive structure from **`toObject`**, **`bytes`** as **base58**. |
| **Stderr** | Operational noise only if you add logging; startup config dumps are avoided so stdout stays clean. |

Kafka **topic / partition / offset** lines are omitted on stdout by design (easier piping and parity with Python/Go baselines).

---

## Requirements

- **Node.js 18+** (LTS recommended).
- **npm** (comes with Node).
- Bitquery **`KAFKA_USERNAME`** / **`KAFKA_PASSWORD`**.

---

## Quick start

From the repository root:

```bash
cd js-consumer-example
npm install
cp .env.example .env
# Edit .env: set KAFKA_USERNAME and KAFKA_PASSWORD
npm start
```

**`npm start`** runs **`node src/index.js`** (see **`package.json`**).

**Debug KafkaJS internals:** **`npm run start:debug`** (sets **`NODE_DEBUG=kafkajs*`**).

---

## Encryption in transit (TLS / SSL)

**This sample defaults to non-TLS Kafka:** **`ssl: false`** + **SASL SCRAM-SHA-512** to **port 9092**. Authentication uses SASL; the **Kafka connection is not TLS**, so **no TLS version** is chosen for that socket.

**Official TLS reference:** Bitquery’s **`SASL_SSL`** brokers (**9093**) and PEM filenames are described in **<a href="https://docs.bitquery.io/docs/streams/kafka-streaming-concepts/#ssl-connection-sasl_ssl-" target="_blank" rel="noopener noreferrer">Kafka streams concepts — SSL connection (SASL_SSL)</a>**.

**Certificates:** Use the three PEM files from **<a href="https://github.com/bitquery/kafka-consumer-example" target="_blank" rel="noopener noreferrer">`bitquery/kafka-consumer-example`</a>** (repo root). How to fetch them: **<a href="../README.md#optional-tls-sasl_ssl-port-9093" target="_blank" rel="noopener noreferrer">root `README.md` — Optional: TLS</a>**.

**To use TLS with KafkaJS**, enable **`ssl`** with **`ca`**, **`cert`**, and **`key`** buffers (matching librdkafka’s **`ssl.ca.location`** / **`ssl.certificate.location`** / **`ssl.key.location`**), point brokers at **`9093`**, keep **`mechanism: 'scram-sha-512'`**, and mirror Bitquery’s **`ssl.endpoint.identification.algorithm: 'none'`** intent via **`rejectUnauthorized: false`** only if your ops/security posture allows it (prefer verifying the server hostname when possible). The example assumes PEMs live **`src/certs/`** under **`js-consumer-example/`** — change **`certDir`** to match where you saved the files:

```javascript
const fs = require('fs');
const path = require('path');
const certDir = path.join(__dirname, 'certs');

const kafka = new Kafka({
  clientId: 'bitquery-js-example',
  brokers: ['rpk0.bitquery.io:9093', 'rpk1.bitquery.io:9093', 'rpk2.bitquery.io:9093'],
  ssl: {
    ca: [fs.readFileSync(path.join(certDir, 'server.cer.pem'))],
    cert: fs.readFileSync(path.join(certDir, 'client.cer.pem')),
    key: fs.readFileSync(path.join(certDir, 'client.key.pem')),
    rejectUnauthorized: false, // Bitquery docs use endpoint identification "none"; tighten if you can validate SANs.
  },
  sasl: { mechanism: 'scram-sha-512', username: process.env.KAFKA_USERNAME, password: process.env.KAFKA_PASSWORD },
});
```

**TLS version:** Use Node **`tls`** options on **`ssl`** if needed — e.g. **`minVersion`** (see <a href="https://nodejs.org/api/tls.html#tlsconnectoptions-callback" target="_blank" rel="noopener noreferrer">Node.js `tls.connect` options</a>). Defaults are typically **TLS 1.2+** when unset.

---

## Configuration

Optional variables are documented in **`.env.example`** (topic, bootstrap servers, consumer group, **`KAFKA_FROM_BEGINNING`**, etc.).

---

## Offsets and commits

**`autoCommit: false`** — same posture as Bitquery’s Node tutorial: offsets are **not** committed unless you extend the code (**`consumer.commitOffsets(...)`**). Restart behaviour depends on **consumer group**, **offset reset**, and broker **retention**.

---

## Troubleshooting

| Symptom | Check |
|---------|--------|
| **`loadProto` / decode errors** | **`KAFKA_TOPIC`** matches a topic and schema enabled for your account. |
| **Connection / SASL** | Outbound access to brokers in **`.env.example`**; credentials correct. |
| **Flooded terminal** | High-throughput topics — reduce logging or filter in application code (see <a href="https://docs.bitquery.io/docs/streams/protobuf/filtering-kafka-streams/" target="_blank" rel="noopener noreferrer">filtering guide</a>). |

---

## Credentials

Never commit **`.env`**. Only **`.env.example`** in git.

---

## Licence / terms

Educational sample — follow Bitquery’s terms for stream and API access.
