# Use cases (Kafka streams)

Larger workflows on Bitquery Kafka protobuf streams — filters, dashboards, liquidity monitoring, wallet sets, trading-adjacent patterns — grouped **one scenario per subdirectory**.

Minimal consumers live beside this folder (**`python-consumer-example/`**, **`js-consumer-example/`**, **`go-consumer-example/`**).

---

## Bundled scenarios

These directories are copies of published examples cited in Bitquery documentation. They ship **without** a nested `.git` directory so the parent repo is a single clone; they **do not** track upstream history automatically. Align updates with upstream projects as appropriate.

| Folder | Scenario | Reference |
|--------|----------|-----------|
| <a href="./sniper-bot-bsc/" target="_blank" rel="noopener noreferrer">`sniper-bot-bsc/`</a> | BSC / launch monitoring with Kafka-backed logic | <a href="https://github.com/bitquery/sniper-bot-bsc" target="_blank" rel="noopener noreferrer">`bitquery/sniper-bot-bsc`</a> |
| <a href="./binance-exchange-wallets-monitoring/" target="_blank" rel="noopener noreferrer">`binance-exchange-wallets-monitoring/`</a> | Many exchange-labeled wallets on BSC (**`bsc.tokens.proto`**) | <a href="https://github.com/bitquery/binance-exchange-wallets-monitoring" target="_blank" rel="noopener noreferrer">`bitquery/binance-exchange-wallets-monitoring`</a> |
| <a href="./solana-wallet-tracker/" target="_blank" rel="noopener noreferrer">`solana-wallet-tracker/`</a> | Large-scale Solana wallet tracking | <a href="https://docs.bitquery.io/docs/usecases/track-millions-of-solana-wallets/" target="_blank" rel="noopener noreferrer">Track millions of Solana wallets via Kafka</a> |
| <a href="./realtime-liquidity-drain-detector/" target="_blank" rel="noopener noreferrer">`realtime-liquidity-drain-detector/`</a> | Real-time liquidity removal patterns | <a href="https://docs.bitquery.io/docs/usecases/realtime-liquidity-drain-detector/" target="_blank" rel="noopener noreferrer">Realtime liquidity drain detector</a> |

**Bitquery documentation**

- <a href="https://docs.bitquery.io/docs/usecases/binance-exchange-wallet-monitoring/" target="_blank" rel="noopener noreferrer">Binance exchange wallet monitoring</a>
- <a href="https://docs.bitquery.io/docs/usecases/track-millions-of-solana-wallets/" target="_blank" rel="noopener noreferrer">Track millions of Solana wallets</a>
- <a href="https://docs.bitquery.io/docs/usecases/realtime-liquidity-drain-detector/" target="_blank" rel="noopener noreferrer">Realtime liquidity drain detector</a>

Each subdirectory retains its upstream README for prerequisites and runtime instructions.

---

## Guidelines for additions

- **No secrets in git** — use environment variables or **`.env.example`** templates only.
- **Self-contained** — no imports from sibling baseline consumer folders unless you intentionally refactor upstream.
- **README required** — problem statement, setup, commands, Kafka topic/schema assumptions, and behavioral limits (offsets, duplicates, throughput).

---

## Related documentation

- <a href="https://docs.bitquery.io/docs/streams/protobuf/filtering-kafka-streams/" target="_blank" rel="noopener noreferrer">Filtering Kafka streams</a>
- <a href="https://docs.bitquery.io/docs/category/kafka-streams/" target="_blank" rel="noopener noreferrer">Kafka streams hub</a>
