# Use cases (Kafka streams)

Larger workflows on Bitquery Kafka protobuf streams — filters, dashboards, liquidity monitoring, wallet sets, trading-adjacent patterns — grouped **one scenario per subdirectory**.

Minimal consumers live beside this folder (**`python-consumer-example/`**, **`js-consumer-example/`**, **`go-consumer-example/`**).

---

## Bundled scenarios

These directories are copies of published examples cited in Bitquery documentation. They ship **without** a nested `.git` directory so the parent repo is a single clone; they **do not** track upstream history automatically. Align updates with upstream projects as appropriate.

| Folder | Scenario | Reference |
|--------|----------|-----------|
| [`sniper-bot-bsc/`](./sniper-bot-bsc/) | BSC / launch monitoring with Kafka-backed logic | [`bitquery/sniper-bot-bsc`](https://github.com/bitquery/sniper-bot-bsc) |
| [`binance-exchange-wallets-monitoring/`](./binance-exchange-wallets-monitoring/) | Many exchange-labeled wallets on BSC (**`bsc.tokens.proto`**) | [`bitquery/binance-exchange-wallets-monitoring`](https://github.com/bitquery/binance-exchange-wallets-monitoring) |
| [`solana-wallet-tracker/`](./solana-wallet-tracker/) | Large-scale Solana wallet tracking | [Track millions of Solana wallets via Kafka](https://docs.bitquery.io/docs/usecases/track-millions-of-solana-wallets/) |
| [`realtime-liquidity-drain-detector/`](./realtime-liquidity-drain-detector/) | Real-time liquidity removal patterns | [Realtime liquidity drain detector](https://docs.bitquery.io/docs/usecases/realtime-liquidity-drain-detector/) |

**Bitquery documentation**

- [Binance exchange wallet monitoring](https://docs.bitquery.io/docs/usecases/binance-exchange-wallet-monitoring/)
- [Track millions of Solana wallets](https://docs.bitquery.io/docs/usecases/track-millions-of-solana-wallets/)
- [Realtime liquidity drain detector](https://docs.bitquery.io/docs/usecases/realtime-liquidity-drain-detector/)

Each subdirectory retains its upstream README for prerequisites and runtime instructions.

---

## Guidelines for additions

- **No secrets in git** — use environment variables or **`.env.example`** templates only.
- **Self-contained** — no imports from sibling baseline consumer folders unless you intentionally refactor upstream.
- **README required** — problem statement, setup, commands, Kafka topic/schema assumptions, and behavioral limits (offsets, duplicates, throughput).

---

## Related documentation

- [Filtering Kafka streams](https://docs.bitquery.io/docs/streams/protobuf/filtering-kafka-streams/)
- [Kafka streams hub](https://docs.bitquery.io/docs/category/kafka-streams/)
