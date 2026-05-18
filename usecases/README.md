# Use cases (Kafka streams)

This directory holds **scenario-sized projects**: filtering, ordering patterns, storage, dashboards, or trading-style flows built **on top of** Bitquery Kafka protobuf streams.

Baseline consumers live one level up (**`python-consumer-example/`**, **`js-consumer-example/`**, **`go-consumer-example/`**). **Use cases** are larger and topic-specific — each lives in **its own subfolder**.

---

## Vendored snapshots (not linked to upstream git)

The subfolders below are **copies** of code that originally lived in other GitHub repositories. **`.git` is removed** here: they are **not** submodules, **not** forks, and **do not** track remotes. To refresh from upstream, re-clone the URL and replace the folder (or copy files over).

**Upstream sources at vendoring time:**

| Folder | Scenario | Clone source |
|--------|-----------|--------------|
| [`sniper-bot-bsc/`](./sniper-bot-bsc/) | BSC sniper / Four Meme launch bot (Kafka + on-chain execution) | [`bitquery/sniper-bot-bsc`](https://github.com/bitquery/sniper-bot-bsc) |
| [`binance-exchange-wallets-monitoring/`](./binance-exchange-wallets-monitoring/) | Monitor many Binance-tagged wallets on BSC (`bsc.tokens.proto`) | [`bitquery/binance-exchange-wallets-monitoring`](https://github.com/bitquery/binance-exchange-wallets-monitoring) |
| [`solana-wallet-tracker/`](./solana-wallet-tracker/) | Scale — track very large Solana wallet sets | [`Akshat-cs/solana-wallet-tracker`](https://github.com/Akshat-cs/solana-wallet-tracker) ([tutorial](https://docs.bitquery.io/docs/usecases/track-millions-of-solana-wallets/) mentions `bitquery/solana-wallet-tracker`; that repo was not publicly cloneable when this snapshot was imported.) |
| [`realtime-liquidity-drain-detector/`](./realtime-liquidity-drain-detector/) | Real-time liquidity drain detection | [`Akshat-cs/realtime-liquidity-drain-detector`](https://github.com/Akshat-cs/realtime-liquidity-drain-detector) |

**Bitquery docs for these scenarios:**

- [Binance exchange wallet monitoring](https://docs.bitquery.io/docs/usecases/binance-exchange-wallet-monitoring/)
- [Track millions of Solana wallets](https://docs.bitquery.io/docs/usecases/track-millions-of-solana-wallets/)
- [Realtime liquidity drain detector](https://docs.bitquery.io/docs/usecases/realtime-liquidity-drain-detector/)

Each subfolder keeps its **own README** and run instructions from upstream (code is **unchanged** from the snapshot).

---

## Rules for anything merged here

- **No secrets in git** — environment variables, **`.env.example`**, never real passwords or private keys.
- **Self-contained folder** — do not import sibling baseline consumer folders; external deps and Bitquery-published packages only.
- **README required** — problem statement, prerequisites, run commands, topic/schema assumptions, limits (ordering, commits, replay).

---

## Related documentation

- [Filtering Kafka streams](https://docs.bitquery.io/docs/streams/protobuf/filtering-kafka-streams/)
- [Kafka streams hub](https://docs.bitquery.io/docs/category/kafka-streams/)
