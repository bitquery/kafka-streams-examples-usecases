# Quick Start Guide

## Prerequisites

- Python
- Access to Bitquery Kafka Streams — **Kafka username and password** are **stream credentials** (not IDE API keys). Request them via the **<a href="https://bitquery.io/forms/api" target="_blank" rel="noopener noreferrer">API request form</a>** or Bitquery support (e.g. Telegram). Trial access may be available — ask Bitquery.

## Installation & Setup

1. Clone the repository then cd:

```bash
cd solana-wallet-tracker
```

2. Install dependencies:

```bash
pip install confluent-kafka protobuf base58 bitquery-pb2-kafka-package python-dotenv
```

3. Configure your credentials in a **`.env`** file (never commit it). Use **`KAFKA_USERNAME`** and **`KAFKA_PASSWORD`** from Bitquery (**stream** credentials, not IDE keys — request via the **<a href="https://bitquery.io/forms/api" target="_blank" rel="noopener noreferrer">API request form</a>** or support).

   Example:

   ```env
   KAFKA_USERNAME=your_kafka_username
   KAFKA_PASSWORD=your_kafka_password
   ```

4. Run the wallet tracker:

```bash
python wallet_balance_extractor.py
```
