# Quick Start Guide

## Prerequisites

- Python
- Access to Bitquery Kafka Streams (reach out to the Bitquery team on Telegram for credentials). It's a completely free trial.

## Installation & Setup

1. Clone the repository then cd:

```bash
cd solana-wallet-tracker
```

2. Install dependencies:

```bash
pip install confluent-kafka protobuf base58 bitquery-pb2-kafka-package python-dotenv
```

3. Configure your credentials:
   Set these variables in a newly created .env file with the credentials you got from BQ support TG channel.

```
# Kafka credentials
KAFKA_USERNAME = <username>
KAFKA_PASSWORD = <password>
```

4. Run the wallet tracker:

```bash
python wallet_balance_extractor.py
```
