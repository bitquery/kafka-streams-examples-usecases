# 🚀 Bitquery Sniper Trading Bot

The **Sniper Trading Bot** is an automated crypto trading tool built for detecting and trading **newly created <a href="https://docs.bitquery.io/docs/blockchain/BSC/four-meme-api?utm_source=github_readme&amp;utm_medium=referral&amp;utm_campaign=evm_sniper&amp;utm_content=four_meme_api&amp;utm_term=four_meme" target="_blank" rel="noopener noreferrer">Four Meme tokens</a>** in real time.  
It follows a simple, high-frequency strategy — **buy instantly when the token launches** and **sell automatically after one minute**.

---

>[!NOTE]
>This is an educational purpose project only. Bitquery in no manner advice or promote any trading strategy or trading decision.

## 🔍 How It Works: Detecting New Four Meme Tokens

The bot leverages **<a href="https://docs.bitquery.io/docs/category/kafka-streams/?utm_source=github_readme&amp;utm_medium=referral&amp;utm_campaign=evm_sniper&amp;utm_content=protobuf_kafka_docs&amp;utm_term=protobuf_kafka" target="_blank" rel="noopener noreferrer">Bitquery’s Protobuf Kafka Streams</a>** to receive token creation events in real-time from the **BSC blockchain**.

You can check out the **<a href="https://docs.bitquery.io/docs/streams/protobuf/kafka-protobuf-js/?utm_source=github_readme&amp;utm_medium=referral&amp;utm_campaign=evm_sniper&amp;utm_content=kafka_protobuf_js_example&amp;utm_term=kafka_js_example" target="_blank" rel="noopener noreferrer">Kafka Protobuf JS example</a>** for a step-by-step JavaScript implementation.

**Key features:**
- Monitors **new token launches** instantly via **Kafka Streams**
- Reduces latency using **Finland-based (eu-north-1)** deployment
- Fully integrates with **Bitquery APIs** for event data streaming

---

## 💰 Buying and Selling Tokens Automatically

The bot interacts with the **Four Meme DEX Smart Contract** through `ethers.js`.  
We define custom wrapper functions:
- `buyViaLaunchpad` → Buys tokens as soon as they’re created  
- `sellTokenViaLaunchpad` → Sells after a predefined delay (default: 1 minute)

Learn more about **<a href="https://docs.ethers.org/v5/?utm_source=github_readme&amp;utm_medium=referral&amp;utm_campaign=evm_sniper&amp;utm_content=ethers_docs&amp;utm_term=ethers" target="_blank" rel="noopener noreferrer">ethers.js contract interaction</a>** and smart contract ABI usage.

---

## ⚙️ Setup Instructions

1. **Clone the repository:**
   ```sh
   git clone https://github.com/Kshitij0O7/evm-sniper?utm_source=github_readme&utm_medium=referral&utm_campaign=evm_sniper&utm_content=repo_link&utm_term=evm-sniper
   cd evm-sniper
   ```

2. **Install dependencies:**

   ```sh
   npm install
   ```

3. **Create your `.env` file** with the following variables:

   ```env
   KAFKA_USERNAME=
   KAFKA_PASSWORD=
   PRIVATE_KEY1=
   ```

Contact <a href="https://t.me/Bloxy_info/?utm_source=github_readme&amp;utm_medium=referral&amp;utm_campaign=bitcoin_streaming" target="_blank" rel="noopener noreferrer">Bitquery Support</a> or fill out this <a href="https://bitquery.io/forms/api/?utm_source=github_readme&amp;utm_medium=referral&amp;utm_campaign=bitcoin_streaming" target="_blank" rel="noopener noreferrer">form</a> to get Kafka Credentials.

4. **Run the bot:**

   ```sh
   npm run start
   ```

---

## ☁️ Deployment Guide (Google Cloud)

You can deploy the bot to a cloud provider (recommended: **<a href="https://console.cloud.google.com/?utm_source=github_readme&amp;utm_medium=referral&amp;utm_campaign=evm_sniper&amp;utm_content=gcp_console&amp;utm_term=google_cloud" target="_blank" rel="noopener noreferrer">Google Cloud</a>**) for 24/7 uptime.

### Steps:

1. Create a **VM (Virtual Machine)** in region **`eu-north-1`** (Finland) for **minimal latency** to the Bitquery Kafka service.

2. SSH into the VM and install dependencies:

   ```sh
   sudo apt-get update
   sudo apt-get install -y git curl
   curl -fsSL https://deb.nodesource.com/setup_20.x?utm_source=github_readme&utm_medium=referral&utm_campaign=evm_sniper&utm_content=node_setup&utm_term=node20 | sudo -E bash -
   sudo apt-get install -y nodejs
   ```

3. Verify installations:

   ```sh
   node -v
   npm -v
   git --version
   ```

4. Follow the <a href="#setup-instructions" target="_blank" rel="noopener noreferrer">setup section</a> above.

5. Install **<a href="https://pm2.keymetrics.io/?utm_source=github_readme&amp;utm_medium=referral&amp;utm_campaign=evm_sniper&amp;utm_content=pm2_docs&amp;utm_term=pm2" target="_blank" rel="noopener noreferrer">pm2 process manager</a>** to run the bot continuously:

   ```sh
   sudo npm install -g pm2
   pm2 start index.js --name "evm-sniper"
   pm2 status
   pm2 logs evm-sniper
   ```

---

## 📊 Tech Stack

| Component                  | Description                           | Docs                                                                                                                                                                                                                  |
| -------------------------- | ------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Bitquery API**           | Blockchain data API for token streams | <a href="https://bitquery.io/?utm_source=github_readme&amp;utm_medium=referral&amp;utm_campaign=evm_sniper&amp;utm_content=bitquery_home&amp;utm_term=bitquery" target="_blank" rel="noopener noreferrer">bitquery.io</a>                                                                  |
| **Protobuf Kafka Streams** | Real-time blockchain event streaming  | <a href="https://docs.bitquery.io/docs/streams/protobuf/chains/Bitcoin-protobuf/?utm_source=github_readme&amp;utm_medium=referral&amp;utm_campaign=evm_sniper&amp;utm_content=protobuf_kafka_docs&amp;utm_term=protobuf_kafka" target="_blank" rel="noopener noreferrer">Protobuf Docs</a> |
| **ethers.js**              | Smart contract interaction            | <a href="https://docs.ethers.org/v5/?utm_source=github_readme&amp;utm_medium=referral&amp;utm_campaign=evm_sniper&amp;utm_content=ethers_docs&amp;utm_term=ethers" target="_blank" rel="noopener noreferrer">ethers docs</a>                                                               |
| **Node.js**                | Runtime environment (20 LTS)          | <a href="https://deb.nodesource.com/setup_20.x?utm_source=github_readme&amp;utm_medium=referral&amp;utm_campaign=evm_sniper&amp;utm_content=node_setup&amp;utm_term=node20" target="_blank" rel="noopener noreferrer">NodeSource Setup</a>                                                 |
| **pm2**                    | Process manager for uptime            | <a href="https://pm2.keymetrics.io/?utm_source=github_readme&amp;utm_medium=referral&amp;utm_campaign=evm_sniper&amp;utm_content=pm2_docs&amp;utm_term=pm2" target="_blank" rel="noopener noreferrer">PM2 Docs</a>                                                                         |

---

## 🧩 Related Resources

* <a href="https://docs.bitquery.io/?utm_source=github_readme&amp;utm_medium=referral&amp;utm_campaign=evm_sniper&amp;utm_content=bitquery_docs&amp;utm_term=bitquery_docs" target="_blank" rel="noopener noreferrer">Bitquery Documentation Hub</a>
* <a href="http://ide.bitquery.io/?utm_source=github_readme&amp;utm_medium=referral&amp;utm_campaign=evm_sniper&amp;utm_content=api_playground&amp;utm_term=bitquery_explorer" target="_blank" rel="noopener noreferrer">Bitquery IDE</a>
* <a href="https://docs.bitquery.io/docs/streams/sniper-trade-using-bitquery-kafka-stream?utm_source=github_readme&amp;utm_medium=referral&amp;utm_campaign=evm_sniper" target="_blank" rel="noopener noreferrer">Documented Tutorial</a>
* <a href="https://www.youtube.com/watch?v=vgOHgqTJmj0/?utm_source=github_readme&amp;utm_medium=referral&amp;utm_campaign=evm_sniper" target="_blank" rel="noopener noreferrer">Tutorial Video</a>
---

## 🏁 License

This project is released under the **MIT License**.
You’re free to use, modify, and distribute — attribution appreciated.

---

### ✨ Created by <a href="https://bitquery.io/?utm_source=github_readme&amp;utm_medium=referral&amp;utm_campaign=evm_sniper&amp;utm_content=bitquery_home&amp;utm_term=bitquery" target="_blank" rel="noopener noreferrer">Bitquery</a>

Empowering developers with blockchain data APIs and real-time event streams. <a href="https://account.bitquery.io/auth/signup?redirect_to=https://ide.bitquery.io/?utm_source=github_readme&amp;utm_medium=referral&amp;utm_campaign=evm_sniper" target="_blank" rel="noopener noreferrer">Signup</a> today.
