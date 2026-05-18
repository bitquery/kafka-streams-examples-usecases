/**
 * Bitquery Kafka consumer sample for Node.js (not for browsers).
 * Run with: `npm start` → spawns `node src/index.js`
 */
"use strict";

require("dotenv").config();

const { Kafka, CompressionTypes, CompressionCodecs } = require("kafkajs");
const LZ4 = require("kafkajs-lz4");
const { v4: uuidv4 } = require("uuid");
const { loadProto } = require("bitquery-protobuf-schema");

const { loadConfig } = require("./config");
const { printProtobufMessage } = require("./printProtobuf");

CompressionCodecs[CompressionTypes.LZ4] = new LZ4().codec;

function createKafka({ clientId, brokers, username, password }) {
  return new Kafka({
    clientId,
    brokers,
    ssl: false,
    sasl: {
      mechanism: "scram-sha-512",
      username,
      password,
    },
    connectionTimeout: 10_000,
    requestTimeout: 60_000,
  });
}

async function main() {
  const cfg = loadConfig();
  const kafka = createKafka({
    clientId: cfg.clientId,
    brokers: cfg.brokers,
    username: cfg.username,
    password: cfg.password,
  });

  const groupId =
    cfg.groupId ?? `${cfg.username}-group-${uuidv4().replace(/-/g, "")}`;

  const consumer = kafka.consumer({
    groupId,
    sessionTimeout: 30_000,
    heartbeatInterval: 3_000,
  });

  const ParsedIdlBlockMessage = await loadProto(cfg.topic);

  const shutdown = async (signal) => {
    console.error(`\nReceived ${signal}; disconnecting consumer…`);
    try {
      await consumer.disconnect();
    } catch (err) {
      console.error("Error during consumer.disconnect():", err);
    } finally {
      process.exit(0);
    }
  };

  process.once("SIGINT", () => void shutdown("SIGINT"));
  process.once("SIGTERM", () => void shutdown("SIGTERM"));

  await consumer.connect();
  await consumer.subscribe({
    topic: cfg.topic,
    fromBeginning: cfg.fromBeginning,
  });

  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ message }) => {
      try {
        const buffer = message.value;
        if (!buffer) {
          return;
        }

        const decoded = ParsedIdlBlockMessage.decode(buffer);
        const msgObj = ParsedIdlBlockMessage.toObject(decoded, {
          longs: String,
          enums: String,
          bytes: Buffer,
        });

        printProtobufMessage(msgObj, 0, "base58");
      } catch (err) {
        console.error("Error decoding/processing message:", err);
      }
    },
  });
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
