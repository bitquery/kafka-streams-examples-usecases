"use strict";

function requiredEnv(name) {
  const value = process.env[name];
  if (!value || String(value).trim() === "") {
    throw new Error(
      `Missing required environment variable "${name}". Copy .env.example to .env and set it.`
    );
  }
  return String(value).trim();
}

function optionalEnv(name, defaultValue) {
  const value = process.env[name];
  if (value === undefined || String(value).trim() === "") {
    return defaultValue;
  }
  return String(value).trim();
}

function parseBool(name, defaultValue) {
  const raw = process.env[name];
  if (raw === undefined || String(raw).trim() === "") {
    return defaultValue;
  }
  return ["1", "true", "yes", "on"].includes(String(raw).trim().toLowerCase());
}

function loadConfig() {
  const username = requiredEnv("KAFKA_USERNAME");
  const password = requiredEnv("KAFKA_PASSWORD");

  const topic = optionalEnv("KAFKA_TOPIC", "solana.transactions.proto");
  const bootstrapServers = optionalEnv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "rpk0.bitquery.io:9092,rpk1.bitquery.io:9092,rpk2.bitquery.io:9092"
  );
  const brokers = bootstrapServers.split(",").map((b) => b.trim()).filter(Boolean);

  const clientId = optionalEnv("KAFKA_CLIENT_ID", username);

  const explicitGroupId = process.env.KAFKA_GROUP_ID;
  const groupId =
    explicitGroupId && String(explicitGroupId).trim() !== ""
      ? String(explicitGroupId).trim()
      : null;

  const fromBeginning = parseBool("KAFKA_FROM_BEGINNING", false);

  return {
    username,
    password,
    topic,
    brokers,
    clientId,
    groupId,
    fromBeginning,
  };
}

module.exports = { loadConfig };
