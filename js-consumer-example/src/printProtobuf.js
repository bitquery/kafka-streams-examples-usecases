"use strict";

const bs58 = require("bs58");

/**
 * Encode protobuf `bytes` fields for display.
 * Solana addresses and signatures are conventionally base58; EVM uses hex (0x-prefixed).
 * @param {Buffer} buffer
 * @param {"base58"|"hex"} encoding
 * @returns {string}
 */
function convertBytes(buffer, encoding = "base58") {
  if (!Buffer.isBuffer(buffer)) {
    return String(buffer);
  }
  if (encoding === "base58") {
    return bs58.encode(buffer);
  }
  return `0x${buffer.toString("hex")}`;
}

/**
 * Recursively print a protobuf-derived plain object (KafkaJS `toObject` with bytes as Buffer).
 * @param {unknown} msg
 * @param {number} indent
 * @param {"base58"|"hex"} encoding
 */
function printProtobufMessage(msg, indent = 0, encoding = "base58") {
  const prefix = " ".repeat(indent);

  if (msg === null || msg === undefined) {
    console.log(`${prefix}${msg}`);
    return;
  }

  if (Array.isArray(msg)) {
    console.log(`${prefix}(array):`);
    msg.forEach((item, idx) => {
      console.log(`${prefix}  [${idx}]:`);
      printProtobufMessage(item, indent + 4, encoding);
    });
    return;
  }

  if (Buffer.isBuffer(msg)) {
    console.log(`${prefix}${convertBytes(msg, encoding)}`);
    return;
  }

  if (typeof msg !== "object") {
    console.log(`${prefix}${msg}`);
    return;
  }

  for (const [key, value] of Object.entries(msg)) {
    if (Array.isArray(value)) {
      if (value.length === 0) continue;
      console.log(`${prefix}${key} (repeated):`);
      value.forEach((item, idx) => {
        console.log(`${prefix}  [${idx}]:`);
        printProtobufMessage(item, indent + 4, encoding);
      });
    } else if (value && typeof value === "object" && Buffer.isBuffer(value)) {
      console.log(`${prefix}${key}: ${convertBytes(value, encoding)}`);
    } else if (value && typeof value === "object") {
      console.log(`${prefix}${key}:`);
      printProtobufMessage(value, indent + 4, encoding);
    } else if (value !== null && value !== undefined) {
      console.log(`${prefix}${key}: ${value}`);
    }
  }
}

module.exports = { convertBytes, printProtobufMessage };
