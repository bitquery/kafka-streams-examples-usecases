import uuid
import base58
import threading
import signal
import time
import logging
import datetime

from confluent_kafka import Consumer, KafkaError, KafkaException
from google.protobuf.message import DecodeError

from solana import parsed_idl_block_message_pb2

import config

# =========================================================
# KAFKA CONFIG
# =========================================================
group_id_suffix = uuid.uuid4().hex

base_conf = {
    "bootstrap.servers": "rpk0.bitquery.io:9092,rpk1.bitquery.io:9092,rpk2.bitquery.io:9092",
    "session.timeout.ms": 10000,
    "heartbeat.interval.ms": 3000,
    "security.protocol": "SASL_PLAINTEXT",
    "ssl.endpoint.identification.algorithm": "none",
    "sasl.mechanisms": "SCRAM-SHA-512",
    "sasl.username": config.solana_username,
    "sasl.password": config.solana_password,
    "enable.auto.commit": False,
    "auto.offset.reset": "latest",
}


topic = 'solana.transactions.proto'
# schema: https://github.com/bitquery/streaming_protobuf/blob/main/solana/parsed_idl_block_message.proto
# pb2 schema as a pypi package: https://pypi.org/project/bitquery-pb2-kafka-package/
# Control flag for graceful shutdown
shutdown_event = threading.Event()
processed_count = 0
processed_count_lock = threading.Lock()

# =========================================================
# LOGGING
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# =========================================================
# HELPERS
# =========================================================
def contains_swap(tx):
    if not hasattr(tx, 'ParsedIdlInstructions'):
        return False
    for instruction in tx.ParsedIdlInstructions:
        prog = getattr(instruction, 'Program', None)
        if prog:
            if getattr(prog, 'Name', '').lower().find('swap') >= 0:
                return True
            if getattr(prog, 'Method', '').lower().find('swap') >= 0:
                return True
        for log in getattr(instruction, 'Logs', []):
            if 'swap' in log.lower():
                return True
    return False

def contains_transfer(tx):
    """Check if transaction contains 'transfer' or 'transfer_checked' in program names or methods"""
    if not hasattr(tx, 'ParsedIdlInstructions'):
        return False
    for instruction in tx.ParsedIdlInstructions:
        prog = getattr(instruction, 'Program', None)
        if prog:
            name = getattr(prog, 'Name', '').lower()
            method = getattr(prog, 'Method', '').lower()
            if 'transfer' in name or 'transfer_checked' in name:
                return True
            if 'transfer' in method or 'transfer_checked' in method:
                return True
    return False

def has_multiple_transfers(tx):
    """Check if transaction has multiple explicit transfer instructions"""
    if not hasattr(tx, 'ParsedIdlInstructions'):
        return False
    
    transfer_count = 0
    
    # Count explicit transfer instructions by program name/method
    for instruction in tx.ParsedIdlInstructions:
        program = getattr(instruction, 'Program', None)
        if not program:
            continue
        name = getattr(program, 'Name', '') or ''
        method = getattr(program, 'Method', '') or ''
        lower_name = name.lower()
        lower_method = method.lower()
        if 'transfer' in lower_name or 'transfer_checked' in lower_name:
            transfer_count += 1
        elif 'transfer' in lower_method or 'transfer_checked' in lower_method:
            transfer_count += 1
    
    # Consider it multiple transfers if there are 2 or more transfer instructions
    return transfer_count >= 2

HUMODIFI_PROGRAM_IDS = {
    "9H6tua7jkLhdm3w8BvgpTn5LZNU7g4ZynDmCiNN3q6Rp",
}

def normalize_address(value):
    """Return a base58 string for common address representations"""
    if isinstance(value, (bytes, bytearray)):
        try:
            return base58.b58encode(value).decode()
        except Exception:
            return None
    if isinstance(value, str):
        return value
    return None

def contains_humodifi(tx):
    """Detect HumodiFi interactions via program name/method or known program ids"""
    if not hasattr(tx, 'ParsedIdlInstructions') or not tx.ParsedIdlInstructions:
        return False

    for instruction in tx.ParsedIdlInstructions:
        program = getattr(instruction, 'Program', None)
        if program:
            program_name = getattr(program, 'Name', None)
            if program_name and 'humidifi' in program_name.lower():
                return True

            program_method = getattr(program, 'Method', None)
            if program_method and 'humidifi' in program_method.lower():
                return True

            for attr in ('ProgramId', 'Address', 'Id'):
                if hasattr(program, attr):
                    addr = normalize_address(getattr(program, attr))
                    if addr and addr in HUMODIFI_PROGRAM_IDS:
                        return True

        if hasattr(instruction, 'Accounts') and instruction.Accounts:
            for account in instruction.Accounts:
                for attr in ('Address', 'Owner', 'Pubkey'):
                    if hasattr(account, attr):
                        addr = normalize_address(getattr(account, attr))
                        if addr and addr in HUMODIFI_PROGRAM_IDS:
                            return True

    return False

def contains_vote(tx):
    """Check if transaction contains 'vote' in program names, methods, or logs"""
    if not hasattr(tx, 'ParsedIdlInstructions'):
        return False
    
    for instruction in tx.ParsedIdlInstructions:
        # Check program name
        if hasattr(instruction, 'Program') and instruction.Program:
            if hasattr(instruction.Program, 'Name') and instruction.Program.Name:
                if 'vote' in instruction.Program.Name.lower():
                    return True
            if hasattr(instruction.Program, 'Method') and instruction.Program.Method:
                if 'vote' in instruction.Program.Method.lower():
                    return True
        
        # Check logs
        if hasattr(instruction, 'Logs') and instruction.Logs:
            for log in instruction.Logs:
                if 'vote' in log.lower():
                    return True
    
    return False

def contains_burn(tx):
    """Check if transaction contains 'burn' in program names, methods, or logs"""
    if not hasattr(tx, 'ParsedIdlInstructions'):
        return False
    
    for instruction in tx.ParsedIdlInstructions:
        # Check program name
        if hasattr(instruction, 'Program') and instruction.Program:
            if hasattr(instruction.Program, 'Name') and instruction.Program.Name:
                if 'burn' in instruction.Program.Name.lower():
                    return True
            if hasattr(instruction.Program, 'Method') and instruction.Program.Method:
                if 'burn' in instruction.Program.Method.lower():
                    return True
        
        # Check logs
        if hasattr(instruction, 'Logs') and instruction.Logs:
            for log in instruction.Logs:
                if 'burn' in log.lower():
                    return True
    
    return False


# =========================================================
# PROCESS MESSAGE
# =========================================================
def process_message(buffer, receive_time):
    global processed_count

    try:
        tx_block = parsed_idl_block_message_pb2.ParsedIdlBlockMessage()
        tx_block.ParseFromString(buffer)

        current_time = datetime.datetime.fromtimestamp(time.time(), datetime.timezone.utc)
        latency = (time.time() - receive_time) * 1000  # Convert to ms

        logger.info(
            "Block %s | Received Time %s | Txs %s | Latency %d ms",
            tx_block.Header.Slot,
            current_time,
            len(tx_block.Transactions),
            latency,
        )

        for tx in tx_block.Transactions:
            sig = base58.b58encode(tx.Signature).decode()
            labels = []

            if contains_swap(tx):
                labels.append("SWAP")
            if has_multiple_transfers(tx):
                labels.append("MULTIPLE_TRANSFERS")
            if contains_vote(tx):
                labels.append("VOTE")
            if contains_humodifi(tx):
                labels.append("UNKNOWN")
            if contains_burn(tx):
                labels.append("BURN")

            if labels:
                label_str = " ".join(f"[{label}]" for label in labels)
                logger.info("  Transaction: %s %s", sig, label_str)
            elif contains_transfer(tx):
                logger.info("  Transaction: %s [NORMAL_TRANSFER]", sig)
            else:
                logger.debug("  Transaction: %s", sig)

        with processed_count_lock:
            processed_count += 1

    except DecodeError as e:
        logger.error(f"Decode error: {e}")

# =========================================================
# CONSUMER WORKER
# =========================================================

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    logger.info(f"Received signal {signum}, initiating shutdown...")
    shutdown_event.set()


def consumer_worker():
    """Single consumer thread"""
    global processed_count

    conf = base_conf.copy()
    conf["group.id"] = f"{config.solana_username}-group-{group_id_suffix}"

    consumer = Consumer(conf)
    consumer.subscribe([topic])

    logger.info("Consumer started")

    try:
        while not shutdown_event.is_set():
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                # Ignore end-of-partition notifications
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())

            try:
                receive_time = time.time()
                process_message(msg.value(), receive_time)
            except Exception as err:
                logger.exception(f"Failed to process message: {err}")

    except KeyboardInterrupt:
        logger.info("Consumer stopping...")
    except Exception as e:
        logger.exception(f"Consumer error: {e}")
    finally:
        consumer.close()
        logger.info("Consumer closed")

# --- Main execution --- #

def main():
    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Start single consumer thread
    thread = threading.Thread(target=consumer_worker, daemon=True)
    thread.start()
    logger.info("Consumer thread started")

    try:
        # Keep the main thread alive
        while not shutdown_event.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, stopping consumer...")
        shutdown_event.set()
    finally:
        # Wait for consumer thread to finish
        logger.info("Waiting for consumer to finish...")
        thread.join(timeout=5)
        logger.info(f"Shutdown complete. Total messages processed: {processed_count}")

if __name__ == "__main__":
    main()