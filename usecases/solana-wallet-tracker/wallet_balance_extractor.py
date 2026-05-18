import uuid
import base58
import json
import os
from dotenv import load_dotenv
import time
from datetime import datetime
from confluent_kafka import Consumer, KafkaError, KafkaException
from google.protobuf.message import DecodeError
from solana import token_block_message_pb2

class IndexedWalletTracker:
    def __init__(self):
        # Create output directory
        self.output_dir = "wallet_balances"
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        
        # Timestamp for file naming
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # In-memory index of wallets: {address: {token: {balance, decimals, symbol, last_updated}}}
        self.wallet_index = {}
        
        # Token metadata cache: {token_mint: {symbol, decimals, name}}
        self.token_metadata = {}
        
        # Statistics tracking
        self.stats = {
            'messages_processed': 0,
            'balance_updates_received': 0,
            'unique_addresses': 0,
            'unique_tokens': 0,
            'start_time': time.time(),
            'last_export_time': time.time()
        }
        
        # Export settings
        self.export_interval = 30  # seconds between exports
        
        print(f"Initialized IndexedWalletTracker")
        print(f"Balances will be exported to the {self.output_dir} directory")
    
    def convert_bytes(self, value):
        """Convert bytes to base58 string"""
        if isinstance(value, bytes):
            return base58.b58encode(value).decode()
        return str(value)
    
    def extract_token_metadata(self, currency):
        """Extract token metadata from Currency object"""
        token_mint = "UNKNOWN"
        token_symbol = "UNKNOWN"
        token_decimals = 0
        token_name = "UNKNOWN"
        
        if hasattr(currency, 'MintAddress') and currency.MintAddress:
            token_mint = self.convert_bytes(currency.MintAddress)
        
        if hasattr(currency, 'Symbol') and currency.Symbol:
            token_symbol = currency.Symbol
        
        if hasattr(currency, 'Decimals'):
            token_decimals = currency.Decimals
        
        if hasattr(currency, 'Name') and currency.Name:
            token_name = currency.Name
        else:
            token_name = token_symbol
        
        # Update token metadata cache
        if token_mint != "UNKNOWN":
            self.token_metadata[token_mint] = {
                "symbol": token_symbol,
                "decimals": token_decimals,
                "name": token_name
            }
            
            # Update unique tokens count if this is a new token
            if len(self.token_metadata) > self.stats['unique_tokens']:
                self.stats['unique_tokens'] = len(self.token_metadata)
        
        return {
            "mint": token_mint,
            "symbol": token_symbol,
            "decimals": token_decimals,
            "name": token_name
        }
    
    def extract_address(self, tx, account_index):
        """Extract address from transaction using account index"""
        address = None
        
        # Try Header.Accounts
        if hasattr(tx, 'Header') and hasattr(tx.Header, 'Accounts'):
            accounts = tx.Header.Accounts
            if account_index < len(accounts):
                account = accounts[account_index]
                if hasattr(account, 'Address'):
                    address = self.convert_bytes(account.Address)
        
        # Try Accounts directly
        if address is None and hasattr(tx, 'Accounts'):
            accounts = tx.Accounts
            if account_index < len(accounts):
                account = accounts[account_index]
                if hasattr(account, 'Address'):
                    address = self.convert_bytes(account.Address)
        
        return address
    
    def calculate_human_balance(self, raw_balance, decimals):
        """Calculate human-readable balance based on token decimals"""
        if decimals > 0:
            return raw_balance / (10 ** decimals)
        return raw_balance
    
    def update_wallet_balance(self, address, token_mint, token_info, raw_balance):
        """Update the wallet balance index with the latest balance"""
        if address == "UNKNOWN" or address is None:
            return False  # Skip unknown addresses
        
        # Initialize address in index if needed
        if address not in self.wallet_index:
            self.wallet_index[address] = {}
            
            # Update unique addresses count
            self.stats['unique_addresses'] = len(self.wallet_index)
        
        # Calculate human-readable balance
        human_balance = self.calculate_human_balance(raw_balance, token_info['decimals'])
        
        # Update token balance for this address
        self.wallet_index[address][token_mint] = {
            'raw_balance': raw_balance,
            'human_balance': human_balance,
            'symbol': token_info['symbol'],
            'decimals': token_info['decimals'],
            'last_updated': int(time.time())
        }
        
        return True
    
    def process_balance_update(self, address, token_info, raw_balance):
        """Process a balance update and update the index"""
        if address and token_info['mint'] != "UNKNOWN":
            success = self.update_wallet_balance(
                address, 
                token_info['mint'], 
                token_info, 
                raw_balance
            )
            
            if success:
                self.stats['balance_updates_received'] += 1
                return True
        
        return False
    
    def export_balances(self, force=False):
        """Export current balances to file if interval has passed or forced"""
        current_time = time.time()
        elapsed = current_time - self.stats['last_export_time']
        
        if force or elapsed >= self.export_interval:
            # Create export file with timestamp
            export_file = os.path.join(self.output_dir, f"balances_{self.timestamp}_latest.json")
            
            # Prepare export data
            export_data = {
                'timestamp': datetime.now().isoformat(),
                'stats': self.stats.copy(),
                'wallets': self.wallet_index
            }
            
            # Add elapsed time and rate stats
            total_elapsed = current_time - self.stats['start_time']
            export_data['stats']['elapsed_seconds'] = total_elapsed
            export_data['stats']['updates_per_second'] = (
                self.stats['balance_updates_received'] / total_elapsed 
                if total_elapsed > 0 else 0
            )
            
            # Write to file
            with open(export_file, 'w') as f:
                json.dump(export_data, f, indent=2)
            
            # Also create a CSV version for easy viewing
            csv_file = os.path.join(self.output_dir, f"balances_{self.timestamp}_latest.csv")
            with open(csv_file, 'w') as f:
                # Write header
                f.write("Address,Token,Symbol,HumanReadableBalance,RawBalance,Decimals,LastUpdated\n")
                
                # Write each wallet balance
                for address, tokens in self.wallet_index.items():
                    for token_mint, data in tokens.items():
                        f.write(f"{address},{token_mint},{data['symbol']}," +
                                f"{data['human_balance']},{data['raw_balance']}," +
                                f"{data['decimals']},{data['last_updated']}\n")
            
            # Update last export time
            self.stats['last_export_time'] = current_time
            
            print(f"Exported {len(self.wallet_index)} wallets with " +
                  f"{self.stats['balance_updates_received']} balance records to {export_file}")
            print(f"CSV export available at: {csv_file}")
            
            return True
        
        return False
    
    def process_message(self, token_block):
        """Process a token block message"""
        self.stats['messages_processed'] += 1
        
        # Process balance updates at block level
        if hasattr(token_block, 'BalanceUpdates'):
            for update in token_block.BalanceUpdates:
                if hasattr(update, 'BalanceUpdate') and hasattr(update, 'Currency'):
                    balance_update = update.BalanceUpdate
                    currency = update.Currency
                    
                    # Extract token metadata
                    token_info = self.extract_token_metadata(currency)
                    
                    # Extract balance and account
                    if hasattr(balance_update, 'PostBalance') and hasattr(balance_update, 'AccountIndex'):
                        post_balance = balance_update.PostBalance
                        account_index = balance_update.AccountIndex
                        
                        # For block level updates, we often can't resolve the address
                        # But we can try to look in transactions
                        address = None
                        if hasattr(token_block, 'Transactions'):
                            for tx in token_block.Transactions:
                                addr = self.extract_address(tx, account_index)
                                if addr:
                                    address = addr
                                    break
                        
                        if address:
                            self.process_balance_update(address, token_info, post_balance)
        
        # Process transactions
        if hasattr(token_block, 'Transactions'):
            for tx in token_block.Transactions:
                # Process transfers - these usually have the best address information
                if hasattr(tx, 'Transfers'):
                    for transfer in tx.Transfers:
                        # Extract token metadata
                        token_info = {"mint": "UNKNOWN", "symbol": "UNKNOWN", "decimals": 0, "name": "UNKNOWN"}
                        if hasattr(transfer, 'Currency'):
                            token_info = self.extract_token_metadata(transfer.Currency)
                        
                        # Get sender and receiver addresses
                        sender_address = None
                        if hasattr(transfer, 'Sender') and hasattr(transfer.Sender, 'Address'):
                            sender_address = self.convert_bytes(transfer.Sender.Address)
                        
                        receiver_address = None
                        if hasattr(transfer, 'Receiver') and hasattr(transfer.Receiver, 'Address'):
                            receiver_address = self.convert_bytes(transfer.Receiver.Address)
                        
                        # Process balance updates in instruction
                        if hasattr(transfer, 'Instruction') and hasattr(transfer.Instruction, 'TokenBalanceUpdates'):
                            for balance_update in transfer.Instruction.TokenBalanceUpdates:
                                if hasattr(balance_update, 'PostBalance') and hasattr(balance_update, 'AccountIndex'):
                                    post_balance = balance_update.PostBalance
                                    account_index = balance_update.AccountIndex
                                    
                                    # Determine address based on account index
                                    address = None
                                    if account_index == 0 and sender_address:
                                        address = sender_address
                                    elif account_index == 2 and receiver_address:
                                        address = receiver_address
                                    
                                    if address:
                                        self.process_balance_update(address, token_info, post_balance)
                
                # Process transaction-level balance updates
                if hasattr(tx, 'BalanceUpdates'):
                    for update in tx.BalanceUpdates:
                        if hasattr(update, 'BalanceUpdate') and hasattr(update, 'Currency'):
                            balance_update = update.BalanceUpdate
                            currency = update.Currency
                            
                            # Extract token metadata
                            token_info = self.extract_token_metadata(currency)
                            
                            # Extract balance and account
                            if hasattr(balance_update, 'PostBalance') and hasattr(balance_update, 'AccountIndex'):
                                post_balance = balance_update.PostBalance
                                account_index = balance_update.AccountIndex
                                
                                # Get address from transaction
                                address = self.extract_address(tx, account_index)
                                
                                if address:
                                    self.process_balance_update(address, token_info, post_balance)
                
                # Process token balance updates
                if hasattr(tx, 'TokenBalanceUpdates'):
                    for update in tx.TokenBalanceUpdates:
                        if hasattr(update, 'PostBalance') and hasattr(update, 'AccountIndex'):
                            post_balance = update.PostBalance
                            account_index = update.AccountIndex
                            
                            # Get address from transaction
                            address = self.extract_address(tx, account_index)
                            
                            # Get token info from account
                            token_info = {"mint": "UNKNOWN", "symbol": "UNKNOWN", "decimals": 0, "name": "UNKNOWN"}
                            
                            if hasattr(tx, 'Accounts') or (hasattr(tx, 'Header') and hasattr(tx.Header, 'Accounts')):
                                accounts = tx.Accounts if hasattr(tx, 'Accounts') else tx.Header.Accounts
                                if account_index < len(accounts):
                                    account = accounts[account_index]
                                    if hasattr(account, 'Token'):
                                        token = account.Token
                                        if hasattr(token, 'Mint'):
                                            token_info["mint"] = self.convert_bytes(token.Mint)
                                        if hasattr(token, 'Decimals'):
                                            token_info["decimals"] = token.Decimals
                            
                            # Use token metadata from cache if available
                            if token_info["mint"] != "UNKNOWN" and token_info["mint"] in self.token_metadata:
                                cached_data = self.token_metadata[token_info["mint"]]
                                token_info["symbol"] = cached_data["symbol"]
                                token_info["name"] = cached_data["name"]
                            
                            if address:
                                self.process_balance_update(address, token_info, post_balance)
        
        # Export balances if it's time
        self.export_balances()
        
        # Periodically print stats
        if self.stats['messages_processed'] % 10 == 0:
            self.print_stats()
    
    def print_stats(self):
        """Print tracker statistics"""
        elapsed = time.time() - self.stats['start_time']
        updates_per_sec = (
            self.stats['balance_updates_received'] / elapsed 
            if elapsed > 0 else 0
        )
        
        print(f"\n--- Wallet Balance Tracker Stats ---")
        print(f"Runtime: {elapsed:.2f} seconds")
        print(f"Messages processed: {self.stats['messages_processed']}")
        print(f"Balance updates received: {self.stats['balance_updates_received']} ({updates_per_sec:.2f}/sec)")
        print(f"Unique addresses tracked: {self.stats['unique_addresses']}")
        print(f"Unique tokens tracked: {self.stats['unique_tokens']}")

def run_consumer():
    """Run the Kafka consumer with the indexed wallet tracker"""
    # Load environment variables from .env file
    load_dotenv()
    
    # Get credentials from environment variables
    kafka_username = os.getenv("KAFKA_USERNAME")
    kafka_password = os.getenv("KAFKA_PASSWORD")
    # Kafka configuration
    group_id_suffix = uuid.uuid4().hex
    conf = {
        'bootstrap.servers': 'rpk0.bitquery.io:9092,rpk1.bitquery.io:9092,rpk2.bitquery.io:9092',
        'group.id': f'{kafka_username}-group-{group_id_suffix}',
        'session.timeout.ms': 30000,
        'security.protocol': 'SASL_PLAINTEXT',
        'ssl.endpoint.identification.algorithm': 'none',
        'sasl.mechanisms': 'SCRAM-SHA-512',
        'sasl.username': kafka_username,
        'sasl.password': kafka_password,
        'auto.offset.reset': 'latest',
    }
    
    # Initialize consumer
    consumer = Consumer(conf)
    topic = 'solana.tokens.proto'
    consumer.subscribe([topic])
    
    # Initialize wallet tracker
    tracker = IndexedWalletTracker()
    
    print(f"Starting indexed wallet balance tracker on topic: {topic}")
    print("Press Ctrl+C to stop...")
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())
            
            try:
                # Parse the message
                buffer = msg.value()
                token_block = token_block_message_pb2.TokenBlockMessage()
                token_block.ParseFromString(buffer)
                
                # Process the message
                tracker.process_message(token_block)
                
            except DecodeError as err:
                print(f"Protobuf decoding error: {err}")
            except Exception as err:
                print(f"Error processing message: {err}")
                import traceback
                traceback.print_exc()
    
    except KeyboardInterrupt:
        print("\nStopping wallet balance tracker...")
    
    finally:
        # Export final balances
        tracker.export_balances(force=True)
        consumer.close()
        print("Consumer closed.")

if __name__ == "__main__":
    run_consumer()