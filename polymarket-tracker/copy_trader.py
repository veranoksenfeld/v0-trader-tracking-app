"""
Polymarket Copy Trader - Executes copy trades using the CLOB API
"""
import sqlite3
import time
import json
import os
from datetime import datetime
import schedule

DATABASE = 'polymarket_trades.db'
CONFIG_FILE = 'config.json'

# Try to import py-clob-client
try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import MarketOrderArgs, OrderArgs, OrderType
    from py_clob_client.order_builder.constants import BUY, SELL
    CLOB_AVAILABLE = True
except ImportError:
    print("WARNING: py-clob-client not installed. Run: pip install py-clob-client")
    CLOB_AVAILABLE = False

HOST = "https://clob.polymarket.com"
CHAIN_ID = 137  # Polygon mainnet


def get_db():
    """Get database connection"""
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn


def load_config():
    """Load configuration from file"""
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r') as f:
            return json.load(f)
    return {}


def save_config(config):
    """Save configuration to file"""
    with open(CONFIG_FILE, 'w') as f:
        json.dump(config, f, indent=2)


def get_clob_client(config):
    """Initialize the Polymarket CLOB client"""
    if not CLOB_AVAILABLE:
        return None
    
    private_key = config.get('private_key')
    funder_address = config.get('funder_address')
    signature_type = config.get('signature_type')
    if signature_type is None:
        signature_type = 1
    signature_type = int(signature_type)  # 0=EOA, 1=email/Magic, 2=browser wallet proxy
    
    if not private_key or not funder_address:
        print("ERROR: Missing private_key or funder_address in config")
        return None
    
    try:
        client = ClobClient(
            HOST,
            key=private_key,
            chain_id=CHAIN_ID,
            signature_type=signature_type,
            funder=funder_address
        )
        # Derive API credentials for authenticated endpoints
        client.set_api_creds(client.create_or_derive_api_creds())
        print(f"[{datetime.now()}] CLOB client initialized successfully")
        return client
    except Exception as e:
        print(f"ERROR: Failed to initialize CLOB client: {e}")
        return None


def check_and_print_balance(client):
    """Check and print the wallet balance"""
    try:
        balance_info = client.get_balance_allowance()
        
        if isinstance(balance_info, dict):
            raw_balance = balance_info.get('balance', 0)
            raw_allowance = balance_info.get('allowance', 0)
            usdc_balance = float(raw_balance) / 1e6
            usdc_allowance = float(raw_allowance) / 1e6
            print(f"  USDC Balance:   ${usdc_balance:,.2f}")
            print(f"  USDC Allowance: ${usdc_allowance:,.2f}")
            return usdc_balance
        else:
            print(f"  Balance info: {balance_info}")
            return 0
    except Exception as e:
        print(f"  Could not fetch balance: {e}")
        return 0


def execute_copy_trade(client, trade, config):
    """Execute a copy trade based on a tracked trader's trade"""
    try:
        # Get copy trading settings
        copy_percentage = config.get('copy_percentage', 10)  # Default 10% of original size
        max_trade_size = config.get('max_trade_size', 100)  # Max $100 per trade
        
        # Calculate our trade size in USDC
        original_size = float(trade['usdc_size'] or 0)
        our_size = min(original_size * (copy_percentage / 100), max_trade_size)
        
        if our_size < 1:  # Minimum trade size
            print(f"Trade size too small: ${our_size:.2f}")
            return False, None
        
        # Determine side
        side = BUY if trade['side'] == 'BUY' else SELL
        
        # Get token ID from the trade
        token_id = trade.get('asset')
        if not token_id:
            print("ERROR: No token ID in trade data")
            return False, None
        
        price = float(trade['price'])
        
        # Use Market Order (FOK - Fill or Kill) for quick execution
        # This buys $our_size worth at market price
        market_order = MarketOrderArgs(
            token_id=token_id,
            amount=our_size,  # USDC amount to spend/receive
            side=side,
            order_type=OrderType.FOK  # Fill or Kill - immediate execution
        )
        
        signed_order = client.create_market_order(market_order)
        resp = client.post_order(signed_order, OrderType.FOK)
        
        print(f"[{datetime.now()}] Copy trade executed: {trade['side']} ${our_size:.2f} on {trade['title']}")
        print(f"  Token: {token_id[:20]}...")
        print(f"  Response: {resp}")
        
        return True, resp
        
    except Exception as e:
        print(f"ERROR executing copy trade: {e}")
        import traceback
        traceback.print_exc()
        return False, str(e)


def check_and_copy_trades():
    """Check for new trades from copy-enabled traders and execute copies"""
    config = load_config()
    
    if not config.get('copy_trading_enabled'):
        return
    
    if not CLOB_AVAILABLE:
        print("Copy trading unavailable - py-clob-client not installed")
        return
    
    conn = get_db()
    cursor = conn.cursor()
    
    # Get last processed trade timestamp
    last_processed = config.get('last_processed_timestamp', 0)
    
    # Get new trades from copy-enabled traders
    cursor.execute('''
        SELECT t.*, tr.wallet_address, tr.name
        FROM trades t
        JOIN traders tr ON t.trader_id = tr.id
        WHERE tr.copy_trading_enabled = 1
        AND t.timestamp > ?
        ORDER BY t.timestamp ASC
    ''', (last_processed,))
    
    new_trades = cursor.fetchall()
    
    if not new_trades:
        conn.close()
        return
    
    print(f"[{datetime.now()}] Found {len(new_trades)} new trades to copy")
    
    # Initialize CLOB client
    client = get_clob_client(config)
    if not client:
        print("ERROR: Could not initialize CLOB client")
        conn.close()
        return
    
    # Check balance before executing trades
    balance = check_and_print_balance(client)
    if balance < 1:
        print("WARNING: Insufficient USDC balance to copy trades. Skipping.")
        conn.close()
        return
    
    # Execute copy trades
    for trade in new_trades:
        trade_dict = dict(trade)
        
        # Check if we should skip this trade
        min_trade_size = config.get('min_trade_size', 10)
        if float(trade_dict.get('usdc_size', 0)) < min_trade_size:
            print(f"Skipping small trade: ${trade_dict.get('usdc_size', 0):.2f}")
            continue
        
        # Execute the copy trade
        success, response = execute_copy_trade(client, trade_dict, config)
        
        if success:
            # Log the copy trade
            cursor.execute('''
                INSERT INTO copy_trades (original_trade_id, status, executed_at, response)
                VALUES (?, ?, ?, ?)
            ''', (trade_dict['id'], 'SUCCESS', datetime.now().isoformat(), str(response)))
        else:
            # Log failed trade
            cursor.execute('''
                INSERT INTO copy_trades (original_trade_id, status, executed_at, response)
                VALUES (?, ?, ?, ?)
            ''', (trade_dict['id'], 'FAILED', datetime.now().isoformat(), str(response)))
        
        # Update last processed timestamp
        config['last_processed_timestamp'] = trade_dict['timestamp']
        save_config(config)
        
        # Delay between trades
        time.sleep(1)
    
    conn.commit()
    conn.close()


def init_copy_trades_table():
    """Initialize the copy trades tracking table and migrate traders table"""
    conn = get_db()
    cursor = conn.cursor()
    
    # Create copy_trades table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS copy_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            original_trade_id INTEGER,
            status TEXT,
            executed_at TEXT,
            our_size REAL,
            our_price REAL,
            response TEXT,
            FOREIGN KEY (original_trade_id) REFERENCES trades(id)
        )
    ''')
    
    # Add copy_trading_enabled column to traders if it doesn't exist
    cursor.execute("PRAGMA table_info(traders)")
    columns = [col[1] for col in cursor.fetchall()]
    
    if 'copy_trading_enabled' not in columns:
        print("Adding copy_trading_enabled column to traders table...")
        cursor.execute('ALTER TABLE traders ADD COLUMN copy_trading_enabled INTEGER DEFAULT 0')
    
    if 'total_profit' not in columns:
        cursor.execute('ALTER TABLE traders ADD COLUMN total_profit REAL DEFAULT 0')
    
    if 'win_rate' not in columns:
        cursor.execute('ALTER TABLE traders ADD COLUMN win_rate REAL DEFAULT 0')
    
    if 'total_trades' not in columns:
        cursor.execute('ALTER TABLE traders ADD COLUMN total_trades INTEGER DEFAULT 0')
    
    if 'volume_traded' not in columns:
        cursor.execute('ALTER TABLE traders ADD COLUMN volume_traded REAL DEFAULT 0')
    
    conn.commit()
    conn.close()
    print("Database migration complete.")


def run_copy_trader():
    """Run the copy trader on a schedule"""
    print("=" * 60)
    print("Polymarket Copy Trader Started")
    print("=" * 60)
    
    # Initialize database table
    init_copy_trades_table()
    
    # Load and validate config
    config = load_config()
    
    if not config.get('private_key'):
        print("\nWARNING: No private key configured!")
        print("Copy trading will not execute until you configure your credentials.")
        print("Use the web interface at http://localhost:5000 to configure.")
    else:
        # Show balance on startup
        print("\nConnecting to Polymarket CLOB API...")
        client = get_clob_client(config)
        if client:
            print("Wallet balance:")
            balance = check_and_print_balance(client)
            if balance < 1:
                print("\n  WARNING: Low USDC balance. Deposit USDC to your funder address to copy trade.")
        else:
            print("ERROR: Could not connect to CLOB API. Check your credentials.")
    
    print(f"\n[{datetime.now()}] Copy trader running. Checking every 30 seconds.")
    print("Press Ctrl+C to stop.\n")
    
    # Run immediately
    check_and_copy_trades()
    
    # Schedule to run every 30 seconds
    schedule.every(30).seconds.do(check_and_copy_trades)
    
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    run_copy_trader()
