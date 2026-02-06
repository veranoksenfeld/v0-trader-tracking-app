"""
Polymarket Copy Trader - Trade execution module
Used by app.py's built-in copy trading engine.
Can also be run standalone for testing.
"""
import sqlite3
import time
import json
import os
from datetime import datetime

DATABASE = 'polymarket_trades.db'
CONFIG_FILE = 'config.json'


def log_event(level, message, details=''):
    """Write to unified_log table so the UI can display copy_trader events"""
    try:
        conn = sqlite3.connect(DATABASE, timeout=60)
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA busy_timeout=60000')
        conn.execute('PRAGMA synchronous=NORMAL')
        conn.row_factory = sqlite3.Row
        try:
            cursor = conn.cursor()
            cursor.execute(
                'INSERT INTO unified_log (timestamp, source, level, message, details) VALUES (?, ?, ?, ?, ?)',
                (datetime.now().isoformat(), 'TRADER', level, message, details)
            )
            conn.commit()
        finally:
            conn.close()
    except Exception:
        pass


# Try to import py-clob-client
try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import MarketOrderArgs, OrderType
    from py_clob_client.order_builder.constants import BUY, SELL
    CLOB_AVAILABLE = True
except ImportError:
    print("WARNING: py-clob-client not installed. Run: pip install py-clob-client")
    CLOB_AVAILABLE = False

HOST = "https://clob.polymarket.com"
CHAIN_ID = 137


def get_db():
    conn = sqlite3.connect(DATABASE, timeout=60, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA busy_timeout=60000')
    conn.execute('PRAGMA synchronous=NORMAL')
    return conn


def load_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r') as f:
            return json.load(f)
    return {}


def save_config(config):
    with open(CONFIG_FILE, 'w') as f:
        json.dump(config, f, indent=2)


def get_clob_client(config):
    """Initialize the Polymarket CLOB client"""
    if not CLOB_AVAILABLE:
        return None
    private_key = config.get('private_key')
    funder_address = config.get('funder_address')
    sig_type = config.get('signature_type')
    if sig_type is None:
        sig_type = 1
    sig_type = int(sig_type)
    if not private_key or not funder_address:
        print("ERROR: Missing private_key or funder_address in config")
        return None
    try:
        client = ClobClient(HOST, key=private_key, chain_id=CHAIN_ID, signature_type=sig_type, funder=funder_address)
        creds = client.create_or_derive_api_creds()
        if creds is None:
            print("ERROR: create_or_derive_api_creds() returned None.")
            return None
        client.set_api_creds(creds)
        print(f"[{datetime.now()}] CLOB client initialized successfully")
        log_event('INFO', 'CLOB client initialized')
        return client
    except Exception as e:
        print(f"ERROR: Failed to initialize CLOB client: {e}")
        log_event('ERROR', 'CLOB client init failed', str(e))
        return None


def get_usdc_balance(funder_address):
    """Get USDC.e balance via Polygon RPC"""
    import requests as req
    USDC_E = '0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174'
    padded = funder_address.replace('0x', '').lower().zfill(64)
    call_data = '0x70a08231' + padded
    for rpc in ['https://polygon-rpc.com', 'https://rpc.ankr.com/polygon', 'https://polygon.llamarpc.com']:
        try:
            resp = req.post(rpc, json={'jsonrpc': '2.0', 'method': 'eth_call', 'params': [{'to': USDC_E, 'data': call_data}, 'latest'], 'id': 1}, timeout=10)
            result = resp.json()
            if 'result' in result and result['result'] != '0x':
                return int(result['result'], 16) / 1e6
        except Exception:
            continue
    return 0


def execute_copy_trade(client, trade, config):
    """Execute a copy trade via CLOB API"""
    try:
        copy_pct = config.get('copy_percentage', 10)
        max_size = config.get('max_trade_size', 100)
        original_size = float(trade.get('usdc_size') or 0)
        our_size = min(original_size * (copy_pct / 100), max_size)
        if our_size < 1:
            return False, 'Size too small', 0

        side = BUY if (trade.get('side') or '').upper() == 'BUY' else SELL
        token_id = trade.get('asset')
        if not token_id:
            return False, 'No token ID', 0

        market_order = MarketOrderArgs(token_id=token_id, amount=our_size, side=side, order_type=OrderType.FOK)
        signed_order = client.create_market_order(market_order)
        resp = client.post_order(signed_order, OrderType.FOK)

        print(f"[{datetime.now()}] Copy trade: {trade['side']} ${our_size:.2f} on {trade.get('title', '?')}")
        log_event('SUCCESS', f'Copy trade: {trade["side"]} ${our_size:.2f} on {trade.get("title", "?")[:50]}', str(resp)[:200])
        return True, str(resp), our_size
    except Exception as e:
        print(f"ERROR executing copy trade: {e}")
        log_event('FAILED', 'Copy trade failed', str(e))
        return False, str(e), 0


def execute_mock_trade(trade, config):
    """Simulate a copy trade for testing"""
    import random
    copy_pct = config.get('copy_percentage', 10)
    max_size = config.get('max_trade_size', 100)
    original_size = float(trade.get('usdc_size') or 0)
    our_size = min(original_size * (copy_pct / 100), max_size)
    if our_size < 0.01:
        our_size = round(random.uniform(1, 10), 2)

    if random.random() < 0.9:
        mock_id = f"MOCK-{random.randint(10000,99999)}"
        return True, f'Mock order {mock_id} filled at {trade.get("price", 0)}', round(our_size, 2)
    else:
        return False, 'Mock: Simulated fill failure (slippage)', round(our_size, 2)


if __name__ == '__main__':
    print("=" * 60)
    print("  Polymarket Copy Trader")
    print("  NOTE: The copy trading engine runs inside app.py")
    print("  Just run: python app.py")
    print("=" * 60)
    print()
    print("This module provides trade execution functions used by app.py.")
    print("The copy trading engine auto-starts when you run app.py.")
    print()

    config = load_config()
    if config.get('private_key'):
        funder = config.get('funder_address', '')
        print(f"Wallet: {funder}")
        bal = get_usdc_balance(funder)
        print(f"USDC.e Balance: ${bal:,.2f}")
        print()
        print("Testing CLOB connection...")
        client = get_clob_client(config)
        if client:
            print("CLOB API: Connected")
        else:
            print("CLOB API: Failed (check credentials)")
    else:
        print("No credentials configured. Run: python setup_config.py")
