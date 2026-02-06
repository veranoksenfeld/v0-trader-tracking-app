"""
Polymarket Copy Trader - Flask Web Application
Integrated copy trading engine with MOCK MODE for testing.
Unified log system for app.py, fetcher.py, copy_trader.py.
"""
from flask import Flask, render_template, jsonify, request, Response
import sqlite3
from datetime import datetime
import os
import json
import time
import threading
import random
import requests as req

app = Flask(__name__)
DATABASE = 'polymarket_trades.db'
CONFIG_FILE = 'config.json'

# ---- Copy Trading Engine State ----
copy_engine = {
    'running': False,
    'thread': None,
    'last_check': None,
    'trades_copied': 0,
    'trades_failed': 0,
    'trades_blocked': 0,
}


# ============================================
#  CIRCUIT BREAKER / RISK GUARD
#  Ported from Rust bot: src/trading/risk_guard.rs
# ============================================

class RiskGuard:
    """
    Multi-layer safety system ported from the Rust copy trading bot.
    Tracks consecutive large trades per token and trips a circuit breaker
    when rapid large-trade sequences exceed a threshold on thin order books.
    """
    def __init__(self, config=None):
        cfg = config or {}
        self.large_trade_threshold = cfg.get('cb_large_trade_usd', 500)   # USD
        self.consecutive_trigger = cfg.get('cb_consecutive_trigger', 5)
        self.sequence_window = cfg.get('cb_sequence_window_secs', 40)
        self.min_depth_usd = cfg.get('cb_min_depth_usd', 200)
        self.trip_duration = cfg.get('cb_trip_duration_secs', 300)  # 5 min default
        self.max_open_positions = cfg.get('max_open_positions', 10)
        self.max_risk_per_trade_pct = cfg.get('max_risk_per_trade_pct', 7)
        # State: token_id -> {'trades': [(timestamp, size)], 'tripped_until': float|None}
        self._tokens = {}
        self._lock = threading.Lock()
        self.stats = {'allowed': 0, 'blocked': 0, 'tripped': 0}

    def check(self, token_id, trade_size_usd, side='BUY'):
        """
        Fast-path check. Returns (allowed: bool, reason: str, details: dict)
        """
        now = time.time()
        with self._lock:
            state = self._tokens.setdefault(token_id, {'trades': [], 'tripped_until': None})

            # Check if tripped
            if state['tripped_until'] and now < state['tripped_until']:
                secs_left = int(state['tripped_until'] - now)
                self.stats['blocked'] += 1
                return False, 'TRIPPED', {'secs_left': secs_left, 'token': token_id[:16]}

            # Clear expired trip
            if state['tripped_until'] and now >= state['tripped_until']:
                state['tripped_until'] = None

            # SELL trades always allowed (exiting positions = reducing risk)
            if side.upper() == 'SELL':
                self.stats['allowed'] += 1
                return True, 'SELL_ALLOWED', {}

            # Small trade fast path
            if trade_size_usd < self.large_trade_threshold:
                self.stats['allowed'] += 1
                return True, 'SMALL_TRADE', {}

            # Count consecutive large trades in window
            cutoff = now - self.sequence_window
            state['trades'] = [(ts, sz) for ts, sz in state['trades'] if ts > cutoff]
            consecutive = sum(1 for ts, sz in state['trades'] if sz >= self.large_trade_threshold) + 1
            state['trades'].append((now, trade_size_usd))

            # Prune old entries
            if len(state['trades']) > 20:
                state['trades'] = state['trades'][-20:]

            if consecutive >= self.consecutive_trigger:
                # Trip the circuit breaker
                state['tripped_until'] = now + self.trip_duration
                self.stats['tripped'] += 1
                self.stats['blocked'] += 1
                return False, 'SEQ_TRIP', {'consecutive': consecutive, 'token': token_id[:16]}

            self.stats['allowed'] += 1
            return True, 'SEQ_OK', {'consecutive': consecutive}

    def check_open_positions(self, current_open):
        """Enforce max open positions limit"""
        if current_open >= self.max_open_positions:
            return False, f'MAX_POSITIONS ({current_open}/{self.max_open_positions})'
        return True, 'OK'

    def check_trade_risk(self, trade_size_usd, portfolio_value):
        """Enforce max risk per trade as % of portfolio"""
        if portfolio_value <= 0:
            return True, 'NO_PORTFOLIO'
        risk_pct = (trade_size_usd / portfolio_value) * 100
        if risk_pct > self.max_risk_per_trade_pct:
            return False, f'RISK_TOO_HIGH ({risk_pct:.1f}% > {self.max_risk_per_trade_pct}%)'
        return True, f'RISK_OK ({risk_pct:.1f}%)'

    def reset(self, token_id=None):
        with self._lock:
            if token_id:
                self._tokens.pop(token_id, None)
            else:
                self._tokens.clear()
                self.stats = {'allowed': 0, 'blocked': 0, 'tripped': 0}

    def get_status(self):
        with self._lock:
            tripped_tokens = []
            now = time.time()
            for tid, state in self._tokens.items():
                if state['tripped_until'] and now < state['tripped_until']:
                    tripped_tokens.append({
                        'token': tid[:24],
                        'secs_left': int(state['tripped_until'] - now),
                    })
            return {
                'stats': dict(self.stats),
                'tripped_tokens': tripped_tokens,
                'config': {
                    'large_trade_threshold': self.large_trade_threshold,
                    'consecutive_trigger': self.consecutive_trigger,
                    'sequence_window': self.sequence_window,
                    'trip_duration': self.trip_duration,
                    'max_open_positions': self.max_open_positions,
                    'max_risk_per_trade_pct': self.max_risk_per_trade_pct,
                }
            }


# Global risk guard instance
risk_guard = RiskGuard()


# ============================================
#  EXECUTION TIERS (from Rust config/mod.rs)
# ============================================

EXEC_TIERS = [
    {'min_usd': 2000, 'price_buffer': 0.01, 'size_multiplier': 1.25, 'order_type': 'FOK', 'label': 'WHALE'},
    {'min_usd': 500,  'price_buffer': 0.01, 'size_multiplier': 1.0,  'order_type': 'FOK', 'label': 'LARGE'},
    {'min_usd': 100,  'price_buffer': 0.00, 'size_multiplier': 1.0,  'order_type': 'FOK', 'label': 'MEDIUM'},
    {'min_usd': 0,    'price_buffer': 0.00, 'size_multiplier': 1.0,  'order_type': 'FOK', 'label': 'SMALL'},
]


def get_exec_tier(trade_size_usd, side='BUY'):
    """Get execution tier params based on trade size (from Rust EXEC_TIERS)"""
    if side.upper() == 'SELL':
        return {'price_buffer': 0.00, 'size_multiplier': 1.0, 'order_type': 'GTC', 'label': 'SELL'}
    for tier in EXEC_TIERS:
        if trade_size_usd >= tier['min_usd']:
            return tier
    return EXEC_TIERS[-1]


# ============================================
#  COPY STRATEGIES (from Rust prod config)
# ============================================

def calculate_copy_size(trade_size_usd, config, side='BUY'):
    """
    Calculate copy trade size using strategy from config.
    Strategies: PERCENTAGE, FIXED, ADAPTIVE (from Rust bot prod version)
    """
    strategy = config.get('copy_strategy', 'PERCENTAGE')
    max_size = config.get('max_trade_size', 100)
    min_size = config.get('min_trade_size', 1)

    if strategy == 'FIXED':
        our_size = config.get('fixed_trade_size', 10)
    elif strategy == 'ADAPTIVE':
        # Adaptive: scale based on trade tier
        tier = get_exec_tier(trade_size_usd, side)
        base_pct = config.get('copy_percentage', 10) / 100
        our_size = trade_size_usd * base_pct * tier.get('size_multiplier', 1.0)
    else:  # PERCENTAGE (default)
        copy_pct = config.get('copy_percentage', 10)
        our_size = trade_size_usd * (copy_pct / 100)

    # Probability-based sizing (from Rust: ENABLE_PROB_SIZING)
    if config.get('prob_sizing_enabled', False):
        # If price is available, adjust size based on implied probability
        # Lower probability = potentially higher payout, increase size slightly
        pass  # Applied at trade-level with price info

    our_size = min(our_size, max_size)
    our_size = max(our_size, 0)

    return round(our_size, 2)


def apply_prob_sizing(our_size, price, config):
    """
    Probability-based position sizing (from Rust ENABLE_PROB_SIZING).
    Adjusts size based on implied probability of the outcome.
    """
    if not config.get('prob_sizing_enabled', False) or not price:
        return our_size

    price = float(price)
    if price <= 0 or price >= 1:
        return our_size

    # Kelly-inspired: slight boost for mid-range probabilities (30-70%)
    # Reduce for extreme probs (>90% or <10%) as they have less edge
    if 0.3 <= price <= 0.7:
        factor = 1.1  # 10% boost for mid-range
    elif price < 0.1 or price > 0.9:
        factor = 0.7  # 30% reduction for extremes
    else:
        factor = 1.0

    return round(our_size * factor, 2)


# ============================================
#  MARKET CACHE (from Rust markets/market_cache.rs)
# ============================================

class MarketCache:
    """
    Local cache for market metadata (slugs, neg_risk, live status).
    Ported from Rust MarketCaches struct.
    """
    def __init__(self):
        self._cache = {}  # condition_id -> {slug, event_slug, neg_risk, is_live, icon, title, ...}
        self._lock = threading.Lock()
        self._last_refresh = 0
        self.refresh_interval = 1800  # 30 min

    def get(self, condition_id):
        with self._lock:
            return self._cache.get(condition_id)

    def set(self, condition_id, data):
        with self._lock:
            existing = self._cache.get(condition_id, {})
            existing.update(data)
            self._cache[condition_id] = existing

    def get_slug(self, condition_id):
        entry = self.get(condition_id)
        return entry.get('slug', '') if entry else ''

    def get_event_slug(self, condition_id):
        entry = self.get(condition_id)
        return entry.get('event_slug', '') if entry else ''

    def needs_refresh(self):
        return (time.time() - self._last_refresh) >= self.refresh_interval

    def mark_refreshed(self):
        self._last_refresh = time.time()

    def size(self):
        with self._lock:
            return len(self._cache)

    def populate_from_trades(self):
        """Populate cache from existing trade data in DB"""
        try:
            conn = get_db()
            cursor = conn.cursor()
            cursor.execute('SELECT DISTINCT condition_id, slug, event_slug, icon, title, outcome FROM trades WHERE condition_id IS NOT NULL AND condition_id != ""')
            for row in cursor.fetchall():
                r = dict(row)
                if r.get('condition_id'):
                    self.set(r['condition_id'], {
                        'slug': r.get('slug', ''),
                        'event_slug': r.get('event_slug', ''),
                        'icon': r.get('icon', ''),
                        'title': r.get('title', ''),
                        'outcome': r.get('outcome', ''),
                    })
            conn.close()
            self.mark_refreshed()
        except Exception:
            pass

    def get_stats(self):
        return {'cached_markets': self.size(), 'last_refresh': self._last_refresh}


# Global market cache instance
market_cache = MarketCache()

# Try to import py-clob-client
try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import MarketOrderArgs, OrderType
    from py_clob_client.order_builder.constants import BUY, SELL
    CLOB_AVAILABLE = True
except ImportError:
    CLOB_AVAILABLE = False
    print("WARNING: py-clob-client not installed. Live trading disabled. Run: pip install py-clob-client")


# ---- Helpers ----
def load_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r') as f:
            return json.load(f)
    return {'copy_trading_enabled': True, 'copy_percentage': 10, 'max_trade_size': 100, 'min_trade_size': 10, 'mock_mode': False}


def save_config(config):
    with open(CONFIG_FILE, 'w') as f:
        json.dump(config, f, indent=2)


def get_db():
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn


def log_event(source, level, message, details=''):
    """Unified log: source is 'APP', 'ENGINE', 'FETCHER', or 'TRADER'"""
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(
            'INSERT INTO unified_log (timestamp, source, level, message, details) VALUES (?, ?, ?, ?, ?)',
            (datetime.now().isoformat(), source, level, message, details)
        )
        conn.commit()
        conn.close()
    except Exception:
        pass


def get_usdc_balance(funder_address):
    """Get USDC.e balance directly from Polygon RPC"""
    if not funder_address:
        return 0
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


# ---- Database Init ----
def init_db():
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS traders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wallet_address TEXT UNIQUE NOT NULL,
            name TEXT, pseudonym TEXT, bio TEXT,
            profile_image TEXT, x_username TEXT,
            verified_badge INTEGER DEFAULT 0,
            created_at TEXT, added_at TEXT DEFAULT CURRENT_TIMESTAMP,
            copy_trading_enabled INTEGER DEFAULT 0,
            total_profit REAL DEFAULT 0, win_rate REAL DEFAULT 0,
            total_trades INTEGER DEFAULT 0, volume_traded REAL DEFAULT 0
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trader_id INTEGER NOT NULL,
            transaction_hash TEXT UNIQUE,
            side TEXT, size REAL, price REAL, usdc_size REAL,
            timestamp INTEGER, title TEXT, slug TEXT, icon TEXT,
            event_slug TEXT, outcome TEXT, outcome_index INTEGER,
            condition_id TEXT, asset TEXT,
            fetched_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (trader_id) REFERENCES traders(id)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS copy_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            original_trade_id INTEGER,
            trader_name TEXT,
            market_title TEXT,
            market_slug TEXT,
            event_slug TEXT,
            condition_id TEXT,
            icon TEXT,
            outcome TEXT,
            side TEXT,
            original_size REAL,
            our_size REAL,
            price REAL,
            current_price REAL,
            pnl REAL DEFAULT 0,
            pnl_pct REAL DEFAULT 0,
            closed INTEGER DEFAULT 0,
            closed_at TEXT,
            result TEXT DEFAULT 'OPEN',
            status TEXT,
            mock INTEGER DEFAULT 0,
            executed_at TEXT,
            response TEXT,
            FOREIGN KEY (original_trade_id) REFERENCES trades(id)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS unified_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            source TEXT NOT NULL DEFAULT 'APP',
            level TEXT NOT NULL,
            message TEXT NOT NULL,
            details TEXT DEFAULT ''
        )
    ''')

    # Migrate old copy_log to unified_log
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='copy_log'")
    if cursor.fetchone():
        try:
            cursor.execute("INSERT OR IGNORE INTO unified_log (timestamp, source, level, message, details) SELECT timestamp, 'ENGINE', level, message, details FROM copy_log")
            conn.commit()
        except Exception:
            pass

    # Migrate tables
    for table, cols in [
        ('traders', [
            ('copy_trading_enabled', 'INTEGER', '0'),
            ('total_profit', 'REAL', '0'), ('win_rate', 'REAL', '0'),
            ('total_trades', 'INTEGER', '0'), ('volume_traded', 'REAL', '0')
        ]),
        ('copy_trades', [
            ('trader_name', 'TEXT', "''"), ('market_title', 'TEXT', "''"),
            ('market_slug', 'TEXT', "''"), ('event_slug', 'TEXT', "''"),
            ('condition_id', 'TEXT', "''"), ('icon', 'TEXT', "''"),
            ('outcome', 'TEXT', "''"),
            ('side', 'TEXT', "''"), ('original_size', 'REAL', '0'),
            ('our_size', 'REAL', '0'), ('price', 'REAL', '0'),
            ('current_price', 'REAL', '0'),
            ('pnl', 'REAL', '0'), ('pnl_pct', 'REAL', '0'),
            ('closed', 'INTEGER', '0'), ('closed_at', 'TEXT', 'NULL'),
            ('result', 'TEXT', "'OPEN'"),
            ('mock', 'INTEGER', '0'),
        ]),
    ]:
        cursor.execute(f"PRAGMA table_info({table})")
        existing = [col[1] for col in cursor.fetchall()]
        for col, typ, default in cols:
            if col not in existing:
                try:
                    cursor.execute(f'ALTER TABLE {table} ADD COLUMN {col} {typ} DEFAULT {default}')
                except Exception:
                    pass

    conn.commit()
    conn.close()


# ============================================
#  WIN RATE CALCULATION
# ============================================

def calculate_win_rate(trader_id):
    """
    Calculate win rate for a trader based on their sell trades.
    A trade is a 'win' if the sell price > average buy price for same market.
    Also counts: if bought at < 50c and outcome was correct (sell > buy).
    """
    conn = get_db()
    cursor = conn.cursor()

    # Get all trades grouped by market (title + outcome)
    cursor.execute('''
        SELECT title, outcome, side, price, usdc_size
        FROM trades WHERE trader_id = ? AND price IS NOT NULL
        ORDER BY timestamp ASC
    ''', (trader_id,))
    trades = [dict(r) for r in cursor.fetchall()]

    if not trades:
        conn.close()
        return 0.0

    # Group by market
    markets = {}
    for t in trades:
        key = (t['title'] or '') + '|' + (t['outcome'] or '')
        if key not in markets:
            markets[key] = {'buys': [], 'sells': []}
        side = (t['side'] or '').upper()
        if side == 'BUY':
            markets[key]['buys'].append(t)
        elif side == 'SELL':
            markets[key]['sells'].append(t)

    wins = 0
    total_resolved = 0

    for key, data in markets.items():
        if not data['buys']:
            continue
        avg_buy = sum(b['price'] for b in data['buys']) / len(data['buys'])

        if data['sells']:
            avg_sell = sum(s['price'] for s in data['sells']) / len(data['sells'])
            total_resolved += 1
            if avg_sell > avg_buy:
                wins += 1
        elif avg_buy <= 0.3:
            # Still holding a cheap position, count as pending (skip)
            pass
        elif avg_buy >= 0.85:
            # Bought near certainty, likely a win
            total_resolved += 1
            wins += 1

    win_rate = (wins / total_resolved * 100) if total_resolved > 0 else 0

    # Update in DB
    cursor.execute('UPDATE traders SET win_rate = ? WHERE id = ?', (round(win_rate, 1), trader_id))

    # Also update total_trades and volume
    cursor.execute('SELECT COUNT(*) as c, SUM(usdc_size) as v FROM trades WHERE trader_id = ?', (trader_id,))
    row = cursor.fetchone()
    cursor.execute('UPDATE traders SET total_trades = ?, volume_traded = ? WHERE id = ?',
                   (row['c'] or 0, round(row['v'] or 0, 2), trader_id))

    conn.commit()
    conn.close()
    return round(win_rate, 1)


def refresh_all_win_rates():
    """Recalculate win rates for all traders"""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('SELECT id FROM traders')
    ids = [r['id'] for r in cursor.fetchall()]
    conn.close()
    for tid in ids:
        calculate_win_rate(tid)


# ============================================
#  COPY TRADING ENGINE (background thread)
# ============================================

HOST_CLOB = "https://clob.polymarket.com"


def get_clob_client(config):
    if not CLOB_AVAILABLE:
        return None
    pk = config.get('private_key')
    funder = config.get('funder_address')
    sig_type = config.get('signature_type')
    if sig_type is None:
        sig_type = 1
    sig_type = int(sig_type)
    if not pk or not funder:
        return None
    try:
        client = ClobClient(HOST_CLOB, key=pk, chain_id=137, signature_type=sig_type, funder=funder)
        creds = client.create_or_derive_api_creds()
        if creds is None:
            log_event('ENGINE', 'ERROR', 'CLOB auth failed', 'create_or_derive_api_creds returned None')
            return None
        client.set_api_creds(creds)
        return client
    except Exception as e:
        log_event('ENGINE', 'ERROR', 'CLOB init failed', str(e))
        return None


def execute_mock_trade(trade, config):
    """Simulate a copy trade -- no real money spent. Uses strategy-based sizing."""
    side_str = (trade.get('side') or '').upper()
    original_size = float(trade.get('usdc_size') or 0)

    # Use strategy-based sizing
    our_size = calculate_copy_size(original_size, config, side_str)

    # Apply probability sizing
    our_size = apply_prob_sizing(our_size, trade.get('price'), config)

    # In mock mode, always set a minimum mock size for testing
    if our_size < 0.01:
        our_size = round(random.uniform(1, 10), 2)

    # Get execution tier info
    tier = get_exec_tier(original_size, side_str)
    tier_label = tier.get('label', '?')

    # Simulate success with ~90% probability
    if random.random() < 0.9:
        mock_id = f"MOCK-{random.randint(10000,99999)}"
        return True, f'Mock {mock_id} [{tier_label}] filled at {trade.get("price", 0)}', round(our_size, 2)
    else:
        return False, f'Mock: Simulated fill failure [{tier_label}] (slippage)', round(our_size, 2)


def execute_live_trade(client, trade, config):
    """Execute a real copy trade via CLOB with tiered execution"""
    try:
        side_str = (trade.get('side') or '').upper()
        original_size = float(trade.get('usdc_size') or 0)

        # Strategy-based sizing
        our_size = calculate_copy_size(original_size, config, side_str)
        our_size = apply_prob_sizing(our_size, trade.get('price'), config)

        if our_size < 1:
            return False, 'Size too small', 0

        side = BUY if side_str == 'BUY' else SELL
        token_id = trade.get('asset')
        if not token_id:
            return False, 'No token ID', 0

        # Get execution tier
        tier = get_exec_tier(original_size, side_str)
        tier_label = tier.get('label', '?')

        # Apply size multiplier from tier
        our_size = round(our_size * tier.get('size_multiplier', 1.0), 2)
        our_size = min(our_size, config.get('max_trade_size', 100))

        market_order = MarketOrderArgs(token_id=token_id, amount=our_size, side=side, order_type=OrderType.FOK)
        signed_order = client.create_market_order(market_order)
        resp = client.post_order(signed_order, OrderType.FOK)

        # Resubmit logic (from Rust): retry with price escalation on failure
        if not resp or (hasattr(resp, 'get') and resp.get('error_msg')):
            max_retries = 3 if original_size >= 2000 else 2
            for attempt in range(max_retries):
                time.sleep(0.05)  # 50ms between retries
                try:
                    resp = client.post_order(signed_order, OrderType.FOK)
                    if resp and not (hasattr(resp, 'get') and resp.get('error_msg')):
                        log_event('ENGINE', 'INFO', f'Resubmit #{attempt+1} succeeded [{tier_label}]')
                        break
                except Exception:
                    pass

        return True, f'[{tier_label}] {str(resp)[:180]}', our_size
    except Exception as e:
        return False, str(e), 0


def copy_trading_loop():
    """Main copy trading loop with circuit breaker, tiered execution, and strategy-based sizing"""
    global copy_engine, risk_guard
    log_event('ENGINE', 'INFO', 'Copy trading engine started with Risk Guard + Tiered Execution')
    copy_engine['running'] = True

    # Initialize risk guard from config
    cfg = load_config()
    risk_guard = RiskGuard(cfg)
    market_cache.populate_from_trades()
    log_event('ENGINE', 'INFO', f'Market cache loaded: {market_cache.size()} markets')

    while copy_engine['running']:
        try:
            config = load_config()
            mock_mode = config.get('mock_mode', False)

            if not mock_mode and (not config.get('private_key') or not config.get('funder_address')):
                time.sleep(10)
                continue

            # Use separate timestamps for mock vs live so toggling mock doesn't skip live trades
            if mock_mode:
                last_processed = config.get('last_mock_processed_timestamp', 0)
            else:
                last_processed = config.get('last_processed_timestamp', 0)

            conn = get_db()
            cursor = conn.cursor()

            # In mock mode, check if any copy-enabled traders exist; if not, use ALL traders
            cursor.execute('SELECT COUNT(*) as c FROM traders WHERE copy_trading_enabled = 1')
            copy_enabled_count = cursor.fetchone()['c']

            if copy_enabled_count > 0:
                cursor.execute('''
                    SELECT t.*, tr.wallet_address, tr.name, tr.pseudonym
                    FROM trades t
                    JOIN traders tr ON t.trader_id = tr.id
                    WHERE tr.copy_trading_enabled = 1 AND t.timestamp > ?
                    ORDER BY t.timestamp ASC LIMIT 20
                ''', (last_processed,))
            elif mock_mode:
                cursor.execute('''
                    SELECT t.*, tr.wallet_address, tr.name, tr.pseudonym
                    FROM trades t
                    JOIN traders tr ON t.trader_id = tr.id
                    WHERE t.timestamp > ?
                    ORDER BY t.timestamp ASC LIMIT 20
                ''', (last_processed,))
            else:
                cursor.execute('SELECT 1 WHERE 0')

            new_trades = [dict(row) for row in cursor.fetchall()]

            # Count open positions for risk guard
            cursor.execute("SELECT COUNT(*) as c FROM copy_trades WHERE result='OPEN' AND status='SUCCESS'")
            open_positions = cursor.fetchone()['c']
            conn.close()

            copy_engine['last_check'] = datetime.now().isoformat()

            if not new_trades:
                time.sleep(5)
                continue

            mode_label = 'MOCK' if mock_mode else 'LIVE'
            log_event('ENGINE', 'INFO', f'Found {len(new_trades)} trade(s) to copy [{mode_label}]')

            # For live mode, check balance and init CLOB
            client = None
            if not mock_mode:
                balance = get_usdc_balance(config.get('funder_address', ''))
                if balance < 1:
                    log_event('ENGINE', 'WARN', f'Low balance: ${balance:.2f}', 'Need at least $1 USDC.e')
                    time.sleep(30)
                    continue
                client = get_clob_client(config)
                if not client:
                    log_event('ENGINE', 'ERROR', 'Could not connect to CLOB API', 'Check credentials')
                    time.sleep(30)
                    continue

            for trade in new_trades:
                min_size = config.get('min_trade_size', 10)
                trade_size = float(trade.get('usdc_size') or 0)
                side_str = (trade.get('side') or '').upper()
                token_id = trade.get('asset') or trade.get('condition_id') or ''

                # In mock mode, allow smaller trades for testing
                effective_min = 0.01 if mock_mode else min_size

                if trade_size < effective_min:
                    ts_key = 'last_mock_processed_timestamp' if mock_mode else 'last_processed_timestamp'
                    config[ts_key] = trade['timestamp']
                    save_config(config)
                    continue

                trader_name = trade.get('name') or trade.get('pseudonym') or (trade.get('wallet_address') or '')[:12]
                market_title = trade.get('title', 'Unknown')

                # --- RISK GUARD CHECK (from Rust risk_guard.rs) ---
                if config.get('risk_guard_enabled', True):
                    # Check circuit breaker
                    allowed, reason, details = risk_guard.check(token_id, trade_size, side_str)
                    if not allowed:
                        copy_engine['trades_blocked'] = copy_engine.get('trades_blocked', 0) + 1
                        log_event('ENGINE', 'SKIP', f'[RISK GUARD] {reason}: {trader_name}',
                                  f'{market_title[:40]} | ${trade_size:.2f} | {json.dumps(details)}')
                        ts_key = 'last_mock_processed_timestamp' if mock_mode else 'last_processed_timestamp'
                        config[ts_key] = trade['timestamp']
                        save_config(config)
                        continue

                    # Check max open positions
                    if side_str == 'BUY':
                        pos_ok, pos_reason = risk_guard.check_open_positions(open_positions)
                        if not pos_ok:
                            copy_engine['trades_blocked'] = copy_engine.get('trades_blocked', 0) + 1
                            log_event('ENGINE', 'SKIP', f'[RISK GUARD] {pos_reason}', f'{market_title[:50]}')
                            ts_key = 'last_mock_processed_timestamp' if mock_mode else 'last_processed_timestamp'
                            config[ts_key] = trade['timestamp']
                            save_config(config)
                            continue

                # Update market cache
                if trade.get('condition_id'):
                    market_cache.set(trade['condition_id'], {
                        'slug': trade.get('slug', ''),
                        'event_slug': trade.get('event_slug', ''),
                        'icon': trade.get('icon', ''),
                        'title': market_title,
                        'outcome': trade.get('outcome', ''),
                    })

                # Get tier info for logging
                tier = get_exec_tier(trade_size, side_str)
                tier_label = tier.get('label', '?')
                strategy = config.get('copy_strategy', 'PERCENTAGE')

                log_event('ENGINE', 'COPY', f'[{mode_label}|{tier_label}|{strategy}] Copying {side_str} from {trader_name}',
                          f'{market_title} | ${trade_size:.2f}')

                if mock_mode:
                    success, response, our_size = execute_mock_trade(trade, config)
                else:
                    success, response, our_size = execute_live_trade(client, trade, config)

                # Record in DB with market metadata
                conn = get_db()
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO copy_trades (original_trade_id, trader_name, market_title, market_slug, event_slug, condition_id, icon, outcome, side, original_size, our_size, price, status, mock, result, executed_at, response)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    trade['id'], trader_name, market_title,
                    trade.get('slug', ''), trade.get('event_slug', ''),
                    trade.get('condition_id', ''), trade.get('icon', ''),
                    trade.get('outcome', ''), side_str,
                    trade_size, our_size, trade.get('price', 0),
                    'SUCCESS' if success else 'FAILED',
                    1 if mock_mode else 0,
                    'OPEN' if side_str == 'BUY' else 'CLOSED',
                    datetime.now().isoformat(), response
                ))
                conn.commit()
                conn.close()

                if success:
                    copy_engine['trades_copied'] += 1
                    if side_str == 'BUY':
                        open_positions += 1
                    log_event('ENGINE', 'SUCCESS', f'[{mode_label}|{tier_label}] ${our_size:.2f} {side_str} on {market_title[:50]}', response[:200])
                else:
                    copy_engine['trades_failed'] += 1
                    log_event('ENGINE', 'FAILED', f'[{mode_label}|{tier_label}] {side_str} on {market_title[:50]}', response[:200])

                ts_key = 'last_mock_processed_timestamp' if mock_mode else 'last_processed_timestamp'
                config[ts_key] = trade['timestamp']
                save_config(config)
                time.sleep(0.5 if mock_mode else 1)

            # Periodically refresh win rates and market cache
            if random.random() < 0.05:
                refresh_all_win_rates()
            if market_cache.needs_refresh():
                market_cache.populate_from_trades()

        except Exception as e:
            log_event('ENGINE', 'ERROR', 'Engine error', str(e))

        time.sleep(5)

    log_event('ENGINE', 'INFO', 'Copy trading engine stopped')


def start_copy_engine():
    global copy_engine
    if copy_engine['thread'] and copy_engine['thread'].is_alive():
        return
    copy_engine['running'] = True
    copy_engine['thread'] = threading.Thread(target=copy_trading_loop, daemon=True)
    copy_engine['thread'].start()


def stop_copy_engine():
    global copy_engine
    copy_engine['running'] = False


# ============================================
#  API ROUTES
# ============================================

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/traders', methods=['GET'])
def get_traders():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM traders ORDER BY added_at DESC')
    traders = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return jsonify(traders)


@app.route('/api/traders', methods=['POST'])
def add_trader():
    data = request.json
    wallet = data.get('wallet_address', '').strip().lower()
    if not wallet:
        return jsonify({'error': 'Wallet address is required'}), 400
    if not wallet.startswith('0x') or len(wallet) != 42:
        return jsonify({'error': 'Invalid wallet address'}), 400
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('SELECT id FROM traders WHERE wallet_address = ?', (wallet,))
    if cursor.fetchone():
        conn.close()
        return jsonify({'error': 'Already tracking this wallet'}), 400
    cursor.execute('INSERT INTO traders (wallet_address) VALUES (?)', (wallet,))
    conn.commit()
    tid = cursor.lastrowid
    conn.close()
    log_event('APP', 'INFO', f'Started tracking wallet', wallet)
    return jsonify({'success': True, 'trader_id': tid})


@app.route('/api/traders/<int:trader_id>', methods=['DELETE'])
def delete_trader(trader_id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('DELETE FROM trades WHERE trader_id = ?', (trader_id,))
    cursor.execute('DELETE FROM traders WHERE id = ?', (trader_id,))
    conn.commit()
    conn.close()
    log_event('APP', 'INFO', f'Removed trader #{trader_id}')
    return jsonify({'success': True})


@app.route('/api/trades', methods=['GET'])
def get_trades():
    trader_id = request.args.get('trader_id')
    limit = request.args.get('limit', 100, type=int)
    conn = get_db()
    cursor = conn.cursor()
    if trader_id:
        cursor.execute('''
            SELECT t.*, tr.wallet_address, tr.name, tr.pseudonym, tr.profile_image, tr.verified_badge
            FROM trades t JOIN traders tr ON t.trader_id = tr.id
            WHERE t.trader_id = ? ORDER BY t.timestamp DESC LIMIT ?
        ''', (trader_id, limit))
    else:
        cursor.execute('''
            SELECT t.*, tr.wallet_address, tr.name, tr.pseudonym, tr.profile_image, tr.verified_badge
            FROM trades t JOIN traders tr ON t.trader_id = tr.id
            ORDER BY t.timestamp DESC LIMIT ?
        ''', (limit,))
    trades = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return jsonify(trades)


@app.route('/api/stats', methods=['GET'])
def get_stats():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) as count FROM traders')
    trader_count = cursor.fetchone()['count']
    cursor.execute('SELECT COUNT(*) as count FROM trades')
    trade_count = cursor.fetchone()['count']
    cursor.execute('SELECT SUM(usdc_size) as total FROM trades')
    total_volume = cursor.fetchone()['total'] or 0
    cursor.execute('SELECT MAX(fetched_at) as last_fetch FROM trades')
    last_fetch = cursor.fetchone()['last_fetch']
    conn.close()
    return jsonify({
        'trader_count': trader_count, 'trade_count': trade_count,
        'total_volume': round(total_volume, 2), 'last_fetch': last_fetch
    })


@app.route('/api/traders/<int:trader_id>/copy', methods=['POST'])
def toggle_copy_trading(trader_id):
    data = request.json
    enabled = data.get('enabled', False)
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('UPDATE traders SET copy_trading_enabled = ? WHERE id = ?', (1 if enabled else 0, trader_id))
    conn.commit()
    conn.close()
    log_event('APP', 'INFO', f'Copy trading {"enabled" if enabled else "disabled"} for trader #{trader_id}')
    return jsonify({'success': True, 'copy_trading_enabled': enabled})


@app.route('/api/traders/<int:trader_id>/details', methods=['GET'])
def get_trader_details(trader_id):
    # Refresh win rate
    wr = calculate_win_rate(trader_id)

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM traders WHERE id = ?', (trader_id,))
    trader = cursor.fetchone()
    if not trader:
        conn.close()
        return jsonify({'error': 'Not found'}), 404
    td = dict(trader)
    cursor.execute('''
        SELECT COUNT(*) as total_trades, SUM(usdc_size) as volume_traded,
            AVG(CASE WHEN side='BUY' THEN price END) as avg_buy_price,
            AVG(CASE WHEN side='SELL' THEN price END) as avg_sell_price,
            COUNT(CASE WHEN side='BUY' THEN 1 END) as buy_count,
            COUNT(CASE WHEN side='SELL' THEN 1 END) as sell_count,
            MIN(timestamp) as first_trade, MAX(timestamp) as last_trade
        FROM trades WHERE trader_id = ?
    ''', (trader_id,))
    s = cursor.fetchone()
    td['stats'] = {
        'total_trades': s['total_trades'] or 0,
        'volume_traded': round(s['volume_traded'] or 0, 2),
        'avg_buy_price': round((s['avg_buy_price'] or 0) * 100, 1),
        'avg_sell_price': round((s['avg_sell_price'] or 0) * 100, 1),
        'buy_count': s['buy_count'] or 0, 'sell_count': s['sell_count'] or 0,
        'first_trade': s['first_trade'], 'last_trade': s['last_trade'],
        'win_rate': wr,
    }
    cursor.execute('''
        SELECT title, slug, icon, outcome, COUNT(*) as trade_count, SUM(usdc_size) as volume
        FROM trades WHERE trader_id = ? GROUP BY title, outcome ORDER BY trade_count DESC LIMIT 5
    ''', (trader_id,))
    td['recent_markets'] = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return jsonify(td)


@app.route('/api/config', methods=['GET'])
def get_config():
    config = load_config()
    funder = config.get('funder_address', '')
    has_creds = bool(config.get('private_key') and funder)
    mock_mode = config.get('mock_mode', False)

    usdc_balance = None
    if has_creds:
        usdc_balance = get_usdc_balance(funder)

    safe = {
        'copy_trading_enabled': config.get('copy_trading_enabled', False),
        'copy_percentage': config.get('copy_percentage', 10),
        'max_trade_size': config.get('max_trade_size', 100),
        'min_trade_size': config.get('min_trade_size', 10),
        'has_credentials': has_creds,
        'funder_address': funder,
        'usdc_balance': round(usdc_balance, 2) if usdc_balance is not None else None,
        'engine_running': copy_engine['running'] and copy_engine['thread'] is not None and copy_engine['thread'].is_alive(),
        'trades_copied': copy_engine['trades_copied'],
        'trades_failed': copy_engine['trades_failed'],
        'trades_blocked': copy_engine.get('trades_blocked', 0),
        'last_check': copy_engine['last_check'],
        'clob_available': CLOB_AVAILABLE,
        'signature_type': config.get('signature_type', 1),
        'mock_mode': mock_mode,
        # Strategy & risk guard (from Rust bot)
        'copy_strategy': config.get('copy_strategy', 'PERCENTAGE'),
        'fixed_trade_size': config.get('fixed_trade_size', 10),
        'prob_sizing_enabled': config.get('prob_sizing_enabled', False),
        'risk_guard_enabled': config.get('risk_guard_enabled', True),
        'max_open_positions': config.get('max_open_positions', 10),
        'max_risk_per_trade_pct': config.get('max_risk_per_trade_pct', 7),
        'cb_large_trade_usd': config.get('cb_large_trade_usd', 500),
        'cb_consecutive_trigger': config.get('cb_consecutive_trigger', 5),
    }
    return jsonify(safe)


@app.route('/api/config', methods=['POST'])
def update_config():
    data = request.json
    config = load_config()
    if 'copy_trading_enabled' in data:
        config['copy_trading_enabled'] = data['copy_trading_enabled']
    if 'copy_percentage' in data:
        config['copy_percentage'] = max(1, min(100, int(data['copy_percentage'])))
    if 'max_trade_size' in data:
        config['max_trade_size'] = max(1, float(data['max_trade_size']))
    if 'min_trade_size' in data:
        config['min_trade_size'] = max(0, float(data['min_trade_size']))
    if 'mock_mode' in data:
        new_mock = bool(data['mock_mode'])
        old_mock = config.get('mock_mode', False)
        config['mock_mode'] = new_mock
        if new_mock and not old_mock:
            config['last_mock_processed_timestamp'] = 0
        log_event('APP', 'INFO', f'Mock mode {"enabled" if new_mock else "disabled"}')
    # Strategy settings (from Rust bot)
    if 'copy_strategy' in data:
        config['copy_strategy'] = data['copy_strategy']
        log_event('APP', 'INFO', f'Copy strategy set to {data["copy_strategy"]}')
    if 'fixed_trade_size' in data:
        config['fixed_trade_size'] = max(0.1, float(data['fixed_trade_size']))
    if 'prob_sizing_enabled' in data:
        config['prob_sizing_enabled'] = bool(data['prob_sizing_enabled'])
    if 'risk_guard_enabled' in data:
        config['risk_guard_enabled'] = bool(data['risk_guard_enabled'])
    if 'max_open_positions' in data:
        config['max_open_positions'] = max(1, int(data['max_open_positions']))
    if 'max_risk_per_trade_pct' in data:
        config['max_risk_per_trade_pct'] = max(1, min(100, float(data['max_risk_per_trade_pct'])))
    if 'cb_large_trade_usd' in data:
        config['cb_large_trade_usd'] = max(10, float(data['cb_large_trade_usd']))
    if 'cb_consecutive_trigger' in data:
        config['cb_consecutive_trigger'] = max(2, int(data['cb_consecutive_trigger']))
    save_config(config)
    return jsonify({'success': True})


@app.route('/api/config/credentials', methods=['POST'])
def update_credentials():
    data = request.json
    config = load_config()
    pk = data.get('private_key', '').strip()
    fa = data.get('funder_address', '').strip().lower()
    st = data.get('signature_type', 1)
    if not pk or not fa:
        return jsonify({'error': 'Private key and funder address required'}), 400
    if not fa.startswith('0x') or len(fa) != 42:
        return jsonify({'error': 'Invalid funder address'}), 400
    config['private_key'] = pk
    config['funder_address'] = fa
    config['signature_type'] = int(st) if st is not None else 1
    save_config(config)
    log_event('APP', 'INFO', 'Credentials updated', f'Funder: {fa[:10]}...')
    return jsonify({'success': True})


@app.route('/api/balance', methods=['GET'])
def get_balance():
    config = load_config()
    funder = config.get('funder_address', '').strip().lower()
    if not funder:
        return jsonify({'error': 'No funder address configured', 'balance': None})
    usdc = get_usdc_balance(funder)
    return jsonify({'success': True, 'usdc_balance': round(usdc, 2), 'funder_address': funder})


@app.route('/api/copy-trades', methods=['GET'])
def get_copy_trades():
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT * FROM copy_trades ORDER BY executed_at DESC LIMIT 100')
        trades = [dict(row) for row in cursor.fetchall()]
    except Exception:
        trades = []
    conn.close()
    return jsonify(trades)


@app.route('/api/copy-trades/summary', methods=['GET'])
def get_copy_trades_summary():
    """Returns win/loss summary for closed copy trades"""
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT COUNT(*) as c FROM copy_trades WHERE status='SUCCESS'")
        total = cursor.fetchone()['c']
        cursor.execute("SELECT COUNT(*) as c FROM copy_trades WHERE result='WIN'")
        wins = cursor.fetchone()['c']
        cursor.execute("SELECT COUNT(*) as c FROM copy_trades WHERE result='LOSS'")
        losses = cursor.fetchone()['c']
        cursor.execute("SELECT COUNT(*) as c FROM copy_trades WHERE result='OPEN'")
        open_count = cursor.fetchone()['c']
        cursor.execute("SELECT SUM(pnl) as total_pnl FROM copy_trades WHERE result IN ('WIN','LOSS')")
        row = cursor.fetchone()
        total_pnl = round(row['total_pnl'] or 0, 2)
        cursor.execute("SELECT SUM(our_size) as total_invested FROM copy_trades WHERE status='SUCCESS'")
        total_invested = round(cursor.fetchone()['total_invested'] or 0, 2)
        cursor.execute("SELECT SUM(pnl) as p FROM copy_trades WHERE result='WIN'")
        total_win_pnl = round((cursor.fetchone()['p'] or 0), 2)
        cursor.execute("SELECT SUM(pnl) as p FROM copy_trades WHERE result='LOSS'")
        total_loss_pnl = round((cursor.fetchone()['p'] or 0), 2)
    except Exception:
        total = wins = losses = open_count = 0
        total_pnl = total_invested = total_win_pnl = total_loss_pnl = 0
    conn.close()
    win_rate = round(wins / (wins + losses) * 100, 1) if (wins + losses) > 0 else 0
    return jsonify({
        'total': total, 'wins': wins, 'losses': losses, 'open': open_count,
        'win_rate': win_rate, 'total_pnl': total_pnl, 'total_invested': total_invested,
        'total_win_pnl': total_win_pnl, 'total_loss_pnl': total_loss_pnl,
    })


@app.route('/api/copy-trades/<int:trade_id>/close', methods=['POST'])
def close_copy_trade(trade_id):
    """Manually close a copy trade with a result"""
    data = request.json
    result = data.get('result', 'WIN')  # WIN or LOSS
    current_price = data.get('current_price', 0)
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM copy_trades WHERE id = ?', (trade_id,))
    trade = cursor.fetchone()
    if not trade:
        conn.close()
        return jsonify({'error': 'Trade not found'}), 404
    td = dict(trade)

    entry_price = td.get('price', 0) or 0
    our_size = td.get('our_size', 0) or 0
    side = (td.get('side', '') or '').upper()

    # Calculate PnL
    if side == 'BUY':
        pnl = (float(current_price) - float(entry_price)) * float(our_size)
    else:
        pnl = (float(entry_price) - float(current_price)) * float(our_size)

    pnl_pct = (pnl / float(our_size) * 100) if our_size > 0 else 0
    actual_result = 'WIN' if pnl >= 0 else 'LOSS'

    cursor.execute('''
        UPDATE copy_trades SET closed = 1, closed_at = ?, result = ?, current_price = ?, pnl = ?, pnl_pct = ?
        WHERE id = ?
    ''', (datetime.now().isoformat(), actual_result, current_price, round(pnl, 4), round(pnl_pct, 2), trade_id))
    conn.commit()
    conn.close()
    log_event('ENGINE', actual_result, f'Trade closed: {td.get("market_title", "")[:50]}', f'PnL: ${pnl:.2f} ({pnl_pct:.1f}%)')
    return jsonify({'success': True, 'pnl': round(pnl, 4), 'result': actual_result})


@app.route('/api/risk-guard', methods=['GET'])
def get_risk_guard_status():
    """Get circuit breaker / risk guard status"""
    return jsonify(risk_guard.get_status())


@app.route('/api/risk-guard/reset', methods=['POST'])
def reset_risk_guard():
    """Reset all circuit breaker trips"""
    risk_guard.reset()
    log_event('ENGINE', 'INFO', 'Risk guard reset')
    return jsonify({'success': True})


@app.route('/api/market-cache/stats', methods=['GET'])
def get_market_cache_stats():
    return jsonify(market_cache.get_stats())


@app.route('/api/logs', methods=['GET'])
def get_logs():
    """Unified log endpoint for all sources"""
    limit = request.args.get('limit', 200, type=int)
    source = request.args.get('source', '')  # APP, ENGINE, FETCHER, TRADER, or '' for all
    since_id = request.args.get('since_id', 0, type=int)
    conn = get_db()
    cursor = conn.cursor()
    try:
        if source:
            cursor.execute('SELECT * FROM unified_log WHERE id > ? AND source = ? ORDER BY id DESC LIMIT ?', (since_id, source, limit))
        else:
            cursor.execute('SELECT * FROM unified_log WHERE id > ? ORDER BY id DESC LIMIT ?', (since_id, limit))
        logs = [dict(row) for row in cursor.fetchall()]
    except Exception:
        logs = []
    conn.close()
    return jsonify(logs)


# Keep old endpoint for compatibility
@app.route('/api/copy-log', methods=['GET'])
def get_copy_log():
    limit = request.args.get('limit', 100, type=int)
    since_id = request.args.get('since_id', 0, type=int)
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT * FROM unified_log WHERE id > ? ORDER BY id DESC LIMIT ?', (since_id, limit))
        logs = [dict(row) for row in cursor.fetchall()]
    except Exception:
        logs = []
    conn.close()
    return jsonify(logs)


@app.route('/api/engine/start', methods=['POST'])
def engine_start():
    start_copy_engine()
    return jsonify({'success': True, 'running': True})


@app.route('/api/engine/stop', methods=['POST'])
def engine_stop():
    stop_copy_engine()
    return jsonify({'success': True, 'running': False})


@app.route('/api/positions/<wallet>')
def get_positions(wallet):
    try:
        resp = req.get(
            'https://data-api.polymarket.com/positions',
            params={'user': wallet, 'sizeThreshold': 0.1, 'limit': 20, 'sortBy': 'CURRENT', 'sortDirection': 'DESC'},
            timeout=15
        )
        if resp.status_code == 200:
            return jsonify(resp.json())
        return jsonify([])
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/stream')
def stream():
    """SSE endpoint for real-time UI updates"""
    def event_stream():
        last_trade_id = 0
        last_log_id = 0
        last_copy_id = 0
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute('SELECT MAX(id) as m FROM trades')
        row = cursor.fetchone()
        if row and row['m']:
            last_trade_id = row['m']
        try:
            cursor.execute('SELECT MAX(id) as m FROM unified_log')
            row = cursor.fetchone()
            if row and row['m']:
                last_log_id = row['m']
        except Exception:
            pass
        try:
            cursor.execute('SELECT MAX(id) as m FROM copy_trades')
            row = cursor.fetchone()
            if row and row['m']:
                last_copy_id = row['m']
        except Exception:
            pass
        conn.close()

        while True:
            time.sleep(2)
            try:
                conn = get_db()
                cursor = conn.cursor()

                # New trades
                cursor.execute('''
                    SELECT t.*, tr.wallet_address, tr.name, tr.pseudonym, tr.profile_image, tr.verified_badge
                    FROM trades t JOIN traders tr ON t.trader_id = tr.id
                    WHERE t.id > ? ORDER BY t.id ASC
                ''', (last_trade_id,))
                new_trades = [dict(row) for row in cursor.fetchall()]
                if new_trades:
                    last_trade_id = new_trades[-1]['id']
                    yield f"data: {json.dumps({'type': 'new_trades', 'trades': new_trades})}\n\n"

                # New unified log entries
                try:
                    cursor.execute('SELECT * FROM unified_log WHERE id > ? ORDER BY id ASC', (last_log_id,))
                    new_logs = [dict(row) for row in cursor.fetchall()]
                    if new_logs:
                        last_log_id = new_logs[-1]['id']
                        yield f"data: {json.dumps({'type': 'logs', 'logs': new_logs})}\n\n"
                except Exception:
                    pass

                # New copy trades
                try:
                    cursor.execute('SELECT * FROM copy_trades WHERE id > ? ORDER BY id ASC', (last_copy_id,))
                    new_copies = [dict(row) for row in cursor.fetchall()]
                    if new_copies:
                        last_copy_id = new_copies[-1]['id']
                        yield f"data: {json.dumps({'type': 'copy_trades', 'trades': new_copies})}\n\n"
                except Exception:
                    pass

                # Heartbeat
                cursor.execute('SELECT COUNT(*) as c FROM trades')
                tc = cursor.fetchone()['c']
                cursor.execute('SELECT COUNT(*) as c FROM traders')
                trc = cursor.fetchone()['c']
                yield f"data: {json.dumps({'type': 'heartbeat', 'trade_count': tc, 'trader_count': trc, 'engine_running': copy_engine.get('running', False) and copy_engine.get('thread') is not None and copy_engine['thread'].is_alive()})}\n\n"

                conn.close()
            except Exception:
                yield f"data: {json.dumps({'type': 'heartbeat'})}\n\n"

    return Response(event_stream(), mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})


if __name__ == '__main__':
    init_db()
    start_copy_engine()
    log_event('APP', 'INFO', 'Server started', f'CLOB: {"available" if CLOB_AVAILABLE else "not installed"}')
    print("=" * 60)
    print("  Polymarket Copy Trader")
    print("  Dashboard: http://localhost:5000")
    print(f"  CLOB client: {'Available' if CLOB_AVAILABLE else 'Not installed (mock only)'}")
    print("  Copy engine running in background")
    print("=" * 60)
    app.run(debug=False, port=5000, threaded=True)
