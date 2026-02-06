"""
Polymarket Real-Time Trade Fetcher
Uses WebSocket (wss://ws-subscriptions-clob.polymarket.com) for near-instant
trade detection, with fast REST polling (every 5s) as fallback.
"""
import requests
import sqlite3
import time
import json
import threading
from datetime import datetime

try:
    from websocket import WebSocketApp
    WS_AVAILABLE = True
except ImportError:
    WS_AVAILABLE = False
    print("WARNING: websocket-client not installed. Using REST polling only.")
    print("Install for real-time: pip install websocket-client")

DATABASE = 'polymarket_trades.db'


def log_event(level, message, details=''):
    """Write to unified_log table so the UI can display fetcher events"""
    try:
        conn = sqlite3.connect(DATABASE, timeout=30)
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA busy_timeout=30000')
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(
            'INSERT INTO unified_log (timestamp, source, level, message, details) VALUES (?, ?, ?, ?, ?)',
            (datetime.now().isoformat(), 'FETCHER', level, message, details)
        )
        conn.commit()
        conn.close()
    except Exception:
        pass  # Table may not exist yet if app.py hasn't run


# Polymarket API endpoints
ACTIVITY_API = 'https://data-api.polymarket.com/activity'
PROFILE_API = 'https://gamma-api.polymarket.com/public-profile'
WSS_URL = 'wss://ws-subscriptions-clob.polymarket.com/ws/market'

# Polling interval in seconds (used as fallback alongside websocket)
FAST_POLL_INTERVAL = 5
PROFILE_REFRESH_INTERVAL = 300  # Refresh profiles every 5 minutes

# Track last known trade per trader for fast dedup
last_trade_timestamps = {}


def get_db():
    """Get database connection with WAL mode and timeout for concurrency"""
    conn = sqlite3.connect(DATABASE, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA busy_timeout=30000')
    return conn


def get_tracked_traders():
    """Get all tracked traders from the database"""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('SELECT id, wallet_address, name FROM traders')
    traders = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return traders


def fetch_trader_profile(wallet_address):
    """Fetch trader profile from Polymarket API"""
    try:
        response = requests.get(
            PROFILE_API,
            params={'address': wallet_address},
            timeout=10
        )
        if response.status_code == 200:
            return response.json()
        return None
    except Exception as e:
        print(f"  Error fetching profile for {wallet_address[:10]}...: {e}")
        return None


def update_trader_profile(trader_id, wallet_address, conn):
    """Update trader profile in database"""
    profile = fetch_trader_profile(wallet_address)
    if profile:
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE traders SET
                name = ?, pseudonym = ?, bio = ?, profile_image = ?,
                x_username = ?, verified_badge = ?, created_at = ?
            WHERE id = ?
        ''', (
            profile.get('name'),
            profile.get('pseudonym'),
            profile.get('bio'),
            profile.get('profileImage'),
            profile.get('xUsername'),
            1 if profile.get('verifiedBadge') else 0,
            profile.get('createdAt'),
            trader_id
        ))
        conn.commit()
        print(f"  Updated profile for {wallet_address[:10]}...")


def detect_trade_direction(trader_id, trade, conn):
    """
    Detect if a trade is opening or closing a position.
    BUY = OPEN LONG (adding to position)
    SELL = CLOSE LONG (reducing/closing position)
    Also check if SELL size >= total bought -> fully closing.
    """
    side = (trade.get('side') or '').upper()
    condition_id = trade.get('conditionId') or ''
    outcome = trade.get('outcome') or ''

    if side == 'BUY':
        return 'OPEN'  # Opening or adding to a long position
    elif side == 'SELL':
        # Check if there are existing BUY trades for this market
        if condition_id:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT COALESCE(SUM(CASE WHEN side='BUY' THEN CAST(size AS REAL) ELSE 0 END), 0) as bought,
                       COALESCE(SUM(CASE WHEN side='SELL' THEN CAST(size AS REAL) ELSE 0 END), 0) as sold
                FROM trades WHERE trader_id = ? AND condition_id = ? AND outcome = ?
            ''', (trader_id, condition_id, outcome))
            row = cursor.fetchone()
            if row:
                net = (row['bought'] or 0) - (row['sold'] or 0)
                sell_size = float(trade.get('size') or 0)
                if net <= 0 or sell_size >= net:
                    return 'CLOSE'  # Fully closing position
                else:
                    return 'REDUCE'  # Partially reducing position
        return 'CLOSE'
    return 'UNKNOWN'


def insert_trade(trader_id, trade, conn):
    """Insert a single trade into the database. Returns True if new."""
    tx_hash = trade.get('transactionHash')
    if not tx_hash:
        return False

    cursor = conn.cursor()
    cursor.execute('SELECT id FROM trades WHERE transaction_hash = ?', (tx_hash,))
    if cursor.fetchone():
        return False

    # Detect position direction
    direction = detect_trade_direction(trader_id, trade, conn)

    cursor.execute('''
        INSERT INTO trades (
            trader_id, transaction_hash, side, size, price, usdc_size,
            timestamp, title, slug, icon, event_slug, outcome,
            outcome_index, condition_id, asset, direction
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        trader_id, tx_hash,
        trade.get('side'), trade.get('size'), trade.get('price'),
        trade.get('usdcSize'), trade.get('timestamp'),
        trade.get('title'), trade.get('slug'), trade.get('icon'),
        trade.get('eventSlug'), trade.get('outcome'),
        trade.get('outcomeIndex'), trade.get('conditionId'),
        trade.get('asset'), direction
    ))
    conn.commit()
    return True


def fetch_trades_for_trader(trader_id, wallet_address, conn, limit=50):
    """Fetch recent trades for a specific trader via REST API"""
    try:
        response = requests.get(
            ACTIVITY_API,
            params={'user': wallet_address, 'type': 'TRADE', 'limit': limit},
            timeout=15
        )
        if response.status_code != 200:
            return 0

        trades = response.json()
        new_count = 0
        for trade in trades:
            if insert_trade(trader_id, trade, conn):
                new_count += 1
                side = trade.get('side', '?')
                title = trade.get('title', 'Unknown')[:40]
                size = trade.get('usdcSize', 0)
                print(f"    NEW TRADE: {side} ${size} on {title}")
                log_event('TRADE', f'Detected: {side} ${size} on {title}')
        return new_count
    except Exception as e:
        print(f"  Error fetching trades for {wallet_address[:10]}...: {e}")
        log_event('ERROR', f'Fetch error for {wallet_address[:10]}', str(e))
        return 0


# ---------------------------------------------------------------------------
# REST Polling (fast interval - every 5 seconds)
# ---------------------------------------------------------------------------

def fast_poll_loop():
    """Poll REST API every 5 seconds for new trades"""
    print(f"[{datetime.now()}] REST polling started (every {FAST_POLL_INTERVAL}s)")
    last_profile_refresh = time.time()

    while True:
        try:
            traders = get_tracked_traders()
            if not traders:
                time.sleep(FAST_POLL_INTERVAL)
                continue

            conn = get_db()
            total_new = 0

            for trader in traders:
                tid = trader['id']
                addr = trader['wallet_address']
                name = trader['name'] or addr[:10] + '...'

                # Refresh profile periodically
                now = time.time()
                if not trader['name'] and (now - last_profile_refresh > PROFILE_REFRESH_INTERVAL):
                    update_trader_profile(tid, addr, conn)

                new = fetch_trades_for_trader(tid, addr, conn, limit=20)
                total_new += new

                # Tiny delay to avoid rate-limiting
                time.sleep(0.3)

            if total_new > 0:
                print(f"[{datetime.now()}] Poll found {total_new} new trade(s)")
                log_event('INFO', f'Poll found {total_new} new trade(s)')

            # Refresh profiles periodically
            if time.time() - last_profile_refresh > PROFILE_REFRESH_INTERVAL:
                last_profile_refresh = time.time()

            conn.close()
        except Exception as e:
            print(f"[{datetime.now()}] Poll error: {e}")

        time.sleep(FAST_POLL_INTERVAL)


# ---------------------------------------------------------------------------
# WebSocket Real-Time Stream
# ---------------------------------------------------------------------------

class PolymarketWSTracker:
    """
    Connects to Polymarket's WebSocket to get real-time last_trade_price
    events. When a trade fires on a market our tracked traders are in,
    we immediately re-fetch that trader's recent activity via REST to
    capture the exact trade details.
    """

    def __init__(self):
        self.ws = None
        self.subscribed_assets = set()
        self.asset_to_traders = {}  # asset_id -> [(trader_id, wallet)]
        self.running = False

    def refresh_subscriptions(self):
        """Scan the DB for tracked traders' known assets and subscribe"""
        conn = get_db()
        cursor = conn.cursor()

        # Get all unique asset IDs from trades of tracked traders
        cursor.execute('''
            SELECT DISTINCT t.asset, t.condition_id, t.trader_id, tr.wallet_address
            FROM trades t
            JOIN traders tr ON t.trader_id = tr.id
            WHERE t.asset IS NOT NULL
        ''')
        rows = cursor.fetchall()

        new_assets = set()
        self.asset_to_traders = {}
        for row in rows:
            asset_id = row['asset']
            if asset_id:
                new_assets.add(asset_id)
                if asset_id not in self.asset_to_traders:
                    self.asset_to_traders[asset_id] = []
                self.asset_to_traders[asset_id].append(
                    (row['trader_id'], row['wallet_address'])
                )

        conn.close()

        # Subscribe to new assets
        to_subscribe = new_assets - self.subscribed_assets
        if to_subscribe and self.ws:
            try:
                msg = json.dumps({
                    'assets_ids': list(to_subscribe),
                    'operation': 'subscribe',
                    'custom_feature_enabled': True
                })
                self.ws.send(msg)
                self.subscribed_assets.update(to_subscribe)
                print(f"[WS] Subscribed to {len(to_subscribe)} new asset(s) (total: {len(self.subscribed_assets)})")
            except Exception as e:
                print(f"[WS] Subscribe error: {e}")

    def on_message(self, ws, message):
        """Handle incoming WebSocket messages"""
        try:
            data = json.loads(message)
            event_type = data.get('event_type', '')

            # We care about last_trade_price events - means a trade just happened
            if event_type == 'last_trade_price':
                asset_id = data.get('asset_id', '')
                price = data.get('price', '?')
                side = data.get('side', '?')
                size = data.get('size', '?')
                print(f"[WS] Trade detected: {side} {size} @ {price} on asset {asset_id[:20]}...")

                # Check if any tracked trader is in this market
                traders_in_market = self.asset_to_traders.get(asset_id, [])
                if traders_in_market:
                    # Immediately fetch fresh trades for those traders
                    conn = get_db()
                    for trader_id, wallet in traders_in_market:
                        new = fetch_trades_for_trader(trader_id, wallet, conn, limit=10)
                        if new > 0:
                            print(f"[WS] Captured {new} trade(s) for tracked trader {wallet[:10]}...")
                    conn.close()

        except json.JSONDecodeError:
            pass
        except Exception as e:
            print(f"[WS] Message error: {e}")

    def on_open(self, ws):
        """WebSocket connected"""
        print(f"[WS] Connected to Polymarket WebSocket")
        log_event('INFO', 'WebSocket connected to Polymarket')
        self.running = True

        # Subscribe to known assets
        self.refresh_subscriptions()

        # Periodically refresh subscriptions (new traders / new markets)
        def subscription_refresher():
            while self.running:
                time.sleep(30)
                try:
                    self.refresh_subscriptions()
                except Exception as e:
                    print(f"[WS] Refresh error: {e}")

        t = threading.Thread(target=subscription_refresher, daemon=True)
        t.start()

    def on_close(self, ws, close_status_code, close_msg):
        """WebSocket disconnected"""
        print(f"[WS] Disconnected (code={close_status_code}). Reconnecting in 5s...")
        self.running = False
        self.subscribed_assets.clear()
        time.sleep(5)
        self.connect()

    def on_error(self, ws, error):
        """WebSocket error"""
        print(f"[WS] Error: {error}")

    def connect(self):
        """Connect to the Polymarket WebSocket"""
        print(f"[WS] Connecting to {WSS_URL}...")

        self.ws = WebSocketApp(
            WSS_URL,
            on_open=self.on_open,
            on_message=self.on_message,
            on_close=self.on_close,
            on_error=self.on_error
        )
        self.ws.run_forever(
            ping_interval=30,
            ping_timeout=10
        )


def run_websocket():
    """Run the WebSocket tracker in a thread"""
    tracker = PolymarketWSTracker()
    tracker.connect()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_fetcher():
    print("=" * 60)
    print("  Polymarket Real-Time Trade Fetcher")
    print("=" * 60)
    print()
    log_event('INFO', 'Fetcher started', f'WS: {"available" if WS_AVAILABLE else "unavailable"}')

    # Initial fetch for all traders
    traders = get_tracked_traders()
    if traders:
        print(f"Tracking {len(traders)} trader(s). Running initial fetch...")
        conn = get_db()
        for trader in traders:
            tid = trader['id']
            addr = trader['wallet_address']
            name = trader['name'] or addr[:10] + '...'

            if not trader['name']:
                update_trader_profile(tid, addr, conn)

            new = fetch_trades_for_trader(tid, addr, conn, limit=100)
            print(f"  {name}: {new} new trade(s)")
            time.sleep(0.5)
        conn.close()
        print()
    else:
        print("No traders tracked yet. Add traders via the web UI.\n")

    # Start REST fast-poll in a background thread
    poll_thread = threading.Thread(target=fast_poll_loop, daemon=True)
    poll_thread.start()

    # Start WebSocket in the main thread (or fall back to just polling)
    if WS_AVAILABLE:
        print(f"[{datetime.now()}] Starting WebSocket real-time stream + REST polling ({FAST_POLL_INTERVAL}s)")
        print("Press Ctrl+C to stop.\n")
        try:
            run_websocket()
        except KeyboardInterrupt:
            print("\nShutting down...")
    else:
        print(f"[{datetime.now()}] Running REST polling only ({FAST_POLL_INTERVAL}s)")
        print("Install websocket-client for real-time: pip install websocket-client")
        print("Press Ctrl+C to stop.\n")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nShutting down...")


if __name__ == '__main__':
    run_fetcher()
