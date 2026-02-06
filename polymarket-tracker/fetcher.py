"""
Polymarket Trade Fetcher - Alchemy Edition
Uses Alchemy's alchemy_getAssetTransfers on Polygon to track ERC-1155
outcome-token transfers through the Polymarket CTF Exchange contracts.
Falls back to the Polymarket Activity REST API when Alchemy data needs
enrichment.
"""
import requests
import sqlite3
import time
import json
import os
import threading
from datetime import datetime

DATABASE = 'polymarket_trades.db'

# Serialised write lock to prevent "database is locked" errors
_db_write_lock = threading.Lock()

# ---------------------------------------------------------------------------
# Alchemy configuration (env var first, then config.json fallback)
# ---------------------------------------------------------------------------
def _resolve_alchemy_key():
    key = os.environ.get('ALCHEMY_API_KEY', '')
    if not key:
        try:
            with open('config.json', 'r') as f:
                key = json.load(f).get('alchemy_api_key', '')
            if key:
                os.environ['ALCHEMY_API_KEY'] = key
        except Exception:
            pass
    return key

ALCHEMY_API_KEY = _resolve_alchemy_key()
ALCHEMY_POLYGON_URL = f'https://polygon-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}'

# Polymarket contract addresses on Polygon
CTF_EXCHANGE_NEGRISK = '0xC5d563A36AE78145C45a50134d48A1215220f80a'.lower()
CTF_EXCHANGE_LEGACY  = '0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E'.lower()
NEG_RISK_ADAPTER     = '0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296'.lower()
CTF_CONTRACT         = '0x4D97DCd97eC945f40cF65F87097ACe5EA0476045'.lower()
USDC_POLYGON         = '0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174'.lower()
USDCE_POLYGON        = '0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359'.lower()

POLYMARKET_CONTRACTS = {CTF_EXCHANGE_NEGRISK, CTF_EXCHANGE_LEGACY, NEG_RISK_ADAPTER}

# Polymarket REST fallback
ACTIVITY_API = 'https://data-api.polymarket.com/activity'
PROFILE_API  = 'https://gamma-api.polymarket.com/public-profile'
GAMMA_MARKETS_API = 'https://gamma-api.polymarket.com/markets'

# Polling intervals
POLL_INTERVAL = 8  # seconds between polls

# Cache: wallet_address -> proxy_wallet
_proxy_wallet_cache = {}

# Cache: condition_id/token_id -> market metadata
_market_cache = {}


def log_event(level, message, details=''):
    """Write to unified_log table so the UI can display fetcher events."""
    try:
        with _db_write_lock:
            conn = get_db()
            try:
                cursor = conn.cursor()
                cursor.execute(
                    'INSERT INTO unified_log (timestamp, source, level, message, details) VALUES (?, ?, ?, ?, ?)',
                    (datetime.now().isoformat(), 'FETCHER', level, message, details)
                )
                conn.commit()
            finally:
                conn.close()
    except Exception:
        pass


def get_db():
    conn = sqlite3.connect(DATABASE, timeout=60, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA busy_timeout=60000')
    conn.execute('PRAGMA synchronous=NORMAL')
    return conn


def get_tracked_traders():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('SELECT id, wallet_address, name FROM traders')
    traders = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return traders


# ---------------------------------------------------------------------------
# Profile / proxy-wallet resolution
# ---------------------------------------------------------------------------

def fetch_trader_profile(wallet_address):
    try:
        r = requests.get(PROFILE_API, params={'address': wallet_address}, timeout=10)
        if r.status_code == 200:
            data = r.json()
            proxy = (data.get('proxyWallet') or data.get('proxy_wallet') or '').lower()
            if proxy:
                _proxy_wallet_cache[wallet_address.lower()] = proxy
                print(f"  Resolved proxy for {wallet_address[:10]}...: {proxy[:10]}...")
            return data
    except Exception as e:
        print(f"  Profile error for {wallet_address[:10]}...: {e}")
    return None


def resolve_proxy_wallet(wallet_address):
    addr = wallet_address.lower()
    if addr in _proxy_wallet_cache:
        return _proxy_wallet_cache[addr]
    profile = fetch_trader_profile(addr)
    if profile:
        proxy = (profile.get('proxyWallet') or profile.get('proxy_wallet') or '').lower()
        if proxy:
            _proxy_wallet_cache[addr] = proxy
            return proxy
    return None


def update_trader_profile(trader_id, wallet_address, conn):
    profile = fetch_trader_profile(wallet_address)
    if profile:
        with _db_write_lock:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE traders SET
                    name = ?, pseudonym = ?, bio = ?, profile_image = ?,
                    x_username = ?, verified_badge = ?, created_at = ?
                WHERE id = ?
            ''', (
                profile.get('name'), profile.get('pseudonym'), profile.get('bio'),
                profile.get('profileImage'), profile.get('xUsername'),
                1 if profile.get('verifiedBadge') else 0, profile.get('createdAt'),
                trader_id
            ))
            conn.commit()


# ---------------------------------------------------------------------------
# Market metadata enrichment (Gamma API)
# ---------------------------------------------------------------------------

def get_market_by_condition(condition_id):
    """Fetch market info from Gamma API by condition_id."""
    if not condition_id:
        return None
    if condition_id in _market_cache:
        return _market_cache[condition_id]
    try:
        r = requests.get(GAMMA_MARKETS_API, params={'condition_id': condition_id}, timeout=10)
        if r.status_code == 200:
            data = r.json()
            markets = data if isinstance(data, list) else [data] if isinstance(data, dict) else []
            if markets:
                m = markets[0]
                info = {
                    'title': m.get('question') or m.get('title') or '',
                    'slug': m.get('slug') or '',
                    'icon': m.get('icon') or '',
                    'event_slug': m.get('eventSlug') or '',
                    'end_date': m.get('endDate') or m.get('end_date_iso') or '',
                    'outcomes': m.get('outcomes') or '[]',
                    'condition_id': m.get('conditionId') or condition_id,
                }
                _market_cache[condition_id] = info
                return info
    except Exception:
        pass
    return None


def get_market_by_token(token_id):
    """Fetch market info from Gamma API by token_id (asset / ERC1155 id)."""
    cache_key = f'token:{token_id}'
    if cache_key in _market_cache:
        return _market_cache[cache_key]
    try:
        # token_id maps to Gamma's clob_token_id
        r = requests.get(GAMMA_MARKETS_API, params={'clob_token_id': token_id}, timeout=10)
        if r.status_code == 200:
            data = r.json()
            markets = data if isinstance(data, list) else [data] if isinstance(data, dict) else []
            if markets:
                m = markets[0]
                # Determine which outcome this token represents
                outcomes_raw = m.get('outcomes', '[]')
                try:
                    outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else (outcomes_raw or [])
                except Exception:
                    outcomes = []
                clobTokenIds = m.get('clobTokenIds') or ''
                try:
                    token_ids = json.loads(clobTokenIds) if isinstance(clobTokenIds, str) else (clobTokenIds or [])
                except Exception:
                    token_ids = []
                outcome = ''
                outcome_index = 0
                for i, tid in enumerate(token_ids):
                    if str(tid) == str(token_id):
                        outcome = outcomes[i] if i < len(outcomes) else ''
                        outcome_index = i
                        break

                info = {
                    'title': m.get('question') or m.get('title') or '',
                    'slug': m.get('slug') or '',
                    'icon': m.get('icon') or '',
                    'event_slug': m.get('eventSlug') or '',
                    'end_date': m.get('endDate') or m.get('end_date_iso') or '',
                    'condition_id': m.get('conditionId') or '',
                    'outcome': outcome,
                    'outcome_index': outcome_index,
                    'asset': token_id,
                }
                _market_cache[cache_key] = info
                return info
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Alchemy - fetch ERC-1155 transfers (Polymarket outcome tokens)
# ---------------------------------------------------------------------------

def _alchemy_rpc(method, params):
    """Make a JSON-RPC call to Alchemy Polygon endpoint."""
    payload = {
        'jsonrpc': '2.0',
        'id': 1,
        'method': method,
        'params': params,
    }
    r = requests.post(ALCHEMY_POLYGON_URL, json=payload, timeout=20)
    r.raise_for_status()
    return r.json().get('result', {})


def fetch_erc1155_transfers(wallet, direction='to', from_block='0x0', page_key=None):
    """
    Fetch ERC-1155 transfers to/from a wallet using Alchemy.
    direction='to'   -> tokens received (BUY side)
    direction='from' -> tokens sent (SELL side)
    """
    params = {
        'category': ['erc1155'],
        'withMetadata': True,
        'excludeZeroValue': True,
        'maxCount': '0x64',  # 100
        'fromBlock': from_block,
        'toBlock': 'latest',
        'order': 'desc',
    }
    if direction == 'to':
        params['toAddress'] = wallet
    else:
        params['fromAddress'] = wallet

    if page_key:
        params['pageKey'] = page_key

    result = _alchemy_rpc('alchemy_getAssetTransfers', [params])
    return result.get('transfers', []), result.get('pageKey', '')


def fetch_usdc_value_from_tx(tx_hash, wallet):
    """
    Get the USDC amount from a transaction receipt by looking at ERC-20
    Transfer events involving the wallet.
    """
    try:
        result = _alchemy_rpc('eth_getTransactionReceipt', [tx_hash])
        if not result or not result.get('logs'):
            return 0.0

        # ERC-20 Transfer event topic
        transfer_topic = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
        usdc_addrs = {USDC_POLYGON, USDCE_POLYGON}
        wallet_padded = '0x' + wallet.replace('0x', '').lower().zfill(64)

        total_usdc = 0.0
        for log in result.get('logs', []):
            contract = (log.get('address') or '').lower()
            topics = log.get('topics', [])
            if contract not in usdc_addrs or len(topics) < 3:
                continue
            if topics[0].lower() != transfer_topic:
                continue
            # Check if wallet is sender or receiver
            from_addr = topics[1].lower()
            to_addr = topics[2].lower()
            if from_addr == wallet_padded or to_addr == wallet_padded:
                raw = int(log.get('data', '0x0'), 16)
                total_usdc += raw / 1e6  # USDC has 6 decimals
        return round(total_usdc, 4)
    except Exception:
        return 0.0


def process_alchemy_transfers(trader_id, wallet, transfers, side, conn):
    """
    Process Alchemy ERC-1155 transfers and insert as trades.
    side='BUY' for tokens received, 'SELL' for tokens sent.
    Returns count of new trades inserted.
    """
    new_count = 0
    for tx in transfers:
        tx_hash = tx.get('hash', '')
        if not tx_hash:
            continue

        # Skip if not involving a Polymarket contract
        from_addr = (tx.get('from') or '').lower()
        to_addr = (tx.get('to') or '').lower()
        if from_addr not in POLYMARKET_CONTRACTS and to_addr not in POLYMARKET_CONTRACTS:
            continue

        with _db_write_lock:
            cursor = conn.cursor()
            cursor.execute('SELECT id FROM trades WHERE transaction_hash = ?', (tx_hash,))
            if cursor.fetchone():
                continue

        # Extract token info
        erc1155_meta = tx.get('erc1155Metadata') or []
        if not erc1155_meta:
            continue

        token_id = erc1155_meta[0].get('tokenId', '')
        token_value_hex = erc1155_meta[0].get('value', '0x0')
        try:
            size = int(token_value_hex, 16) / 1e6 if token_value_hex.startswith('0x') else float(token_value_hex)
        except Exception:
            size = 0.0

        # Get USDC value from the tx receipt
        usdc_size = fetch_usdc_value_from_tx(tx_hash, wallet)

        # Calculate price (USDC per token)
        price = usdc_size / size if size > 0 else 0.0

        # Get market metadata from Gamma API using the token_id
        # Convert hex token_id to decimal for Gamma API lookup
        try:
            token_id_dec = str(int(token_id, 16)) if token_id.startswith('0x') else token_id
        except Exception:
            token_id_dec = token_id

        market = get_market_by_token(token_id_dec)

        title = ''
        slug = ''
        icon = ''
        event_slug = ''
        outcome = ''
        outcome_index = 0
        condition_id = ''
        end_date = ''

        if market:
            title = market.get('title', '')
            slug = market.get('slug', '')
            icon = market.get('icon', '')
            event_slug = market.get('event_slug', '')
            outcome = market.get('outcome', '')
            outcome_index = market.get('outcome_index', 0)
            condition_id = market.get('condition_id', '')
            end_date = market.get('end_date', '')

        # Get timestamp from metadata
        block_ts = ''
        meta = tx.get('metadata', {})
        if meta and meta.get('blockTimestamp'):
            block_ts = meta['blockTimestamp']

        # Convert ISO timestamp to unix epoch (seconds)
        timestamp_unix = 0
        if block_ts:
            try:
                dt = datetime.fromisoformat(block_ts.replace('Z', '+00:00'))
                timestamp_unix = int(dt.timestamp())
            except Exception:
                pass

        direction = 'OPEN' if side == 'BUY' else 'CLOSE'

        with _db_write_lock:
            cursor = conn.cursor()
            # Recheck to avoid race
            cursor.execute('SELECT id FROM trades WHERE transaction_hash = ?', (tx_hash,))
            if cursor.fetchone():
                continue
            cursor.execute('''
                INSERT INTO trades (
                    trader_id, transaction_hash, side, size, price, usdc_size,
                    timestamp, title, slug, icon, event_slug, outcome,
                    outcome_index, condition_id, asset, direction, end_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                trader_id, tx_hash, side, size, price, usdc_size,
                timestamp_unix, title, slug, icon, event_slug, outcome,
                outcome_index, condition_id, token_id_dec,
                direction, end_date
            ))
            conn.commit()
        new_count += 1
        price_c = f"{price*100:.0f}c" if price else '?'
        print(f"    NEW: {side} {outcome or '?'} {size:.2f} @ {price_c} ${usdc_size:.2f} | {title[:40]}")
        log_event('TRADE', f'{side} {outcome} ${usdc_size:.2f} @ {price_c} on {title[:40]}')

    return new_count


# ---------------------------------------------------------------------------
# Polymarket Activity API fallback (original approach)
# ---------------------------------------------------------------------------

def _fetch_activity(address, limit=50):
    try:
        r = requests.get(ACTIVITY_API, params={'user': address, 'type': 'TRADE', 'limit': limit}, timeout=15)
        if r.status_code == 200:
            data = r.json()
            return data if isinstance(data, list) else []
    except Exception:
        pass
    return []


def fetch_end_date_for_condition(condition_id):
    if not condition_id:
        return ''
    info = get_market_by_condition(condition_id)
    return info.get('end_date', '') if info else ''


def insert_trade_from_activity(trader_id, trade, conn):
    """Insert a trade from Polymarket Activity API format. Returns True if new."""
    tx_hash = trade.get('transactionHash')
    if not tx_hash:
        return False
    with _db_write_lock:
        cursor = conn.cursor()
        cursor.execute('SELECT id FROM trades WHERE transaction_hash = ?', (tx_hash,))
        if cursor.fetchone():
            return False

        side_raw = (trade.get('side') or '').upper()
        direction = 'OPEN' if side_raw == 'BUY' else 'CLOSE'
        cid = trade.get('conditionId') or ''
        end_date = fetch_end_date_for_condition(cid)

        cursor.execute('''
            INSERT INTO trades (
                trader_id, transaction_hash, side, size, price, usdc_size,
                timestamp, title, slug, icon, event_slug, outcome,
                outcome_index, condition_id, asset, direction, end_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            trader_id, tx_hash,
            trade.get('side'), trade.get('size'), trade.get('price'),
            trade.get('usdcSize'), trade.get('timestamp'),
            trade.get('title'), trade.get('slug'), trade.get('icon'),
            trade.get('eventSlug'), trade.get('outcome'),
            trade.get('outcomeIndex'), cid,
            trade.get('asset'), direction, end_date
        ))
        conn.commit()
    return True


# ---------------------------------------------------------------------------
# Combined fetch: Alchemy first, Activity API fallback
# ---------------------------------------------------------------------------

def fetch_trades_for_trader(trader_id, wallet_address, conn, limit=50):
    """
    Fetch trades for a trader.
    Strategy:
    1. Try Alchemy ERC-1155 transfers (most reliable)
    2. Fallback to Polymarket Activity API
    """
    total_new = 0

    # Resolve proxy wallet (Polymarket uses proxy wallets for on-chain trades)
    proxy = resolve_proxy_wallet(wallet_address)
    trading_wallet = proxy or wallet_address.lower()

    # --- Method 1: Alchemy ---
    if ALCHEMY_API_KEY:
        try:
            # Fetch tokens received (BUY)
            buys, _ = fetch_erc1155_transfers(trading_wallet, direction='to')
            new_buys = process_alchemy_transfers(trader_id, trading_wallet, buys, 'BUY', conn)
            total_new += new_buys

            # Fetch tokens sent (SELL)
            sells, _ = fetch_erc1155_transfers(trading_wallet, direction='from')
            new_sells = process_alchemy_transfers(trader_id, trading_wallet, sells, 'SELL', conn)
            total_new += new_sells

            if total_new > 0:
                print(f"  [Alchemy] {total_new} new trade(s) for {wallet_address[:10]}... (proxy: {trading_wallet[:10]}...)")
            return total_new
        except Exception as e:
            print(f"  [Alchemy] Error for {wallet_address[:10]}...: {e}")
            log_event('WARN', f'Alchemy error, falling back to Activity API', str(e))

    # --- Method 2: Polymarket Activity API fallback ---
    try:
        addresses = [wallet_address.lower()]
        if proxy and proxy != wallet_address.lower():
            addresses.append(proxy)

        for addr in addresses:
            trades = _fetch_activity(addr, limit)
            for trade in trades:
                if insert_trade_from_activity(trader_id, trade, conn):
                    total_new += 1
                    side = trade.get('side', '?')
                    outcome = trade.get('outcome', '')
                    title = trade.get('title', 'Unknown')[:40]
                    usdc = trade.get('usdcSize', 0)
                    price = trade.get('price', 0)
                    price_c = f"{float(price)*100:.0f}c" if price else '?'
                    print(f"    NEW: {side} {outcome} ${usdc} @ {price_c} on {title}")
                    log_event('TRADE', f'{side} {outcome} ${usdc} @ {price_c} on {title}')
            time.sleep(0.3)

        if total_new > 0:
            print(f"  [Activity API] {total_new} new trade(s) for {wallet_address[:10]}...")
    except Exception as e:
        print(f"  [Activity API] Error for {wallet_address[:10]}...: {e}")
        log_event('ERROR', f'Fetch error for {wallet_address[:10]}', str(e))

    return total_new


# ---------------------------------------------------------------------------
# Polling loop
# ---------------------------------------------------------------------------

def poll_loop():
    print(f"[{datetime.now()}] Polling started (every {POLL_INTERVAL}s)")
    print(f"  Alchemy: {'configured' if ALCHEMY_API_KEY else 'NOT SET - using Activity API fallback'}")
    if not ALCHEMY_API_KEY:
        print("  Set ALCHEMY_API_KEY env var for reliable on-chain tracking")

    last_profile_refresh = time.time()

    while True:
        try:
            traders = get_tracked_traders()
            if not traders:
                time.sleep(POLL_INTERVAL)
                continue

            conn = get_db()
            total_new = 0

            for trader in traders:
                tid = trader['id']
                addr = trader['wallet_address']
                name = trader['name'] or addr[:10] + '...'

                # Refresh profile periodically
                if not trader['name'] and (time.time() - last_profile_refresh > 300):
                    update_trader_profile(tid, addr, conn)

                new = fetch_trades_for_trader(tid, addr, conn, limit=20)
                total_new += new
                time.sleep(0.5)

            if total_new > 0:
                print(f"[{datetime.now()}] Poll found {total_new} new trade(s)")
                log_event('INFO', f'Poll found {total_new} new trade(s)')

            if time.time() - last_profile_refresh > 300:
                last_profile_refresh = time.time()

            conn.close()
        except Exception as e:
            print(f"[{datetime.now()}] Poll error: {e}")

        time.sleep(POLL_INTERVAL)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_fetcher():
    print("=" * 60)
    print("  Polymarket Trade Fetcher (Alchemy + Activity API)")
    print("=" * 60)
    mode = 'Alchemy (Polygon)' if ALCHEMY_API_KEY else 'Activity API only'
    print(f"  Mode: {mode}")
    log_event('INFO', 'Fetcher started', f'Mode: {mode}')

    # Initial fetch for all traders
    traders = get_tracked_traders()
    if traders:
        print(f"  Tracking {len(traders)} trader(s). Running initial fetch...")
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
        print("  No traders tracked yet. Add traders via the web UI.\n")

    # Start polling
    poll_loop()


if __name__ == '__main__':
    run_fetcher()
