"""
Polymarket Trade Fetcher - Polygon RPC Edition
Uses free public Polygon RPCs (polygon-rpc.com) to poll on-chain
transactions for tracked wallets.  Falls back to the Polymarket
Data API REST endpoints for trade enrichment.
"""
import requests
import time
import json
import os
import threading
from datetime import datetime

from db import get_db, get_conn, get_write_conn, db_write, DATABASE, _write_lock as _db_write_lock

# ---------------------------------------------------------------------------
# Polygon RPC configuration
# ---------------------------------------------------------------------------
POLYGON_RPCS = [
    'https://polygon-rpc.com',
    'https://rpc.ankr.com/polygon',
    'https://polygon.llamarpc.com',
]

# Polymarket contract addresses on Polygon
CTF_EXCHANGE_NEGRISK = '0xC5d563A36AE78145C45a50134d48A1215220f80a'.lower()
CTF_EXCHANGE_LEGACY  = '0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E'.lower()
NEG_RISK_ADAPTER     = '0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296'.lower()
CTF_CONTRACT         = '0x4D97DCd97eC945f40cF65F87097ACe5EA0476045'.lower()
USDC_POLYGON         = '0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174'.lower()
USDCE_POLYGON        = '0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359'.lower()

POLYMARKET_CONTRACTS = {CTF_EXCHANGE_NEGRISK, CTF_EXCHANGE_LEGACY, NEG_RISK_ADAPTER, CTF_CONTRACT}

# Polymarket REST fallback
ACTIVITY_API = 'https://data-api.polymarket.com/activity'
TRADES_API   = 'https://data-api.polymarket.com/trades'
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
        with get_write_conn() as conn:
            conn.execute(
                'INSERT INTO unified_log (timestamp, source, level, message, details) VALUES (?, ?, ?, ?, ?)',
                (datetime.now().isoformat(), 'FETCHER', level, message, details)
            )
    except Exception:
        pass


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
            print(f"  [Proxy] {addr[:10]}... -> proxy {proxy[:10]}...")
            return proxy
    # If no proxy found, the wallet itself might BE the proxy wallet
    # (users sometimes paste the proxy address from Polygonscan)
    print(f"  [Proxy] No proxy found for {addr[:10]}... - will scan this address directly")
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
# Polygon RPC On-Chain Polling
# Scans recent blocks for Polymarket contract interactions by watched
# wallets.  No third-party API key needed -- uses free public RPCs.
# ---------------------------------------------------------------------------

# Track the last scanned block number so we only process new blocks
_last_scanned_block = 0
_rpc_watched_addresses = set()
_rpc_tx_queue = []
_rpc_tx_queue_lock = threading.Lock()
_rpc_poll_running = False


def rpc_watch_address(address):
    """Register a wallet address for RPC polling."""
    _rpc_watched_addresses.add(address.lower())
    return True


def rpc_drain_queue():
    """Drain and return all queued transactions discovered via RPC polling."""
    with _rpc_tx_queue_lock:
        txns = list(_rpc_tx_queue)
        _rpc_tx_queue.clear()
    return txns


def _rpc_get_latest_block():
    """Get the latest block number from Polygon RPC."""
    result = _polygon_rpc('eth_blockNumber', [])
    if result:
        return int(result, 16)
    return 0


def _rpc_get_block_txns(block_num):
    """Fetch a full block with transactions."""
    hex_block = hex(block_num)
    result = _polygon_rpc('eth_getBlockByNumber', [hex_block, True])
    if result and isinstance(result, dict):
        return result.get('transactions', [])
    return []


def _rpc_scan_blocks(wallets, from_block, to_block):
    """
    Scan a range of blocks for txns from/to watched wallets that interact
    with Polymarket contracts.  Returns list of relevant tx dicts.
    """
    found = []
    wallets_set = set(w.lower() for w in wallets)
    for bn in range(from_block, to_block + 1):
        txns = _rpc_get_block_txns(bn)
        for tx in txns:
            if not isinstance(tx, dict):
                continue
            tx_from = (tx.get('from') or '').lower()
            tx_to = (tx.get('to') or '').lower()
            # Must involve one of our wallets
            if tx_from not in wallets_set and tx_to not in wallets_set:
                continue
            # Must involve a Polymarket contract
            if tx_to not in POLYMARKET_CONTRACTS and tx_from not in POLYMARKET_CONTRACTS:
                continue
            found.append(tx)
    return found


def rpc_poll_once():
    """
    Poll new blocks from Polygon RPC and queue any Polymarket txns
    for watched wallets.  Called from the main polling loop.
    """
    global _last_scanned_block
    if not _rpc_watched_addresses:
        return 0

    latest = _rpc_get_latest_block()
    if latest == 0:
        return 0

    if _last_scanned_block == 0:
        # First run: start from a few blocks back to avoid scanning
        # the entire chain.  ~2 seconds per Polygon block.
        _last_scanned_block = max(0, latest - 5)

    if latest <= _last_scanned_block:
        return 0

    # Cap scan range to avoid long-running requests
    from_block = _last_scanned_block + 1
    to_block = min(latest, from_block + 10)  # max 10 blocks per poll

    try:
        txns = _rpc_scan_blocks(_rpc_watched_addresses, from_block, to_block)
        if txns:
            with _rpc_tx_queue_lock:
                _rpc_tx_queue.extend(txns)
            print(f"  [RPC] Found {len(txns)} Polymarket tx(s) in blocks {from_block}-{to_block}")
    except Exception as e:
        print(f"  [RPC] Block scan error: {e}")

    _last_scanned_block = to_block
    return len(txns) if txns else 0


def _polygon_rpc(method, params):
    """Make a JSON-RPC call to a free public Polygon RPC."""
    payload = {'jsonrpc': '2.0', 'id': 1, 'method': method, 'params': params}
    for rpc in POLYGON_RPCS:
        try:
            r = requests.post(rpc, json=payload, timeout=15)
            r.raise_for_status()
            result = r.json()
            if 'result' in result:
                return result['result']
        except Exception:
            continue
    return {}


def fetch_usdc_value_from_tx(tx_hash, wallet):
    """
    Get the USDC amount from a transaction receipt by looking at ERC-20
    Transfer events involving the wallet. Uses free public Polygon RPCs.
    """
    try:
        result = _polygon_rpc('eth_getTransactionReceipt', [tx_hash])
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
            from_addr = topics[1].lower()
            to_addr = topics[2].lower()
            if from_addr == wallet_padded or to_addr == wallet_padded:
                raw = int(log.get('data', '0x0'), 16)
                total_usdc += raw / 1e6  # USDC has 6 decimals
        return round(total_usdc, 4)
    except Exception:
        return 0.0


def process_rpc_txns(trader_id, wallet, txns, conn):
    """
    Process on-chain transactions found via Polygon RPC polling and insert
    relevant Polymarket trades into DB. Returns count of new trades inserted.
    """
    new_count = 0
    wallet_lower = wallet.lower()

    for txn in txns:
        tx_hash = txn.get('hash', '')
        if not tx_hash:
            continue

        # Check if this involves Polymarket contracts
        to_addr = (txn.get('to') or '').lower()
        from_addr = (txn.get('from') or '').lower()
        is_polymarket = (
            to_addr in POLYMARKET_CONTRACTS or
            from_addr in POLYMARKET_CONTRACTS
        )
        if not is_polymarket:
            continue

        with _db_write_lock:
            cursor = conn.cursor()
            cursor.execute('SELECT id FROM trades WHERE transaction_hash = ?', (tx_hash,))
            if cursor.fetchone():
                continue

        # Determine side based on contract interaction
        side = 'BUY' if to_addr in POLYMARKET_CONTRACTS else 'SELL'

        # Get USDC value from tx receipt
        usdc_size = fetch_usdc_value_from_tx(tx_hash, wallet_lower)

        # Get timestamp
        timestamp_unix = txn.get('pendingTimeStamp') or txn.get('confirmedTimeStamp') or int(time.time())
        if isinstance(timestamp_unix, str):
            try:
                timestamp_unix = int(timestamp_unix)
            except ValueError:
                try:
                    dt = datetime.fromisoformat(timestamp_unix.replace('Z', '+00:00'))
                    timestamp_unix = int(dt.timestamp())
                except Exception:
                    timestamp_unix = int(time.time())

        # Try to extract token info from input data if available
        # For now, we'll use the Polymarket Data API to enrich
        direction = 'OPEN' if side == 'BUY' else 'CLOSE'

        with _db_write_lock:
            cursor = conn.cursor()
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
                trader_id, tx_hash, side, 0, 0, usdc_size,
                timestamp_unix, '', '', '', '', '',
                0, '', '', direction, ''
            ))
            conn.commit()
        new_count += 1
        print(f"    NEW [RPC]: {side} ${usdc_size:.2f} | tx {tx_hash[:16]}...")
        log_event('TRADE', f'[RPC] {side} ${usdc_size:.2f} | tx {tx_hash[:16]}')

    return new_count


# ---------------------------------------------------------------------------
# Polymarket Data API (primary source)
# ---------------------------------------------------------------------------

def _fetch_activity(address, limit=50):
    """Fetch from Data API /activity endpoint (uses profile address)."""
    try:
        r = requests.get(ACTIVITY_API, params={'user': address, 'type': 'TRADE', 'limit': limit}, timeout=15)
        if r.status_code == 200:
            data = r.json()
            return data if isinstance(data, list) else []
    except Exception:
        pass
    return []


def _fetch_trades_api(address, limit=100):
    """Fetch from Data API /trades endpoint (uses profile address)."""
    try:
        r = requests.get(TRADES_API, params={'user': address, 'limit': limit}, timeout=15)
        if r.status_code == 200:
            data = r.json()
            return data if isinstance(data, list) else []
    except Exception:
        pass
    return []


def insert_trade_from_trades_api(trader_id, trade, conn=None):
    """Insert a trade from Polymarket /trades API format. Returns True if new.
    Uses its own write connection to avoid holding caller's conn across writes."""
    tx_hash = trade.get('transactionHash')
    if not tx_hash:
        return False

    # Quick read check: does this trade already exist?
    with get_conn() as rc:
        cur = rc.cursor()
        cur.execute('SELECT id FROM trades WHERE transaction_hash = ?', (tx_hash,))
        if cur.fetchone():
            return False

    side_raw = (trade.get('side') or '').upper()
    direction = 'OPEN' if side_raw == 'BUY' else 'CLOSE'
    cid = trade.get('conditionId') or ''
    size = trade.get('size') or 0
    price = trade.get('price') or 0
    usdc_size = float(size) * float(price) if size and price else 0

    with get_write_conn() as wc:
        wc.execute('''
            INSERT OR IGNORE INTO trades (
                trader_id, transaction_hash, side, size, price, usdc_size,
                timestamp, title, slug, icon, event_slug, outcome,
                outcome_index, condition_id, asset, direction, end_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            trader_id, tx_hash,
            trade.get('side'), size, price, usdc_size,
            trade.get('timestamp'), trade.get('title'), trade.get('slug'),
            trade.get('icon'), trade.get('eventSlug'), trade.get('outcome'),
            trade.get('outcomeIndex'), cid,
            trade.get('asset'), direction, ''
        ))
    return True


def fetch_end_date_for_condition(condition_id):
    if not condition_id:
        return ''
    info = get_market_by_condition(condition_id)
    return info.get('end_date', '') if info else ''


def insert_trade_from_activity(trader_id, trade, conn=None):
    """Insert a trade from Polymarket Activity API format. Returns True if new.
    Uses its own write connection to avoid holding caller's conn across writes."""
    tx_hash = trade.get('transactionHash')
    if not tx_hash:
        return False

    # Quick read check
    with get_conn() as rc:
        cur = rc.cursor()
        cur.execute('SELECT id FROM trades WHERE transaction_hash = ?', (tx_hash,))
        if cur.fetchone():
            return False

    side_raw = (trade.get('side') or '').upper()
    direction = 'OPEN' if side_raw == 'BUY' else 'CLOSE'
    cid = trade.get('conditionId') or ''
    end_date = fetch_end_date_for_condition(cid)

    with get_write_conn() as wc:
        wc.execute('''
            INSERT OR IGNORE INTO trades (
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
    return True


# ---------------------------------------------------------------------------
# Combined fetch: Data API first, Polygon RPC on-chain fallback
# ---------------------------------------------------------------------------

def fetch_trades_for_trader(trader_id, wallet_address, conn, limit=50):
    """
    Fetch trades for a trader.
    Strategy:
    1. Try Polymarket Data API /trades endpoint (authoritative, uses profile address)
    2. Try Polymarket Data API /activity endpoint
    3. Process any on-chain transactions from Polygon RPC polling
    """
    total_new = 0
    eoa = wallet_address.lower()

    print(f"  [Fetcher] Fetching for {eoa[:10]}...")

    # --- Method 1: Polymarket /trades API (primary - most accurate) ---
    try:
        trades = _fetch_trades_api(eoa, limit)
        print(f"  [Trades API] {eoa[:10]}... returned {len(trades)} trade(s)")
        for trade in trades:
            if insert_trade_from_trades_api(trader_id, trade, conn):
                total_new += 1
                side = trade.get('side', '?')
                outcome = trade.get('outcome', '')
                title = trade.get('title', 'Unknown')[:40]
                size = trade.get('size', 0)
                price = trade.get('price', 0)
                usdc = float(size) * float(price) if size and price else 0
                price_c = f"{float(price)*100:.0f}c" if price else '?'
                print(f"    NEW: {side} {outcome} ${usdc:.2f} @ {price_c} on {title}")
                log_event('TRADE', f'{side} {outcome} ${usdc:.2f} @ {price_c} on {title}')

        if total_new > 0:
            print(f"  [Trades API] {total_new} new trade(s) for {eoa[:10]}...")
            log_event('INFO', f'Data API: {total_new} new trade(s) for {eoa[:10]}')
            return total_new
    except Exception as e:
        print(f"  [Trades API] Error for {eoa[:10]}...: {e}")

    # --- Method 2: Polymarket /activity API ---
    try:
        activities = _fetch_activity(eoa, limit)
        print(f"  [Activity API] {eoa[:10]}... returned {len(activities)} trade(s)")
        for trade in activities:
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
            print(f"  [Activity API] {total_new} new trade(s) for {eoa[:10]}...")
            log_event('INFO', f'Activity API: {total_new} new trade(s) for {eoa[:10]}')
            return total_new
    except Exception as e:
        print(f"  [Activity API] Error for {eoa[:10]}...: {e}")

    # --- Method 3: Polygon RPC on-chain polling (process queued txns) ---
    proxy = resolve_proxy_wallet(wallet_address)
    wallets_to_scan = [eoa]
    if proxy and proxy != eoa:
        wallets_to_scan.append(proxy)

    # Make sure addresses are registered for RPC polling
    for w in wallets_to_scan:
        rpc_watch_address(w)

    # Poll new blocks from the chain
    rpc_poll_once()

    # Process any queued transactions from RPC scan
    try:
        queued_txns = rpc_drain_queue()
        if queued_txns:
            relevant = [t for t in queued_txns
                        if (t.get('from') or '').lower() in wallets_to_scan
                        or (t.get('to') or '').lower() in wallets_to_scan]
            if relevant:
                for w in wallets_to_scan:
                    wallet_txns = [t for t in relevant
                                   if (t.get('from') or '').lower() == w
                                   or (t.get('to') or '').lower() == w]
                    new = process_rpc_txns(trader_id, w, wallet_txns, conn)
                    total_new += new

            if total_new > 0:
                print(f"  [RPC] {total_new} new trade(s) for {eoa[:10]}...")
                log_event('INFO', f'RPC: {total_new} new trade(s) for {eoa[:10]}')
    except Exception as e:
        print(f"  [RPC] Error for {eoa[:10]}...: {e}")
        log_event('WARN', f'RPC error for {eoa[:10]}', str(e))

    return total_new


# ---------------------------------------------------------------------------
# Polling loop
# ---------------------------------------------------------------------------

def poll_loop():
    print(f"[{datetime.now()}] Polling started (every {POLL_INTERVAL}s)")
    print(f"  Mode: Data API + Polygon RPC on-chain polling")

    last_profile_refresh = time.time()

    while True:
        try:
            traders = get_tracked_traders()
            if not traders:
                time.sleep(POLL_INTERVAL)
                continue

            total_new = 0

            for trader in traders:
                tid = trader['id']
                addr = trader['wallet_address']
                name = trader['name'] or addr[:10] + '...'

                # Refresh profile periodically -- open+close a conn just for this
                if not trader['name'] and (time.time() - last_profile_refresh > 300):
                    with get_conn() as c:
                        update_trader_profile(tid, addr, c)

                # Each trader gets its own short-lived connection
                with get_conn() as c:
                    new = fetch_trades_for_trader(tid, addr, c, limit=20)
                total_new += new
                time.sleep(0.5)

            if total_new > 0:
                print(f"[{datetime.now()}] Poll found {total_new} new trade(s)")
                log_event('INFO', f'Poll found {total_new} new trade(s)')

            if time.time() - last_profile_refresh > 300:
                last_profile_refresh = time.time()

        except Exception as e:
            print(f"[{datetime.now()}] Poll error: {e}")

        time.sleep(POLL_INTERVAL)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_fetcher():
    print("=" * 60)
    print("  Polymarket Trade Fetcher (Data API + Polygon RPC)")
    print("=" * 60)
    print(f"  Mode: Data API + Polygon RPC on-chain polling")
    print(f"  RPCs: {', '.join(POLYGON_RPCS)}")
    log_event('INFO', 'Fetcher started', 'Mode: Data API + Polygon RPC')

    # Initial fetch for all traders
    traders = get_tracked_traders()
    if traders:
        print(f"  Tracking {len(traders)} trader(s). Running initial fetch...")
        for trader in traders:
            tid = trader['id']
            addr = trader['wallet_address']
            name = trader['name'] or addr[:10] + '...'

            if not trader['name']:
                with get_conn() as c:
                    update_trader_profile(tid, addr, c)

            # Register addresses for RPC on-chain monitoring
            rpc_watch_address(addr)
            proxy = resolve_proxy_wallet(addr)
            if proxy and proxy != addr.lower():
                rpc_watch_address(proxy)

            with get_conn() as c:
                new = fetch_trades_for_trader(tid, addr, c, limit=100)
            print(f"  {name}: {new} new trade(s)")
            time.sleep(0.5)
        print()
    else:
        print("  No traders tracked yet. Add traders via the web UI.\n")

    # Start polling
    poll_loop()


if __name__ == '__main__':
    run_fetcher()
