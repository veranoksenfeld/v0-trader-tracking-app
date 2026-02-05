"""
Polymarket Copy Trader - Flask Web Application
Integrated copy trading engine runs as a background thread.
"""
from flask import Flask, render_template, jsonify, request, Response
import sqlite3
from datetime import datetime
import os
import json
import time
import threading
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
    'errors': [],
}

# Try to import py-clob-client
try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import MarketOrderArgs, OrderType
    from py_clob_client.order_builder.constants import BUY, SELL
    CLOB_AVAILABLE = True
except ImportError:
    CLOB_AVAILABLE = False
    print("WARNING: py-clob-client not installed. Copy trading disabled. Run: pip install py-clob-client")


# ---- Helpers ----
def load_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r') as f:
            return json.load(f)
    return {'copy_trading_enabled': False, 'copy_percentage': 10, 'max_trade_size': 100, 'min_trade_size': 10}


def save_config(config):
    with open(CONFIG_FILE, 'w') as f:
        json.dump(config, f, indent=2)


def get_db():
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn


def log_event(level, message, details=''):
    """Write a log entry to the copy_log table"""
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(
            'INSERT INTO copy_log (timestamp, level, message, details) VALUES (?, ?, ?, ?)',
            (datetime.now().isoformat(), level, message, details)
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
            side TEXT,
            original_size REAL,
            our_size REAL,
            price REAL,
            status TEXT,
            executed_at TEXT,
            response TEXT,
            FOREIGN KEY (original_trade_id) REFERENCES trades(id)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS copy_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            level TEXT NOT NULL,
            message TEXT NOT NULL,
            details TEXT DEFAULT ''
        )
    ''')

    # Migrate traders table if needed
    cursor.execute("PRAGMA table_info(traders)")
    columns = [col[1] for col in cursor.fetchall()]
    for col, typ, default in [
        ('copy_trading_enabled', 'INTEGER', '0'),
        ('total_profit', 'REAL', '0'), ('win_rate', 'REAL', '0'),
        ('total_trades', 'INTEGER', '0'), ('volume_traded', 'REAL', '0')
    ]:
        if col not in columns:
            cursor.execute(f'ALTER TABLE traders ADD COLUMN {col} {typ} DEFAULT {default}')

    conn.commit()
    conn.close()


# ============================================
#  COPY TRADING ENGINE (background thread)
# ============================================

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
        return None
    try:
        client = ClobClient(HOST_CLOB, key=private_key, chain_id=137, signature_type=sig_type, funder=funder_address)
        creds = client.create_or_derive_api_creds()
        if creds is None:
            log_event('ERROR', 'CLOB auth failed', 'create_or_derive_api_creds returned None. Check private key + funder address.')
            return None
        client.set_api_creds(creds)
        return client
    except Exception as e:
        log_event('ERROR', 'CLOB init failed', str(e))
        return None


HOST_CLOB = "https://clob.polymarket.com"


def execute_single_copy(client, trade, config):
    """Execute a single copy trade"""
    try:
        copy_pct = config.get('copy_percentage', 10)
        max_size = config.get('max_trade_size', 100)
        original_size = float(trade['usdc_size'] or 0)
        our_size = min(original_size * (copy_pct / 100), max_size)
        if our_size < 1:
            return False, 'Size too small', 0

        side = BUY if (trade['side'] or '').upper() == 'BUY' else SELL
        token_id = trade.get('asset')
        if not token_id:
            return False, 'No token ID', 0

        market_order = MarketOrderArgs(
            token_id=token_id, amount=our_size, side=side, order_type=OrderType.FOK
        )
        signed_order = client.create_market_order(market_order)
        resp = client.post_order(signed_order, OrderType.FOK)
        return True, str(resp), our_size
    except Exception as e:
        return False, str(e), 0


def copy_trading_loop():
    """Main copy trading loop -- runs in a background thread"""
    global copy_engine
    log_event('INFO', 'Copy trading engine started')
    copy_engine['running'] = True

    while copy_engine['running']:
        try:
            config = load_config()
            if not config.get('copy_trading_enabled'):
                time.sleep(5)
                continue

            if not config.get('private_key') or not config.get('funder_address'):
                time.sleep(10)
                continue

            last_processed = config.get('last_processed_timestamp', 0)

            conn = get_db()
            cursor = conn.cursor()
            cursor.execute('''
                SELECT t.*, tr.wallet_address, tr.name, tr.pseudonym
                FROM trades t
                JOIN traders tr ON t.trader_id = tr.id
                WHERE tr.copy_trading_enabled = 1 AND t.timestamp > ?
                ORDER BY t.timestamp ASC
            ''', (last_processed,))
            new_trades = [dict(row) for row in cursor.fetchall()]
            conn.close()

            if new_trades:
                copy_engine['last_check'] = datetime.now().isoformat()

                # Check balance
                balance = get_usdc_balance(config.get('funder_address', ''))
                if balance < 1:
                    log_event('WARN', f'Low balance: ${balance:.2f}', 'Need at least $1 USDC.e')
                    time.sleep(30)
                    continue

                # Init CLOB client
                client = get_clob_client(config)
                if not client:
                    log_event('ERROR', 'Could not connect to CLOB API', 'Check credentials')
                    time.sleep(30)
                    continue

                for trade in new_trades:
                    min_size = config.get('min_trade_size', 10)
                    if float(trade.get('usdc_size', 0)) < min_size:
                        log_event('SKIP', f'Trade too small: ${trade.get("usdc_size", 0):.2f}', f'Min: ${min_size}')
                        config['last_processed_timestamp'] = trade['timestamp']
                        save_config(config)
                        continue

                    trader_name = trade.get('name') or trade.get('pseudonym') or trade.get('wallet_address', '')[:12]
                    market_title = trade.get('title', 'Unknown')
                    side_str = (trade.get('side') or '').upper()

                    log_event('COPY', f'Copying {side_str} from {trader_name}', f'{market_title} | Original: ${trade.get("usdc_size", 0):.2f}')

                    success, response, our_size = execute_single_copy(client, trade, config)

                    # Record in copy_trades table
                    conn = get_db()
                    cursor = conn.cursor()
                    cursor.execute('''
                        INSERT INTO copy_trades (original_trade_id, trader_name, market_title, side, original_size, our_size, price, status, executed_at, response)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        trade['id'], trader_name, market_title, side_str,
                        trade.get('usdc_size', 0), our_size, trade.get('price', 0),
                        'SUCCESS' if success else 'FAILED',
                        datetime.now().isoformat(), response
                    ))
                    conn.commit()
                    conn.close()

                    if success:
                        copy_engine['trades_copied'] += 1
                        log_event('SUCCESS', f'Copied ${our_size:.2f} {side_str} on {market_title[:50]}', response[:200])
                    else:
                        copy_engine['trades_failed'] += 1
                        log_event('FAILED', f'Failed to copy {side_str} on {market_title[:50]}', response[:200])

                    config['last_processed_timestamp'] = trade['timestamp']
                    save_config(config)
                    time.sleep(1)
            else:
                copy_engine['last_check'] = datetime.now().isoformat()

        except Exception as e:
            log_event('ERROR', 'Engine error', str(e))

        time.sleep(5)

    log_event('INFO', 'Copy trading engine stopped')


def start_copy_engine():
    """Start the background copy trading thread"""
    global copy_engine
    if copy_engine['thread'] and copy_engine['thread'].is_alive():
        return
    copy_engine['running'] = True
    copy_engine['thread'] = threading.Thread(target=copy_trading_loop, daemon=True)
    copy_engine['thread'].start()


def stop_copy_engine():
    """Stop the background copy trading thread"""
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
    log_event('INFO', f'Started tracking wallet', wallet)
    return jsonify({'success': True, 'trader_id': tid})


@app.route('/api/traders/<int:trader_id>', methods=['DELETE'])
def delete_trader(trader_id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('DELETE FROM trades WHERE trader_id = ?', (trader_id,))
    cursor.execute('DELETE FROM traders WHERE id = ?', (trader_id,))
    conn.commit()
    conn.close()
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
    log_event('INFO', f'Copy trading {"enabled" if enabled else "disabled"} for trader #{trader_id}')
    return jsonify({'success': True, 'copy_trading_enabled': enabled})


@app.route('/api/traders/<int:trader_id>/details', methods=['GET'])
def get_trader_details(trader_id):
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
        'first_trade': s['first_trade'], 'last_trade': s['last_trade']
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
    
    # Get balance if we have credentials
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
        'last_check': copy_engine['last_check'],
        'clob_available': CLOB_AVAILABLE,
        'signature_type': config.get('signature_type', 1),
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
    log_event('INFO', 'Credentials updated', f'Funder: {fa[:10]}...')
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
        cursor.execute('''
            SELECT * FROM copy_trades ORDER BY executed_at DESC LIMIT 50
        ''')
        trades = [dict(row) for row in cursor.fetchall()]
    except Exception:
        trades = []
    conn.close()
    return jsonify(trades)


@app.route('/api/copy-log', methods=['GET'])
def get_copy_log():
    """Get copy trading event log"""
    limit = request.args.get('limit', 100, type=int)
    since_id = request.args.get('since_id', 0, type=int)
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT * FROM copy_log WHERE id > ? ORDER BY id DESC LIMIT ?', (since_id, limit))
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
            cursor.execute('SELECT MAX(id) as m FROM copy_log')
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

                # New log entries
                try:
                    cursor.execute('SELECT * FROM copy_log WHERE id > ? ORDER BY id ASC', (last_log_id,))
                    new_logs = [dict(row) for row in cursor.fetchall()]
                    if new_logs:
                        last_log_id = new_logs[-1]['id']
                        yield f"data: {json.dumps({'type': 'copy_log', 'logs': new_logs})}\n\n"
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

                # Heartbeat with stats
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
    # Auto-start the copy trading engine
    start_copy_engine()
    print("=" * 60)
    print("  Polymarket Copy Trader")
    print("  Dashboard: http://localhost:5000")
    print("  Copy engine running in background")
    print("=" * 60)
    app.run(debug=False, port=5000, threaded=True)
