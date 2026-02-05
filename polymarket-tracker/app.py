"""
Polymarket Trader Tracker - Flask Web Application
"""
from flask import Flask, render_template, jsonify, request, Response
import sqlite3
from datetime import datetime
import os
import json
import time

app = Flask(__name__)
DATABASE = 'polymarket_trades.db'
CONFIG_FILE = 'config.json'


def load_config():
    """Load configuration from file"""
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r') as f:
            return json.load(f)
    return {
        'copy_trading_enabled': False,
        'copy_percentage': 10,
        'max_trade_size': 100,
        'min_trade_size': 10
    }


def save_config(config):
    """Save configuration to file"""
    with open(CONFIG_FILE, 'w') as f:
        json.dump(config, f, indent=2)


def get_db():
    """Get database connection"""
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Initialize the database with required tables"""
    conn = get_db()
    cursor = conn.cursor()
    
    # Create traders table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS traders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wallet_address TEXT UNIQUE NOT NULL,
            name TEXT,
            pseudonym TEXT,
            bio TEXT,
            profile_image TEXT,
            x_username TEXT,
            verified_badge INTEGER DEFAULT 0,
            created_at TEXT,
            added_at TEXT DEFAULT CURRENT_TIMESTAMP,
            copy_trading_enabled INTEGER DEFAULT 0,
            total_profit REAL DEFAULT 0,
            win_rate REAL DEFAULT 0,
            total_trades INTEGER DEFAULT 0,
            volume_traded REAL DEFAULT 0
        )
    ''')
    
    # Create trades table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trader_id INTEGER NOT NULL,
            transaction_hash TEXT UNIQUE,
            side TEXT,
            size REAL,
            price REAL,
            usdc_size REAL,
            timestamp INTEGER,
            title TEXT,
            slug TEXT,
            icon TEXT,
            event_slug TEXT,
            outcome TEXT,
            outcome_index INTEGER,
            condition_id TEXT,
            asset TEXT,
            fetched_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (trader_id) REFERENCES traders(id)
        )
    ''')
    
    conn.commit()
    conn.close()


@app.route('/')
def index():
    """Main dashboard page"""
    return render_template('index.html')


@app.route('/api/traders', methods=['GET'])
def get_traders():
    """Get all tracked traders"""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM traders ORDER BY added_at DESC')
    traders = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return jsonify(traders)


@app.route('/api/traders', methods=['POST'])
def add_trader():
    """Add a new trader to track"""
    data = request.json
    wallet_address = data.get('wallet_address', '').strip().lower()
    
    if not wallet_address:
        return jsonify({'error': 'Wallet address is required'}), 400
    
    if not wallet_address.startswith('0x') or len(wallet_address) != 42:
        return jsonify({'error': 'Invalid wallet address format'}), 400
    
    conn = get_db()
    cursor = conn.cursor()
    
    # Check if trader already exists
    cursor.execute('SELECT id FROM traders WHERE wallet_address = ?', (wallet_address,))
    existing = cursor.fetchone()
    
    if existing:
        conn.close()
        return jsonify({'error': 'Trader already being tracked'}), 400
    
    # Insert new trader (profile will be fetched by the fetcher script)
    cursor.execute('''
        INSERT INTO traders (wallet_address) VALUES (?)
    ''', (wallet_address,))
    
    conn.commit()
    trader_id = cursor.lastrowid
    conn.close()
    
    return jsonify({'success': True, 'trader_id': trader_id})


@app.route('/api/traders/<int:trader_id>', methods=['DELETE'])
def delete_trader(trader_id):
    """Remove a trader and their trades"""
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute('DELETE FROM trades WHERE trader_id = ?', (trader_id,))
    cursor.execute('DELETE FROM traders WHERE id = ?', (trader_id,))
    
    conn.commit()
    conn.close()
    
    return jsonify({'success': True})


@app.route('/api/trades', methods=['GET'])
def get_trades():
    """Get all trades, optionally filtered by trader"""
    trader_id = request.args.get('trader_id')
    limit = request.args.get('limit', 100, type=int)
    
    conn = get_db()
    cursor = conn.cursor()
    
    if trader_id:
        cursor.execute('''
            SELECT t.*, tr.wallet_address, tr.name, tr.pseudonym, tr.profile_image, tr.verified_badge
            FROM trades t
            JOIN traders tr ON t.trader_id = tr.id
            WHERE t.trader_id = ?
            ORDER BY t.timestamp DESC
            LIMIT ?
        ''', (trader_id, limit))
    else:
        cursor.execute('''
            SELECT t.*, tr.wallet_address, tr.name, tr.pseudonym, tr.profile_image, tr.verified_badge
            FROM trades t
            JOIN traders tr ON t.trader_id = tr.id
            ORDER BY t.timestamp DESC
            LIMIT ?
        ''', (limit,))
    
    trades = [dict(row) for row in cursor.fetchall()]
    conn.close()
    
    return jsonify(trades)


@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Get overall statistics"""
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
        'trader_count': trader_count,
        'trade_count': trade_count,
        'total_volume': round(total_volume, 2),
        'last_fetch': last_fetch
    })


@app.route('/api/traders/<int:trader_id>/copy', methods=['POST'])
def toggle_copy_trading(trader_id):
    """Toggle copy trading for a trader"""
    data = request.json
    enabled = data.get('enabled', False)
    
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute('UPDATE traders SET copy_trading_enabled = ? WHERE id = ?', (1 if enabled else 0, trader_id))
    conn.commit()
    conn.close()
    
    return jsonify({'success': True, 'copy_trading_enabled': enabled})


@app.route('/api/traders/<int:trader_id>/details', methods=['GET'])
def get_trader_details(trader_id):
    """Get detailed trader info including stats"""
    conn = get_db()
    cursor = conn.cursor()
    
    # Get trader info
    cursor.execute('SELECT * FROM traders WHERE id = ?', (trader_id,))
    trader = cursor.fetchone()
    
    if not trader:
        conn.close()
        return jsonify({'error': 'Trader not found'}), 404
    
    trader_dict = dict(trader)
    
    # Calculate stats from trades
    cursor.execute('''
        SELECT 
            COUNT(*) as total_trades,
            SUM(usdc_size) as volume_traded,
            AVG(CASE WHEN side = 'BUY' THEN price ELSE NULL END) as avg_buy_price,
            AVG(CASE WHEN side = 'SELL' THEN price ELSE NULL END) as avg_sell_price,
            COUNT(CASE WHEN side = 'BUY' THEN 1 END) as buy_count,
            COUNT(CASE WHEN side = 'SELL' THEN 1 END) as sell_count,
            MIN(timestamp) as first_trade,
            MAX(timestamp) as last_trade
        FROM trades WHERE trader_id = ?
    ''', (trader_id,))
    
    stats = cursor.fetchone()
    trader_dict['stats'] = {
        'total_trades': stats['total_trades'] or 0,
        'volume_traded': round(stats['volume_traded'] or 0, 2),
        'avg_buy_price': round((stats['avg_buy_price'] or 0) * 100, 1),
        'avg_sell_price': round((stats['avg_sell_price'] or 0) * 100, 1),
        'buy_count': stats['buy_count'] or 0,
        'sell_count': stats['sell_count'] or 0,
        'first_trade': stats['first_trade'],
        'last_trade': stats['last_trade']
    }
    
    # Get recent markets traded
    cursor.execute('''
        SELECT title, slug, icon, outcome, COUNT(*) as trade_count, SUM(usdc_size) as volume
        FROM trades WHERE trader_id = ?
        GROUP BY title, outcome
        ORDER BY trade_count DESC
        LIMIT 5
    ''', (trader_id,))
    
    trader_dict['recent_markets'] = [dict(row) for row in cursor.fetchall()]
    
    conn.close()
    return jsonify(trader_dict)


@app.route('/api/config', methods=['GET'])
def get_config():
    """Get copy trading configuration"""
    config = load_config()
    # Don't expose private key to frontend
    safe_config = {
        'copy_trading_enabled': config.get('copy_trading_enabled', False),
        'copy_percentage': config.get('copy_percentage', 10),
        'max_trade_size': config.get('max_trade_size', 100),
        'min_trade_size': config.get('min_trade_size', 10),
        'has_credentials': bool(config.get('private_key') and config.get('funder_address')),
        'funder_address': config.get('funder_address', '')[:10] + '...' if config.get('funder_address') else ''
    }
    return jsonify(safe_config)


@app.route('/api/config', methods=['POST'])
def update_config():
    """Update copy trading configuration"""
    data = request.json
    config = load_config()
    
    # Update settings
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
    """Update Polymarket API credentials"""
    data = request.json
    config = load_config()
    
    private_key = data.get('private_key', '').strip()
    funder_address = data.get('funder_address', '').strip().lower()
    signature_type = data.get('signature_type', 1)
    
    if not private_key or not funder_address:
        return jsonify({'error': 'Private key and funder address are required'}), 400
    
    if not funder_address.startswith('0x') or len(funder_address) != 42:
        return jsonify({'error': 'Invalid funder address format'}), 400
    
    config['private_key'] = private_key
    config['funder_address'] = funder_address
    config['signature_type'] = signature_type
    
    save_config(config)
    return jsonify({'success': True})


@app.route('/api/balance', methods=['GET'])
def get_balance():
    """Get wallet balance using Polymarket CLOB client"""
    config = load_config()
    
    if not config.get('private_key') or not config.get('funder_address'):
        return jsonify({'error': 'No credentials configured', 'balance': None})
    
    try:
        from py_clob_client.client import ClobClient
        
        sig_type = config.get('signature_type')
        if sig_type is None:
            sig_type = 1
        sig_type = int(sig_type)
        
        private_key = config['private_key']
        funder = config['funder_address']
        
        if not private_key or not funder:
            return jsonify({'error': 'Private key or funder address is empty', 'balance': None})
        
        client = ClobClient(
            "https://clob.polymarket.com",
            key=private_key,
            chain_id=137,
            signature_type=sig_type,
            funder=funder
        )
        
        # Derive API creds - this can fail if key/funder mismatch
        try:
            creds = client.create_or_derive_api_creds()
            if creds is None:
                return jsonify({'error': 'Failed to derive API credentials. Check your private key and funder address.', 'balance': None})
            client.set_api_creds(creds)
        except Exception as cred_err:
            return jsonify({'error': f'Credential error: {str(cred_err)}', 'balance': None})
        
        # Get balance allowances which includes USDC balance info
        try:
            balance_info = client.get_balance_allowance()
        except Exception as bal_err:
            return jsonify({'error': f'Balance fetch error: {str(bal_err)}', 'balance': None})
        
        return jsonify({
            'success': True,
            'balance': balance_info,
            'funder_address': funder
        })
    except ImportError:
        return jsonify({'error': 'py-clob-client not installed. Run: pip install py-clob-client', 'balance': None})
    except Exception as e:
        return jsonify({'error': str(e), 'balance': None})


@app.route('/api/copy-trades', methods=['GET'])
def get_copy_trades():
    """Get executed copy trades"""
    conn = get_db()
    cursor = conn.cursor()
    
    # Check if copy_trades table exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='copy_trades'")
    if not cursor.fetchone():
        conn.close()
        return jsonify([])
    
    cursor.execute('''
        SELECT ct.*, t.title, t.side, t.usdc_size as original_size
        FROM copy_trades ct
        LEFT JOIN trades t ON ct.original_trade_id = t.id
        ORDER BY ct.executed_at DESC
        LIMIT 50
    ''')
    
    trades = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return jsonify(trades)


@app.route('/api/positions/<wallet>')
def get_positions(wallet):
    """Proxy endpoint to fetch target wallet positions from Polymarket Data API (avoids CORS)"""
    import requests as req
    try:
        resp = req.get(
            'https://data-api.polymarket.com/positions',
            params={
                'user': wallet,
                'sizeThreshold': 0.1,
                'limit': 20,
                'sortBy': 'CURRENT',
                'sortDirection': 'DESC'
            },
            timeout=15
        )
        if resp.status_code == 200:
            return jsonify(resp.json())
        return jsonify([])
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/stream')
def stream():
    """Server-Sent Events endpoint for real-time UI updates.
    Checks the DB every 2 seconds for new trades and pushes them to the browser."""
    def event_stream():
        last_trade_id = 0
        # Get current max trade id
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute('SELECT MAX(id) as max_id FROM trades')
        row = cursor.fetchone()
        if row and row['max_id']:
            last_trade_id = row['max_id']
        conn.close()

        while True:
            time.sleep(2)
            try:
                conn = get_db()
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT t.*, tr.wallet_address, tr.name, tr.pseudonym,
                           tr.profile_image, tr.verified_badge
                    FROM trades t
                    JOIN traders tr ON t.trader_id = tr.id
                    WHERE t.id > ?
                    ORDER BY t.id ASC
                ''', (last_trade_id,))
                new_trades = [dict(row) for row in cursor.fetchall()]

                if new_trades:
                    last_trade_id = new_trades[-1]['id']
                    yield f"data: {json.dumps({'type': 'new_trades', 'trades': new_trades})}\n\n"

                # Also send stats
                cursor.execute('SELECT COUNT(*) as c FROM trades')
                trade_count = cursor.fetchone()['c']
                cursor.execute('SELECT COUNT(*) as c FROM traders')
                trader_count = cursor.fetchone()['c']
                yield f"data: {json.dumps({'type': 'heartbeat', 'trade_count': trade_count, 'trader_count': trader_count})}\n\n"

                conn.close()
            except Exception:
                yield f"data: {json.dumps({'type': 'heartbeat'})}\n\n"

    return Response(event_stream(), mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})


if __name__ == '__main__':
    init_db()
    app.run(debug=True, port=5000)
