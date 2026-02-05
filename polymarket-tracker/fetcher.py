"""
Polymarket Trade Fetcher - Runs every minute to fetch new trades
"""
import requests
import sqlite3
import time
from datetime import datetime
import schedule

DATABASE = 'polymarket_trades.db'

# Polymarket API endpoints
ACTIVITY_API = 'https://data-api.polymarket.com/activity'
PROFILE_API = 'https://gamma-api.polymarket.com/public-profile'


def get_db():
    """Get database connection"""
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn


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
        else:
            print(f"[{datetime.now()}] Failed to fetch profile for {wallet_address}: {response.status_code}")
            return None
    except Exception as e:
        print(f"[{datetime.now()}] Error fetching profile for {wallet_address}: {e}")
        return None


def update_trader_profile(trader_id, wallet_address, conn):
    """Update trader profile in database"""
    profile = fetch_trader_profile(wallet_address)
    
    if profile:
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE traders SET
                name = ?,
                pseudonym = ?,
                bio = ?,
                profile_image = ?,
                x_username = ?,
                verified_badge = ?,
                created_at = ?
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
        print(f"[{datetime.now()}] Updated profile for {wallet_address}")


def fetch_trades_for_trader(trader_id, wallet_address, conn):
    """Fetch trades for a specific trader"""
    try:
        # Fetch activity (trades) for this user
        response = requests.get(
            ACTIVITY_API,
            params={
                'user': wallet_address,
                'type': 'TRADE',
                'limit': 100
            },
            timeout=30
        )
        
        if response.status_code != 200:
            print(f"[{datetime.now()}] Failed to fetch trades for {wallet_address}: {response.status_code}")
            return 0
        
        trades = response.json()
        cursor = conn.cursor()
        new_trades = 0
        
        for trade in trades:
            tx_hash = trade.get('transactionHash')
            
            if not tx_hash:
                continue
            
            # Check if trade already exists
            cursor.execute('SELECT id FROM trades WHERE transaction_hash = ?', (tx_hash,))
            if cursor.fetchone():
                continue
            
            # Insert new trade
            cursor.execute('''
                INSERT INTO trades (
                    trader_id, transaction_hash, side, size, price, usdc_size,
                    timestamp, title, slug, icon, event_slug, outcome,
                    outcome_index, condition_id, asset
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                trader_id,
                tx_hash,
                trade.get('side'),
                trade.get('size'),
                trade.get('price'),
                trade.get('usdcSize'),
                trade.get('timestamp'),
                trade.get('title'),
                trade.get('slug'),
                trade.get('icon'),
                trade.get('eventSlug'),
                trade.get('outcome'),
                trade.get('outcomeIndex'),
                trade.get('conditionId'),
                trade.get('asset')
            ))
            new_trades += 1
        
        conn.commit()
        return new_trades
        
    except Exception as e:
        print(f"[{datetime.now()}] Error fetching trades for {wallet_address}: {e}")
        return 0


def fetch_all_trades():
    """Fetch trades for all tracked traders"""
    print(f"\n[{datetime.now()}] Starting trade fetch cycle...")
    
    conn = get_db()
    cursor = conn.cursor()
    
    # Get all traders
    cursor.execute('SELECT id, wallet_address, name FROM traders')
    traders = cursor.fetchall()
    
    if not traders:
        print(f"[{datetime.now()}] No traders to track. Add some traders first!")
        conn.close()
        return
    
    total_new_trades = 0
    
    for trader in traders:
        trader_id = trader['id']
        wallet_address = trader['wallet_address']
        trader_name = trader['name'] or trader['wallet_address'][:10] + '...'
        
        # Update profile if name is missing
        if not trader['name']:
            update_trader_profile(trader_id, wallet_address, conn)
        
        # Fetch trades
        new_trades = fetch_trades_for_trader(trader_id, wallet_address, conn)
        total_new_trades += new_trades
        
        if new_trades > 0:
            print(f"[{datetime.now()}] Found {new_trades} new trades for {trader_name}")
        
        # Small delay to avoid rate limiting
        time.sleep(0.5)
    
    conn.close()
    print(f"[{datetime.now()}] Fetch cycle complete. Total new trades: {total_new_trades}")


def run_scheduler():
    """Run the fetcher on a schedule"""
    print("=" * 60)
    print("Polymarket Trade Fetcher Started")
    print("=" * 60)
    print(f"[{datetime.now()}] Fetcher running. Will check for new trades every minute.")
    print("Press Ctrl+C to stop.\n")
    
    # Run immediately on start
    fetch_all_trades()
    
    # Schedule to run every minute
    schedule.every(1).minutes.do(fetch_all_trades)
    
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    run_scheduler()
