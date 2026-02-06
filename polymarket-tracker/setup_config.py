"""
Polymarket Copy Trader - Configuration Setup
Run this script once to configure your wallet and copy trading settings.
All settings are saved to config.json.

Usage:
    python setup_config.py
"""
import json
import os

CONFIG_FILE = 'config.json'


def load_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r') as f:
            return json.load(f)
    return {}


def save_config(config):
    with open(CONFIG_FILE, 'w') as f:
        json.dump(config, f, indent=2)


def get_usdc_balance(funder_address):
    """Check USDC.e balance via Polygon RPC"""
    import requests
    USDC_E = '0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174'
    padded = funder_address.replace('0x', '').lower().zfill(64)
    call_data = '0x70a08231' + padded
    for rpc in ['https://polygon-rpc.com', 'https://rpc.ankr.com/polygon', 'https://polygon.llamarpc.com']:
        try:
            resp = requests.post(rpc, json={
                'jsonrpc': '2.0', 'method': 'eth_call',
                'params': [{'to': USDC_E, 'data': call_data}, 'latest'], 'id': 1
            }, timeout=10)
            result = resp.json()
            if 'result' in result and result['result'] != '0x':
                return int(result['result'], 16) / 1e6
        except Exception:
            continue
    return 0


def setup():
    print("=" * 60)
    print("  Polymarket Copy Trader - Setup")
    print("  (You can also configure via the web dashboard)")
    print("=" * 60)
    print()

    config = load_config()

    # Show current config
    if config.get('private_key'):
        print(f"  Current wallet:  {config.get('funder_address', 'Not set')}")
        bal = get_usdc_balance(config.get('funder_address', ''))
        print(f"  USDC.e Balance:  ${bal:,.2f}")
        print(f"  Copy enabled:    {'Yes' if config.get('copy_trading_enabled') else 'No'}")
        strategy = config.get('copy_strategy', 'PERCENTAGE')
        print(f"  Strategy:        {strategy}")
        if strategy == 'FIXED':
            print(f"  Fixed Amount:    ${config.get('fixed_trade_size', 10)}")
        else:
            print(f"  Copy %:          {config.get('copy_percentage', 10)}%")
        print(f"  Max Trade $:     ${config.get('max_trade_size', 100)}")
        mpe = config.get('max_trades_per_event', 0)
        print(f"  Max Trades/Event:{mpe if mpe > 0 else 'Unlimited'}")
        print()
        reconfigure = input("Reconfigure? (y/N): ").strip().lower()
        if reconfigure != 'y':
            print("No changes made.")
            return
        print()

    # Private key
    print("--- Wallet Configuration ---")
    print("Get your private key from: https://reveal.polymarket.com")
    print()
    pk = input("  Private Key: ").strip()
    if not pk:
        print("ERROR: Private key is required.")
        return

    # Funder address
    funder = input("  Funder/Proxy Address (0x...): ").strip().lower()
    if not funder.startswith('0x') or len(funder) != 42:
        print("ERROR: Invalid address. Must be 0x + 40 hex characters.")
        return

    # Signature type
    print()
    print("  Signature type:")
    print("    1 = Email / Magic Wallet (default)")
    print("    2 = Browser Wallet (MetaMask, etc.)")
    sig_input = input("  Select (1/2) [1]: ").strip()
    sig_type = int(sig_input) if sig_input in ('1', '2') else 1

    # Copy trading settings
    print()
    print("--- Copy Trading Settings ---")
    print()
    print("  Strategy:")
    print("    1 = Percentage  (copy a % of the trader's size)")
    print("    2 = Fixed       (use a fixed $ amount every trade)")
    print("    3 = Adaptive    (tiered sizing based on trade size)")
    cur_strat = config.get('copy_strategy', 'PERCENTAGE')
    strat_map = {'PERCENTAGE': '1', 'FIXED': '2', 'ADAPTIVE': '3'}
    strat_default = strat_map.get(cur_strat, '1')
    strat_input = input(f"  Select (1/2/3) [{strat_default}]: ").strip()
    strategy = {'1': 'PERCENTAGE', '2': 'FIXED', '3': 'ADAPTIVE'}.get(strat_input, cur_strat)

    copy_pct = config.get('copy_percentage', 10)
    fixed_size = config.get('fixed_trade_size', 10)

    if strategy == 'FIXED':
        fs_input = input(f"  Fixed trade amount USD [{fixed_size}]: ").strip()
        fixed_size = float(fs_input) if fs_input else fixed_size
        fixed_size = max(0.1, fixed_size)
    else:
        cp_input = input(f"  Copy percentage (1-100) [{copy_pct}]: ").strip()
        copy_pct = int(cp_input) if cp_input else copy_pct
        copy_pct = max(1, min(100, copy_pct))

    mx_input = input(f"  Max Trade $ (cap per trade) [{config.get('max_trade_size', 100)}]: ").strip()
    max_size = float(mx_input) if mx_input else config.get('max_trade_size', 100)
    max_size = max(1, max_size)

    mpe_cur = config.get('max_trades_per_event', 0)
    mpe_input = input(f"  Max Trades per Event (0 = unlimited) [{mpe_cur}]: ").strip()
    max_per_event = int(mpe_input) if mpe_input else mpe_cur
    max_per_event = max(0, max_per_event)

    enable_input = input("  Enable copy trading now? (y/N): ").strip().lower()
    enabled = enable_input == 'y'

    # Save
    config['private_key'] = pk
    config['funder_address'] = funder
    config['signature_type'] = sig_type
    config['copy_strategy'] = strategy
    config['copy_percentage'] = copy_pct
    config['fixed_trade_size'] = fixed_size
    config['max_trade_size'] = max_size
    config['max_trades_per_event'] = max_per_event
    config['copy_trading_enabled'] = enabled

    save_config(config)

    # Check balance
    print()
    print("Checking wallet balance...")
    bal = get_usdc_balance(funder)
    print(f"  USDC.e Balance: ${bal:,.2f}")

    if bal < 1:
        print("  WARNING: Low balance. Deposit USDC.e to your funder address.")

    # Test CLOB connection
    print()
    print("Testing CLOB API connection...")
    try:
        from py_clob_client.client import ClobClient
        client = ClobClient(
            "https://clob.polymarket.com",
            key=pk, chain_id=137,
            signature_type=sig_type, funder=funder
        )
        creds = client.create_or_derive_api_creds()
        if creds:
            client.set_api_creds(creds)
            print("  CLOB API: Connected successfully!")
        else:
            print("  CLOB API: WARNING - Could not derive credentials.")
            print("  Check that your private key and funder address match.")
    except ImportError:
        print("  CLOB API: py-clob-client not installed. Run: pip install py-clob-client")
    except Exception as e:
        print(f"  CLOB API: Error - {e}")

    print()
    print("=" * 60)
    print("  Configuration saved to config.json")
    print()
    print(f"  Strategy:     {strategy}")
    if strategy == 'FIXED':
        print(f"  Fixed Amount: ${fixed_size}")
    else:
        print(f"  Copy %:       {copy_pct}%")
    print(f"  Max Trade $:  ${max_size}")
    print(f"  Max Trades/Event: {max_per_event if max_per_event > 0 else 'Unlimited'}")
    print()
    print("  Next steps:")
    print("    1. python fetcher.py   (fetch trades from target wallets)")
    print("    2. python app.py       (start dashboard + copy engine)")
    print("    3. Open http://localhost:5000")
    print("=" * 60)


if __name__ == '__main__':
    setup()
