"""
Polymarket Copy Trader - Configuration Setup
Run this script once to configure your wallet and copy trading settings.
All settings are saved to config.json.

Usage:
    python setup_config.py
"""
import json
import os
import sys


def input_secret(prompt=""):
    """Read input showing * for each character typed."""
    print(prompt, end='', flush=True)
    chars = []
    try:
        if sys.platform == 'win32':
            import msvcrt
            while True:
                ch = msvcrt.getwch()
                if ch in ('\r', '\n'):
                    print()
                    break
                if ch == '\b' or ch == '\x7f':
                    if chars:
                        chars.pop()
                        sys.stdout.write('\b \b')
                        sys.stdout.flush()
                elif ch == '\x03':
                    raise KeyboardInterrupt
                else:
                    chars.append(ch)
                    sys.stdout.write('*')
                    sys.stdout.flush()
        else:
            import tty, termios
            fd = sys.stdin.fileno()
            old = termios.tcgetattr(fd)
            try:
                tty.setraw(fd)
                while True:
                    ch = sys.stdin.read(1)
                    if ch in ('\r', '\n'):
                        sys.stdout.write('\n')
                        sys.stdout.flush()
                        break
                    if ch == '\x7f' or ch == '\x08':
                        if chars:
                            chars.pop()
                            sys.stdout.write('\b \b')
                            sys.stdout.flush()
                    elif ch == '\x03':
                        raise KeyboardInterrupt
                    else:
                        chars.append(ch)
                        sys.stdout.write('*')
                        sys.stdout.flush()
            finally:
                termios.tcsetattr(fd, termios.TCSADRAIN, old)
    except (ImportError, OSError):
        # Fallback if terminal tricks unavailable
        import getpass
        return getpass.getpass(prompt)
    return ''.join(chars)

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
    print("=" * 60)
    print()

    config = load_config()

    # Show current config
    if config.get('private_key'):
        pk = config['private_key']
        masked_pk = pk[:4] + '*' * (len(pk) - 8) + pk[-4:] if len(pk) > 8 else '***'
        print(f"  Private key:     {masked_pk}")
        print(f"  Current wallet:  {config.get('funder_address', 'Not set')}")
        if config.get('funder_address'):
            bal = get_usdc_balance(config.get('funder_address', ''))
            print(f"  USDC.e Balance:  ${bal:,.2f}")
        print(f"  RPC mode:        Polygon RPC (polygon-rpc.com)")
        print(f"  Copy enabled:    {'Yes' if config.get('copy_trading_enabled') else 'No'}")
        print(f"  Copy percentage: {config.get('copy_percentage', 10)}%")
        print(f"  Max trade size:  ${config.get('max_trade_size', 100)}")
        print(f"  Min trade size:  ${config.get('min_trade_size', 10)}")
        mpe = config.get('max_trades_per_event', 0)
        print(f"  Max / event:     {mpe if mpe > 0 else 'unlimited'}")
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
    pk = input_secret("  Private Key (shows *): ").strip()
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

    cp_input = input(f"  Copy percentage (1-100) [{config.get('copy_percentage', 10)}]: ").strip()
    copy_pct = int(cp_input) if cp_input else config.get('copy_percentage', 10)
    copy_pct = max(1, min(100, copy_pct))

    mx_input = input(f"  Max trade size USD [{config.get('max_trade_size', 100)}]: ").strip()
    max_size = float(mx_input) if mx_input else config.get('max_trade_size', 100)

    mn_input = input(f"  Min trade size to copy USD [{config.get('min_trade_size', 10)}]: ").strip()
    min_size = float(mn_input) if mn_input else config.get('min_trade_size', 10)

    mpe_input = input(f"  Max trades per event (0 = unlimited) [{config.get('max_trades_per_event', 0)}]: ").strip()
    max_per_event = int(mpe_input) if mpe_input else config.get('max_trades_per_event', 0)
    max_per_event = max(0, max_per_event)

    enable_input = input("  Enable copy trading now? (y/N): ").strip().lower()
    enabled = enable_input == 'y'

    # Save
    config['private_key'] = pk
    config['funder_address'] = funder
    config['signature_type'] = sig_type
    config['copy_percentage'] = copy_pct
    config['max_trade_size'] = max_size
    config['min_trade_size'] = min_size
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
    print("  Next steps:")
    print("    1. python app.py       (start dashboard + copy engine)")
    print("    2. python fetcher.py   (fetch trades from target wallets)")
    print("    3. Open http://localhost:5000")
    print("=" * 60)


if __name__ == '__main__':
    setup()
