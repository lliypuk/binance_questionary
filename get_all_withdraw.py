import os
import requests
import time
import sqlite3
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

BINANCE_API_KEY = os.getenv("API_KEY")
BINANCE_API_SECRET = os.getenv("API_SECRET")
METABASE_API_KEY = os.getenv("METABASE_TOKEN")
METABASE_HOST = "https://metabase.banxe.com"
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")
DB_FILE = "sent_withdrawal_checks.db"
BINANCE_BASE_URL = "https://api.binance.com"


def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS checked_tx (tx_id TEXT PRIMARY KEY)''')
    conn.commit()
    conn.close()


def is_checked(tx_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT 1 FROM checked_tx WHERE tx_id = ?", (tx_id,))
    exists = c.fetchone() is not None
    conn.close()
    return exists


def mark_as_checked(tx_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO checked_tx (tx_id) VALUES (?)", (tx_id,))
    conn.commit()
    conn.close()


def send_telegram_message(text):
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    data = {"chat_id": TG_CHAT_ID, "text": text}
    try:
        response = requests.post(url, data=data)
        if not response.ok:
            print(f"[TG ERROR] {response.status_code}: {response.text}")
    except Exception as e:
        print(f"[TG EXCEPTION] {e}")


def get_timestamp_ms(dt):
    return int(dt.timestamp() * 1000)


def sign(query_string, secret):
    import hmac
    import hashlib
    return hmac.new(secret.encode(), query_string.encode(), hashlib.sha256).hexdigest()


def get_binance_withdrawals(start_time_ms, end_time_ms):
    endpoint = "/sapi/v1/capital/withdraw/history"
    params = {
        "startTime": start_time_ms,
        "endTime": end_time_ms,
        "timestamp": int(time.time() * 1000)
    }
    query_string = "&".join(f"{k}={v}" for k, v in params.items())
    signature = sign(query_string, BINANCE_API_SECRET)
    url = f"{BINANCE_BASE_URL}{endpoint}?{query_string}&signature={signature}"
    headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


def get_metabase_outgoing_transactions():
    url = f"{METABASE_HOST}/api/card/892/query/json"
    headers = {"X-API-KEY": METABASE_API_KEY}
    response = requests.post(url, headers=headers, timeout=10)
    response.raise_for_status()
    return response.json()


def is_withdrawal_matched(out_tx, binance_tx_list):
    to_address = out_tx.get("to", "").lower()
    amount = float(out_tx.get("amount", 0))
    token = out_tx.get("token", "").upper()
    blockchain = out_tx.get("blockchain", "").upper()

    print(f"\n🔍 Проверка транзакции:")
    print(f"- amount:     {amount}")
    print(f"- to:         {to_address}")
    print(f"- token:      {token}")
    print(f"- blockchain: {blockchain}")

    for btx in binance_tx_list:
        b_address = btx.get("address", "").lower()
        b_token = btx.get("coin", "").upper()
        b_chain = btx.get("network", "").upper()
        b_amount = float(btx.get("amount", 0))

        print(f"\n→ Сравниваем с Binance TX:")
        print(f"  b_amount:     {b_amount}")
        print(f"  b_address:    {b_address}")
        print(f"  b_token:      {b_token}")
        print(f"  b_blockchain: {b_chain}")

        if b_address != to_address:
            print("  ✗ address не совпадает")
            continue
        if b_token != token:
            print("  ✗ token не совпадает")
            continue
        if b_chain != blockchain:
            print("  ✗ blockchain не совпадает")
            continue
        if abs(b_amount - amount) / amount > 0.01:
            print(f"  ✗ amount не совпадает (разница {abs(b_amount - amount):.6f})")
            continue

        print("  ✅ MATCH FOUND")
        return True

    print("⛔️ Нет совпадений")
    return False



def check_and_notify_missing_withdrawals():
    init_db()
    now = datetime.utcnow()+ timedelta(days=1)
    yesterday = now - timedelta(days=1)

    metabase_txs = get_metabase_outgoing_transactions()
    binance_txs = get_binance_withdrawals(
        get_timestamp_ms(yesterday),
        get_timestamp_ms(now)
    )

    for tx in metabase_txs:
        tx_id = str(tx["id"])
        if is_checked(tx_id):
            continue
        if not is_withdrawal_matched(tx, binance_txs):
            msg = (
                f"📤️Вывод НЕ найден на Binance\n\n"
                f"ID: {tx_id}\n"
                f"sub_account_id: {tx.get('sub_account_id', 'unknown')}\n"
                f"binance email: {tx.get('binance_email', 'unknown')}\n\n"
                f"{tx['amount']} {tx['token']} → {tx['to']} via {tx['blockchain']}\n\n\n"
                f"email: {tx.get('email', 'unknown')}\n"
                f"name: {tx.get('full_name', 'unknown')}\n"
                f"type: {tx.get('type', 'unknown')}\n\n"
                f"created_at: {tx.get('created_at')}"
            )
            print(msg)
            send_telegram_message(msg)
        mark_as_checked(tx_id)


if __name__ == "__main__":
    check_and_notify_missing_withdrawals()
