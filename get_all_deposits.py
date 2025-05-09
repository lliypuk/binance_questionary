from dotenv import load_dotenv
import os
import time
import hmac
import hashlib
import requests
import sqlite3
from urllib.parse import urlencode
from datetime import datetime

load_dotenv()

API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
BASE_URL = 'https://api.binance.com'
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")
DB_FILE = "sent_deposits.db"

METABASE_HOST = "https://metabase.banxe.com"
METABASE_TOKEN = os.getenv("METABASE_TOKEN")


def get_timestamp():
    return int(time.time() * 1000)


def sign(params: dict, secret: str):
    query_string = urlencode(params)
    return hmac.new(secret.encode(), query_string.encode(), hashlib.sha256).hexdigest()


def make_signed_request(method: str, endpoint: str, params: dict, retries=5, delay=3):
    params['timestamp'] = get_timestamp()
    query = urlencode(params)
    signature = sign(params, API_SECRET)
    query += f"&signature={signature}"
    headers = {'X-MBX-APIKEY': API_KEY}
    url = f"{BASE_URL}{endpoint}?{query}"

    for attempt in range(retries):
        try:
            response = requests.request(method, url, headers=headers, timeout=10)
            if response.status_code != 200:
                print(f"[WARN] Статус {response.status_code}: {response.text}")
                return []
            return response.json()
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            print(f"[RETRY] Ошибка соединения: {e} — попытка {attempt + 1}/{retries}")
            time.sleep(delay)
        except Exception as e:
            print(f"[ERROR] Неожиданная ошибка: {e}")
            return []
    print("[FAIL] Превышено число попыток запроса.")
    return []


def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS sent_deposits (depositId TEXT PRIMARY KEY)''')
    conn.commit()
    conn.close()


def is_sent(deposit_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT 1 FROM sent_deposits WHERE depositId = ?", (deposit_id,))
    exists = c.fetchone() is not None
    conn.close()
    return exists


def mark_as_sent(deposit_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO sent_deposits (depositId) VALUES (?)", (deposit_id,))
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


def notify_pending_deposits():
    init_db()
    start_time = int(datetime(2025, 4, 30).timestamp() * 1000)
    end_time = int(time.time() * 1000)
    endpoint = '/sapi/v1/broker/subAccount/depositHist'

    offset = 0
    limit = 500

    while start_time < end_time:
        params = {
            'startTime': start_time,
            'endTime': min(start_time + 7 * 24 * 60 * 60 * 1000, end_time),
            'limit': limit,
            'offset': 0
        }

        while True:
            params['offset'] = offset
            batch = make_signed_request('GET', endpoint, params)
            for dep in batch:
                if dep.get('travelRuleStatus') == 1:
                    deposit_id = str(dep['depositId'])
                    if is_sent(deposit_id):
                        continue
                    sub_id = dep['subAccountId']
                    client = get_client_info_from_metabase(sub_id)
                    msg = (
                        f"❗️Депозит НЕ зачислен\n"
                        f"subAccountId: {sub_id}\n"
                        f"binance email: {client['binance_email']}\n\n"
                        
                        f"{dep['amount']} {dep['coin']}\n\n"
                        f"email: {client['email']}\n\n"
                        f"name: {client['full_name']}\n"
                        f"type: {client['type']}\n"
                        f"txId: {dep['txId']}\n\n"
                        f"insertTime: {datetime.fromtimestamp(dep['insertTime'] / 1000)}\n"
                        f"depositId: {deposit_id}"
                    )
                    print(msg)
                    send_telegram_message(msg)
                    mark_as_sent(deposit_id)
            if len(batch) < limit:
                break
            offset += limit

        start_time += 7 * 24 * 60 * 60 * 1000
        offset = 0


def get_client_info_from_metabase(sub_account_id):
    url = f"{METABASE_HOST}/api/card/891/query/json"
    headers = {
        "X-API-KEY": METABASE_TOKEN,
        "Content-Type": "application/x-www-form-urlencoded"
    }

    # Параметры передаются в виде URL-кодированной строки
    payload = f"parameters=%5B%7B%22type%22%3A%22category%22%2C%22target%22%3A%5B%22variable%22%2C%5B%22template-tag%22%2C%22sub_account_id%22%5D%5D%2C%22value%22%3A%22{sub_account_id}%22%7D%5D"

    try:
        response = requests.post(url, headers=headers, data=payload, timeout=10)
        response.raise_for_status()
        data = response.json()
        if not data or not isinstance(data, list):
            return {'email': 'unknown', 'full_name': 'unknown', 'type': 'unknown'}
        row = data[0]
        return {
            'email': row.get('email', 'unknown'),
            'full_name': row.get('full_name', 'unknown'),
            'type': row.get('type', 'unknown'),
            'binance_email': row.get('binance_email', 'unknown')
        }
    except requests.exceptions.RequestException as e:
        print(f"[Metabase API ERROR] sub_account_id {sub_account_id}: {e}")
        return {'email': 'unknown', 'full_name': 'unknown', 'type': 'unknown'}



if __name__ == '__main__':
    notify_pending_deposits()

