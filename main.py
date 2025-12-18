# ------------------------ IMPORTS ------------------------
import os
import json
import gspread
import requests
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO
import time
from datetime import datetime
from google.oauth2.service_account import Credentials

# ------------------------ LOGGING FUNCTION -------------------------
def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

# ------------------------ WRITE SERVICE ACCOUNT FILE ------------------------
with open("service_account.json", "w") as f:
    f.write(os.getenv("SERVICE_ACCOUNT_JSON"))
log("✅ Google credentials file written.")

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
gc = gspread.authorize(creds)
log("✅ Google Sheets authorization complete.")

# ------------------------ LOGIN FUNCTION ------------------------
def login_to_screener(session, username, password):
    try:
        login_url = 'https://www.screener.in/login/'
        session.headers.update({
            'User-Agent': 'Mozilla/5.0',
            'Referer': login_url,
            'Origin': 'https://www.screener.in',
        })
        res = session.get(login_url)
        soup = BeautifulSoup(res.text, 'html.parser')
        csrf_token = soup.find('input', {'name': 'csrfmiddlewaretoken'})['value']

        payload = {
            'csrfmiddlewaretoken': csrf_token,
            'username': username,
            'password': password,
            'next': '/'
        }
        headers = session.headers.copy()
        headers['Referer'] = login_url
        res2 = session.post(login_url, data=payload, headers=headers)

        return 'Core Watchlist' in res2.text
    except Exception as e:
        log(f"❌ Login error: {e}")
        return False

# ------------------------ RETRY FUNCTION ------------------------
def fetch_data_with_retry(session, url, retries=10, delay=1):
    for attempt in range(retries):
        try:
            response = session.get(url, headers=session.headers)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            if attempt < retries - 1:
                log(f"[Retry {attempt + 1}] {e}. Waiting {delay} seconds...")
                time.sleep(delay)
            else:
                log(f"❌ Failed after {retries} retries: {e}")
                return None

# ------------------------ ACCOUNTS ------------------------
accounts = [
    {"username": "amarbhavsarb@gmail.com",     "password": "abcd@0000", "url": "https://www.screener.in/screens/1790669/ttyy/?page={}", "range": "A1:T6000",  "add_classification": True},
    {"username": "amarbhavsarb+2@gmail.com",   "password": "abcd@0000", "url": "https://www.screener.in/screens/1790603/ttyy/?page={}", "range": "Z1:AQ6000", "add_classification": False},
    {"username": "amarbhavsarb+3@gmail.com",   "password": "abcd@0000", "url": "https://www.screener.in/screens/1790798/ttyy/?page={}", "range": "AY1:BP6000", "add_classification": False},
    {"username": "amarbhavsarb+4@gmail.com",   "password": "abcd@0000", "url": "https://www.screener.in/screens/2113854/ttyy/?page={}", "range": "BX1:CO6000", "add_classification": False},
    {"username": "amarbhavsarb+5@gmail.com",   "password": "abcd@0000", "url": "https://www.screener.in/screens/2358928/ttyy/?page={}", "range": "CW1:DN6000", "add_classification": False},
]

sheet_url = 'https://docs.google.com/spreadsheets/d/1aWHNmBkkTDnWLhMJwbErc6gs6Vtl78LW0kM4-I6Kj4o/edit?gid=0#gid=0'
sheet = gc.open_by_url(sheet_url).worksheet('Sheet2')
log("✅ Connected to Google Sheet.")

# ------------------------ MAIN SCRAPER ------------------------
for idx, acc in enumerate(accounts):
    log(f"\n🚀 Scraping Account {idx+1}: {acc['username']}")
    session = requests.Session()
    if login_to_screener(session, acc['username'], acc['password']):
        log("✅ Login successful")
        all_data = []
        page = 1

        while True:
            page_url = acc['url'].format(page)
            response = fetch_data_with_retry(session, page_url)
            if not response:
                break

            try:
                df = pd.read_html(StringIO(response.text), header=0)[0].fillna('')
            except:
                log(f"⚠️ Failed to parse table on page {page}")
                break

            if acc['add_classification']:
                df['Classification'] = None
                df['Hyperlink'] = None
                soup = BeautifulSoup(response.content, 'html.parser')
                rows = soup.find('table', class_='data-table').find('tbody').find_all('tr')
                for i, row in enumerate(rows):
                    cols = row.find_all('td')
                    if len(cols) > 1:
                        try:
                            value = float(cols[5].text.replace(',', ''))
                            classification = None
                            if 0.01 <= value <= 99.99:
                                classification = 1
                            elif 100 <= value <= 999.99:
                                classification = 2
                            elif 1000 <= value <= 99999.99:
                                classification = 3
                            elif value >= 100000:
                                classification = 4
                            if i > 0:
                                df.iloc[i - 1, -2] = classification
                        except:
                            if i > 0:
                                df.iloc[i - 1, -2] = None

                        name_column = cols[1]
                        name_link = name_column.find('a')['href'] if name_column.find('a') else ''
                        hyperlink_formula = f'=HYPERLINK("https://www.screener.in{name_link}", "{name_column.text.strip()}")'
                        if i > 0:
                            df.iloc[i - 1, -1] = hyperlink_formula

                df = df[[*df.columns[:-2], 'Classification', 'Hyperlink']]

                if 'Down  %' in df.columns:
                    df['Down  %'] = df['Down  %'].apply(
                        lambda x: f'-{float(x)}' if str(x).replace('.', '', 1).isdigit() else x
                    )
            else:
                df = df.iloc[:, :18]

            blank_row = [""] * len(df.columns)
            all_data += [df.columns.tolist()] + df.values.tolist() + [blank_row]
            log(f"✅ Page {page} scraped.")
            if 'Next' not in response.text:
                break
            page += 1
            time.sleep(0.9)

        try:
            sheet.batch_clear([acc['range']])
            sheet.update(values=all_data, range_name=acc['range'], value_input_option='USER_ENTERED')
            log(f"✅ Data written to Google Sheet range: {acc['range']}")
        except Exception as e:
            log(f"❌ Sheet update failed: {e}")
    else:
        log("❌ Login failed")

# ------------------------ TRIGGER GOOGLE APPS SCRIPT ------------------------
log("\n🔔 Triggering Google Apps Script...")
time.sleep(10)
apps_script_url = 'https://script.google.com/macros/s/AKfycbw6i46e87MVm0deMIjV9dtcgAxPLZhOy_1rkpdgol1j5_6TZQVgmeYW8p9zbB10jnQS1w/exec'
try:
    final_response = requests.get(apps_script_url)
    if final_response.status_code == 200:
        log("✅ Google Apps Script function triggered successfully.")
    else:
        log(f"❌ Script trigger failed: {final_response.status_code}")
except Exception as e:
    log(f"❌ Error calling Google Apps Script: {e}")
