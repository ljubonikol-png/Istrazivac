import os
import time
import threading
import queue
import hashlib
import sqlite3
import requests
import warnings
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from warcio.warcwriter import WARCWriter
from io import BytesIO

# Try/except za pakete koji prave problem na Cloud-u
try:
    import pytesseract
except ImportError:
    pytesseract = None

try:
    from PySide6 import QtWidgets
except ImportError:
    QtWidgets = None

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    sync_playwright = None

# Streamlit import
try:
    import streamlit as st
except ImportError:
    st = None

# =========================
# CONFIG
# =========================

ROOT = r"E:\GarbageMan"
DB_PATH = os.path.join(ROOT, "crawler.db")
WARC_PATH = os.path.join(ROOT, "archive.warc.gz")
PAYLOAD_DIR = os.path.join(ROOT, "payloads")
OUTPUT_DIR = os.path.join(ROOT, "output")

SEED_URLS = [
    "https://www.gov.rs/",
]

MAX_DEPTH = 2
THREADS = 4
TIMEOUT = 20
USER_AGENT = "GarbageMan/1.0 (archival crawler; TLS relaxed)"

os.makedirs(PAYLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# =========================
# LOGGING
# =========================

def log(msg):
    print(time.strftime("[%H:%M:%S]"), msg, flush=True)

# =========================
# DATABASE
# =========================

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("PRAGMA journal_mode=WAL;")
    c.execute("""
        CREATE TABLE IF NOT EXISTS seen (
            url TEXT PRIMARY KEY,
            hash TEXT
        )
    """)
    conn.commit()
    conn.close()

def db_connection():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn

# =========================
# NETWORK (TLS-ROBUST)
# =========================

warnings.simplefilter("ignore")

session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT})

def fetch(url):
    try:
        r = session.get(url, timeout=TIMEOUT, verify=False, allow_redirects=True)
        return r.content, r.headers.get("Content-Type", "")
    except Exception as e:
        log(f"fetch error {url} : {e}")
        return None, None

# =========================
# HASHING
# =========================

def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

# =========================
# WARC (THREAD SAFE)
# =========================

warc_lock = threading.Lock()
warc_file = open(WARC_PATH, "ab")
warc_writer = WARCWriter(warc_file, gzip=True)

def write_warc(url, payload: bytes):
    with warc_lock:
        record = warc_writer.create_warc_record(url, record_type="resource", payload=BytesIO(payload))
        warc_writer.write_record(record)

# =========================
# CRAWLER CORE
# =========================

task_queue = queue.Queue()
seen_lock = threading.Lock()

def process_url(url, depth, conn):
    c = conn.cursor()
    c.execute("SELECT 1 FROM seen WHERE url=?", (url,))
    if c.fetchone():
        return

    log(f"fetching d={depth} {url}")

    data, ctype = fetch(url)
    if not data:
        return

    h = sha256(data)

    with seen_lock:
        c.execute("INSERT OR IGNORE INTO seen(url, hash) VALUES (?,?)", (url, h))
        conn.commit()

    with open(os.path.join(PAYLOAD_DIR, h), "wb") as f:
        f.write(data)

    write_warc(url, data)

    if depth >= MAX_DEPTH:
        return

    if "html" not in (ctype or "").lower():
        return

    try:
        soup = BeautifulSoup(data, "lxml")
        for a in soup.find_all("a", href=True):
            link = urljoin(url, a["href"])
            if link.startswith("http"):
                task_queue.put((link, depth + 1))
    except Exception as e:
        log(f"parse error {url} : {e}")

def worker():
    conn = db_connection()
    while True:
        try:
            url, depth = task_queue.get(timeout=3)
        except queue.Empty:
            break
        try:
            process_url(url, depth, conn)
        finally:
            task_queue.task_done()
    conn.close()

# =========================
# EXPORT
# =========================

def export_csv_json():
    conn = db_connection()
    df = None
    try:
        import pandas as pd
        df = pd.read_sql_query("SELECT * FROM seen", conn)
        csv_path = os.path.join(OUTPUT_DIR, "seen.csv")
        json_path = os.path.join(OUTPUT_DIR, "seen.json")
        df.to_csv(csv_path, index=False)
        df.to_json(json_path, orient="records")
        log(f"CSV export zavrsen: {csv_path}")
        log(f"JSON export zavrsen: {json_path}")
    except Exception as e:
        log(f"export error: {e}")
    finally:
        conn.close()

# =========================
# STREAMLIT INTERFACE
# =========================

def run_web_interface():
    if st is None:
        print("Streamlit not installed. Running offline mode only.")
        return

    if 'crawl_count' not in st.session_state:
        st.session_state.crawl_count = 0
    if 'premium_status' not in st.session_state:
        st.session_state.premium_status = False

    st.title("GarbageMan Web Crawler")

    username = st.text_input("Username")
    password = st.text_input("Password", type="password")

    premium_users = {"premiumuser": "1234"}
    free_users = {"freeuser": "freepass"}

    login = st.button("Login")

    if login:
        if username in premium_users and password == premium_users[username]:
            st.session_state.premium_status = True
            st.success("Premium login successful!")
        elif username in free_users and password == free_users[username]:
            st.session_state.premium_status = False
            st.success("Free login successful!")
        else:
            st.error("Invalid credentials")

    if st.session_state.premium_status is not None:
        if not st.session_state.premium_status and st.session_state.crawl_count >= 2:
            st.warning("Free user limit reached")
        else:
            seed = st.text_input("Seed URL", "https://www.gov.rs/")
            if st.button("Start Crawl"):
                st.session_state.crawl_count += 1
                task_queue.put((seed, 0))
                threads = []
                for _ in range(THREADS):
                    t = threading.Thread(target=worker, daemon=True)
                    t.start()
                    threads.append(t)
                task_queue.join()
                export_csv_json()
                st.success("Crawl complete!")

# =========================
# MAIN
# =========================

def main():
    log("initializing database")
    init_db()

    if st is not None:
        run_web_interface()
    else:
        log("running offline crawl")
        for u in SEED_URLS:
            task_queue.put((u, 0))
        threads = []
        for _ in range(THREADS):
            t = threading.Thread(target=worker, daemon=True)
            t.start()
            threads.append(t)
        task_queue.join()
        export_csv_json()
        warc_file.close()
        log("offline crawl complete")

if __name__ == "__main__":
    main()
