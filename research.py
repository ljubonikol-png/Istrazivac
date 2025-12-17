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

# Cloud-safe imports
try:
    import streamlit as st
except ImportError:
    st = None

try:
    import pandas as pd
except ImportError:
    pd = None

# Optional packages
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

# =========================
# CONFIG
# =========================

# Writeable folder for Streamlit Cloud
DATA_DIR = os.path.join(os.getcwd(), "data")
os.makedirs(DATA_DIR, exist_ok=True)

DB_PATH = os.path.join(DATA_DIR, "crawler.db")
USERS_DB = os.path.join(DATA_DIR, "users.db")
WARC_PATH = os.path.join(DATA_DIR, "archive.warc.gz")
PAYLOAD_DIR = os.path.join(DATA_DIR, "payloads")
OUTPUT_DIR = os.path.join(DATA_DIR, "output")

os.makedirs(PAYLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

SEED_URLS = ["https://www.gov.rs/"]
MAX_DEPTH = 2
THREADS = 4
TIMEOUT = 20
USER_AGENT = "GarbageMan/1.0 (archival crawler; TLS relaxed)"

# =========================
# LOGGING
# =========================

def log(msg):
    print(time.strftime("[%H:%M:%S]"), msg, flush=True)

# =========================
# DATABASES
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

def init_users_db():
    conn = sqlite3.connect(USERS_DB)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY,
            password TEXT,
            role TEXT,
            crawl_count INTEGER DEFAULT 0,
            created_at TEXT
        )
    """)
    # Default admin
    c.execute("SELECT 1 FROM users WHERE username='admin'")
    if not c.fetchone():
        c.execute(
            "INSERT INTO users VALUES (?,?,?,?,?)",
            ("admin", "MiraAndjaDiki.,.,1234567890,.,.", "admin", 0, time.ctime())
        )
    # Example free and premium users
    for u, p, r in [("freeuser","freepass","free"), ("premiumuser","1234","premium")]:
        c.execute("SELECT 1 FROM users WHERE username=?", (u,))
        if not c.fetchone():
            c.execute(
                "INSERT INTO users VALUES (?,?,?,?,?)",
                (u,p,r,0,time.ctime())
            )
    conn.commit()
    conn.close()

def db_connection(path):
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn

# =========================
# NETWORK
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
# WARC
# =========================

warc_lock = threading.Lock()
warc_file = open(WARC_PATH, "ab")
warc_writer = WARCWriter(warc_file, gzip=True)

def write_warc(url, payload: bytes):
    with warc_lock:
        record = warc_writer.create_warc_record(url, record_type="resource", payload=BytesIO(payload))
        warc_writer.write_record(record)

# =========================
# CRAWLER
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
    conn = db_connection(DB_PATH)
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
    try:
        conn = db_connection(DB_PATH)
        df = pd.read_sql_query("SELECT * FROM seen", conn)
        csv_path = os.path.join(OUTPUT_DIR, "seen.csv")
        json_path = os.path.join(OUTPUT_DIR, "seen.json")
        df.to_csv(csv_path, index=False)
        df.to_json(json_path, orient="records")
        log(f"CSV export: {csv_path}")
        log(f"JSON export: {json_path}")
        return csv_path, json_path
    except Exception as e:
        log(f"export error: {e}")
        return None, None
    finally:
        conn.close()

# =========================
# AUTH
# =========================

def authenticate(username, password):
    conn = db_connection(USERS_DB)
    c = conn.cursor()
    c.execute("SELECT role, crawl_count FROM users WHERE username=? AND password=?", (username, password))
    row = c.fetchone()
    conn.close()
    return row

def increment_crawl(username):
    conn = db_connection(USERS_DB)
    c = conn.cursor()
    c.execute("UPDATE users SET crawl_count = crawl_count+1 WHERE username=?", (username,))
    conn.commit()
    conn.close()

# =========================
# STREAMLIT INTERFACE
# =========================

def run_web_interface():
    if st is None:
        print("Streamlit not installed. Running offline mode only.")
        return

    if 'username' not in st.session_state:
        st.session_state.username = None
        st.session_state.role = None

    st.title("GarbageMan Web Crawler")

    if st.session_state.username is None:
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        if st.button("Login"):
            auth = authenticate(username, password)
            if auth:
                st.session_state.username = username
                st.session_state.role = auth[0]
                st.success(f"Logged in as {st.session_state.role}")
            else:
                st.error("Invalid credentials")
        return

    role = st.session_state.role
    username = st.session_state.username

    if role == "admin":
        st.subheader("Admin Panel")
        conn = db_connection(USERS_DB)
        df_users = pd.read_sql("SELECT * FROM users", conn)
        st.dataframe(df_users)

        st.markdown("---")
        st.subheader("Change user password")
        user_to_change = st.text_input("Username to change password")
        new_pass = st.text_input("New password", type="password")
        if st.button("Update Password"):
            if user_to_change and new_pass:
                c = conn.cursor()
                c.execute("UPDATE users SET password=? WHERE username=?", (new_pass, user_to_change))
                conn.commit()
                st.success(f"Password updated for {user_to_change}")
                df_users = pd.read_sql("SELECT * FROM users", conn)
                st.dataframe(df_users)
            else:
                st.warning("Enter username and new password")
        conn.close()

    seed = st.text_input("Seed URL", "https://www.gov.rs/")
    if st.button("Start Crawl"):
        if role == "free":
            conn = db_connection(USERS_DB)
            c = conn.cursor()
            c.execute("SELECT crawl_count FROM users WHERE username=?", (username,))
            count = c.fetchone()[0]
            conn.close()
            if count >= 2:
                st.warning("Free user limit reached")
                return
        # enqueue crawl
        task_queue.put((seed, 0))
        threads = []
        for _ in range(THREADS):
            t = threading.Thread(target=worker, daemon=True)
            t.start()
            threads.append(t)
        task_queue.join()
        increment_crawl(username)
        csv_file, json_file = export_csv_json()
        if csv_file:
            st.success("Crawl complete!")
            st.download_button("Download CSV", csv_file)
            st.download_button("Download JSON", json_file)

# =========================
# MAIN
# =========================

def main():
    log("Initializing databases")
    init_db()
    init_users_db()
    if st is not None:
        run_web_interface()
    else:
        log("Running offline crawl")
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
        log("Offline crawl complete")

if __name__ == "__main__":
    main()
