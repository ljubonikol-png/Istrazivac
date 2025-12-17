import os
import time
import threading
import queue
import hashlib
import sqlite3
import requests
import warnings
import csv
import json
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from warcio.warcwriter import WARCWriter
from io import BytesIO

# Cloud-safe imports
try:
    import streamlit as st
except Exception:
    st = None

try:
    import pandas as pd
except Exception:
    pd = None

# =========================
# CONFIG (writeable DATA dir)
# =========================

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
# DATABASE HELPERS
# =========================

def db_connection(path):
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn

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
    c.execute("PRAGMA journal_mode=WAL;")
    c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY,
            password TEXT,
            role TEXT,
            crawl_count INTEGER DEFAULT 0,
            created_at TEXT
        )
    """)
    # Insert admin only if not exists
    c.execute("SELECT 1 FROM users WHERE username='admin'")
    if not c.fetchone():
        c.execute(
            "INSERT INTO users (username, password, role, crawl_count, created_at) VALUES (?,?,?,?,?)",
            ("admin", "MiraAndjaDiki.,.,1234567890,.,.", "admin", 0, time.ctime())
        )
    # Example free/premium users (only if not exist)
    for u, p, r in [("freeuser","freepass","free"), ("premiumuser","1234","premium")]:
        c.execute("SELECT 1 FROM users WHERE username=?", (u,))
        if not c.fetchone():
            c.execute(
                "INSERT INTO users (username, password, role, crawl_count, created_at) VALUES (?,?,?,?,?)",
                (u, p, r, 0, time.ctime())
            )
    conn.commit()
    conn.close()

# =========================
# NETWORK (TLS relaxed)
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
# HASH
# =========================

def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

# =========================
# WARC (thread-safe; open/close per write to avoid locks on Cloud)
# =========================

warc_lock = threading.Lock()

def write_warc(url, payload: bytes):
    with warc_lock:
        # Open, write record, close — safe for Cloud multi-process
        with open(WARC_PATH, "ab") as warc_file:
            warc_writer = WARCWriter(warc_file, gzip=True)
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
    # save payload
    try:
        with open(os.path.join(PAYLOAD_DIR, h), "wb") as f:
            f.write(data)
    except Exception as e:
        log(f"payload write error {e}")
    # write warc
    try:
        write_warc(url, data)
    except Exception as e:
        log(f"warc write error {e}")
    # follow links if html
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
# EXPORT CSV/JSON
# =========================

def export_csv_json():
    csv_path = os.path.join(OUTPUT_DIR, "seen.csv")
    json_path = os.path.join(OUTPUT_DIR, "seen.json")
    try:
        if pd is not None:
            conn = db_connection(DB_PATH)
            df = pd.read_sql_query("SELECT url, hash FROM seen", conn)
            df.to_csv(csv_path, index=False)
            df.to_json(json_path, orient="records")
            conn.close()
        else:
            conn = db_connection(DB_PATH)
            c = conn.cursor()
            c.execute("SELECT url, hash FROM seen")
            rows = c.fetchall()
            conn.close()
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["url","hash"])
                writer.writerows(rows)
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump([{"url":u,"hash":h} for u,h in rows], f, indent=2)
        log(f"CSV export: {csv_path}")
        log(f"JSON export: {json_path}")
        return csv_path, json_path
    except Exception as e:
        log(f"export error: {e}")
        return None, None

# =========================
# AUTH helpers
# =========================

def authenticate(username, password):
    conn = db_connection(USERS_DB)
    c = conn.cursor()
    c.execute("SELECT role, crawl_count FROM users WHERE username=? AND password=?", (username, password))
    row = c.fetchone()
    conn.close()
    return row  # (role, crawl_count) or None

def increment_crawl(username):
    conn = db_connection(USERS_DB)
    c = conn.cursor()
    c.execute("UPDATE users SET crawl_count = crawl_count + 1 WHERE username=?", (username,))
    conn.commit()
    conn.close()

# =========================
# ADMIN helpers
# =========================

def list_users_df():
    try:
        conn = db_connection(USERS_DB)
        df = None
        if pd is not None:
            df = pd.read_sql("SELECT username, role, crawl_count, created_at FROM users", conn)
        else:
            c = conn.cursor()
            c.execute("SELECT username, role, crawl_count, created_at FROM users")
            rows = c.fetchall()
            import pandas as _pd
            df = _pd.DataFrame(rows, columns=["username","role","crawl_count","created_at"])
        conn.close()
        return df
    except Exception as e:
        log(f"list_users error: {e}")
        return None

def update_password(username, new_password):
    conn = db_connection(USERS_DB)
    c = conn.cursor()
    c.execute("UPDATE users SET password=? WHERE username=?", (new_password, username))
    conn.commit()
    conn.close()

def set_premium(username):
    conn = db_connection(USERS_DB)
    c = conn.cursor()
    c.execute("UPDATE users SET role='premium' WHERE username=?", (username,))
    conn.commit()
    conn.close()

def reset_crawl_count(username):
    conn = db_connection(USERS_DB)
    c = conn.cursor()
    c.execute("UPDATE users SET crawl_count=0 WHERE username=?", (username,))
    conn.commit()
    conn.close()

# =========================
# STREAMLIT UI
# =========================

def safe_download_button(path, label):
    try:
        if os.path.exists(path):
            with open(path, "rb") as f:
                data = f.read()
            st.download_button(label, data=data, file_name=os.path.basename(path))
        else:
            st.warning(f"File not found: {os.path.basename(path)}")
    except Exception as e:
        st.error(f"Download error: {e}")

def run_web_interface():
    if st is None:
        print("Streamlit not available. Running offline mode only.")
        return

    if 'username' not in st.session_state:
        st.session_state.username = None
        st.session_state.role = None

    st.title("GarbageMan Web Crawler")

    # LOGIN SCREEN
    if st.session_state.username is None:
        with st.form("login_form"):
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            submitted = st.form_submit_button("Login")
        if submitted:
            auth = authenticate(username, password)
            if auth:
                st.session_state.username = username
                st.session_state.role = auth[0]
                st.success(f"Logged in as {st.session_state.username} ({st.session_state.role})")
                # show quick info
                conn = db_connection(USERS_DB)
                c = conn.cursor()
                c.execute("SELECT crawl_count FROM users WHERE username=?", (st.session_state.username,))
                r = c.fetchone()
                conn.close()
                if r:
                    st.info(f"Your crawl count: {r[0]}")
            else:
                st.error("Invalid credentials")
        st.stop()

    # AUTHENTICATED AREA
    role = st.session_state.role
    username = st.session_state.username

    # ADMIN PANEL
    if role == "admin":
        st.subheader("Admin panel")
        df_users = list_users_df()
        if df_users is not None:
            st.dataframe(df_users)

        st.markdown("---")
        st.subheader("Admin actions")

        col1, col2 = st.columns(2)
        with col1:
            user_to_change = st.text_input("Username to change password")
            new_pass = st.text_input("New password", type="password")
            if st.button("Update Password"):
                if user_to_change and new_pass:
                    update_password(user_to_change, new_pass)
                    st.success(f"Password updated for {user_to_change}")
                else:
                    st.warning("Enter username and new password")
        with col2:
            user_premium = st.text_input("Username to promote to premium")
            if st.button("Promote to Premium"):
                if user_premium:
                    set_premium(user_premium)
                    st.success(f"{user_premium} is now premium")
                else:
                    st.warning("Enter username")
            if st.button("Reset crawl count for user"):
                if user_premium:
                    reset_crawl_count(user_premium)
                    st.success(f"Crawl count reset for {user_premium}")
                else:
                    st.warning("Enter username (same field)")

        st.markdown("---")

    # GENERAL UI
    st.write(f"Logged in as: **{username}** ({role})")
    seed = st.text_input("Seed URL", value=SEED_URLS[0])
    depth = st.slider("Depth", 1, 4, value=MAX_DEPTH)

    # show existing exports
    st.markdown("### Available exports")
    csv_path = os.path.join(OUTPUT_DIR, "seen.csv")
    json_path = os.path.join(OUTPUT_DIR, "seen.json")
    if os.path.exists(csv_path):
        st.write(f"- CSV: {os.path.basename(csv_path)}")
        safe_download_button(csv_path, "Download seen.csv")
    if os.path.exists(json_path):
        st.write(f"- JSON: {os.path.basename(json_path)}")
        safe_download_button(json_path, "Download seen.json")
    if os.path.exists(WARC_PATH):
        st.write(f"- WARC: {os.path.basename(WARC_PATH)}")
        safe_download_button(WARC_PATH, "Download WARC archive")

    # Start crawl
    if st.button("Start Crawl"):
        # check free user limit
        if role == "free":
            conn = db_connection(USERS_DB)
            c = conn.cursor()
            c.execute("SELECT crawl_count FROM users WHERE username=?", (username,))
            r = c.fetchone()
            conn.close()
            if r and r[0] >= 2:
                st.warning("Free user crawl limit reached (2). Contact admin to upgrade.")
                st.stop()
        # set global depth temporarily
        global MAX_DEPTH
        old_depth = MAX_DEPTH
        MAX_DEPTH = depth
        # start crawl threads
        task_queue.put((seed, 0))
        threads = []
        for _ in range(THREADS):
            t = threading.Thread(target=worker, daemon=True)
            t.start()
            threads.append(t)
        task_queue.join()
        # restore old depth
        MAX_DEPTH = old_depth
        # increment crawl counter
        increment_crawl(username)
        # export and show downloads
        csv_file, json_file = export_csv_json()
        if csv_file:
            st.success("Crawl complete — exports generated.")
            safe_download_button(csv_file, "Download CSV")
            safe_download_button(json_file, "Download JSON")
        else:
            st.warning("Crawl finished but export failed. Check logs.")

# =========================
# MAIN
# =========================

def main():
    log("Initializing DBs")
    init_db()
    init_users_db()
    if st is not None:
        run_web_interface()
    else:
        # offline CLI run
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
        log("Offline crawl complete")

if __name__ == "__main__":
    main()
