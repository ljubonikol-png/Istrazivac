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
import csv
import json
import glob

# =========================
# CONFIG
# =========================
ROOT = r"E:\GarbageMan"
DB_PATH = os.path.join(ROOT, "crawler.db")
WARC_PATH = os.path.join(ROOT, "archive.warc.gz")
PAYLOAD_DIR = os.path.join(ROOT, "payloads")
OUTPUT_DIR = os.path.join(ROOT, "output")
USERS_DB = os.path.join(ROOT, "users.db")

SEED_URLS = ["https://www.gov.rs/"]
MAX_DEPTH = 2
THREADS = 4
TIMEOUT = 20
USER_AGENT = "GarbageMan/1.0 (archival crawler; TLS relaxed)"

os.makedirs(PAYLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# =========================
# LOGGING
# =========================
log_file = os.path.join(ROOT, "crawler.log")
def log(msg):
    timestamp = time.strftime("[%H:%M:%S]")
    line = f"{timestamp} {msg}"
    print(line, flush=True)
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(line + "\n")

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
warnings.simplefilter("ignore", requests.packages.urllib3.exceptions.InsecureRequestWarning)
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
def export_seen():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT url, hash FROM seen")
    rows = c.fetchall()
    conn.close()
    csv_file = os.path.join(OUTPUT_DIR, "seen.csv")
    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["url", "hash"])
        writer.writerows(rows)
    log(f"CSV export zavrsen: {csv_file}")
    json_file = os.path.join(OUTPUT_DIR, "seen.json")
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump([{"url": u, "hash": h} for u, h in rows], f, indent=2)
    log(f"JSON export zavrsen: {json_file}")

# =========================
# USERS DB
# =========================
def init_users_db():
    conn = sqlite3.connect(USERS_DB)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY,
            password TEXT,
            premium INTEGER DEFAULT 0
        )
    """)
    c.execute("INSERT OR IGNORE INTO users(username, password, premium) VALUES (?, ?, ?)", ("freeuser","freepass",0))
    c.execute("INSERT OR IGNORE INTO users(username, password, premium) VALUES (?, ?, ?)", ("premiumuser","1234",1))
    conn.commit()
    conn.close()

# =========================
# MAIN
# =========================
def main():
    log("initializing database")
    init_db()
    log("seeding crawl")
    for u in SEED_URLS:
        task_queue.put((u, 0))
    log("starting workers")
    threads = []
    for _ in range(THREADS):
        t = threading.Thread(target=worker, daemon=True)
        t.start()
        threads.append(t)
    task_queue.join()
    warc_file.close()
    export_seen()
    log("crawl complete")

# =========================
# STREAMLIT WEB INTERFACE
# =========================
def run_web_interface():
    try:
        import streamlit as st
        import threading
        import pandas as pd
        import base64

        st.title("GarbageMan Web Archiver")

        # =========================
        # LOGIN
        # =========================
        premium_status = False
        username = st.text_input("Korisničko ime")
        password = st.text_input("Lozinka", type="password")

        if username and password:
            conn = sqlite3.connect(USERS_DB)
            c = conn.cursor()
            c.execute("SELECT premium FROM users WHERE username=? AND password=?", (username, password))
            row = c.fetchone()
            conn.close()

            if not row:
                st.warning("Pogrešno korisničko ime ili lozinka")
                st.stop()
            else:
                premium_status = bool(row[0])
                st.success(f"Prijavljen kao {username} | {'Premium' if premium_status else 'Free'}")
        else:
            st.stop()

        # =========================
        # CRAWL PARAMS
        # =========================
        url = st.text_input("Unesi URL za arhiviranje", value=SEED_URLS[0])
        depth = st.slider("Dubina pretrage", 1, 5, value=MAX_DEPTH)

        if "crawl_count" not in st.session_state:
            st.session_state.crawl_count = 0

        if not premium_status and st.session_state.crawl_count >= 2:
            st.warning("Dostigli ste limit broja crawl-ova po sesiji. Platite pretplatu za više.")
            st.stop()

        if st.button("Pokreni Crawl"):
            if not premium_status:
                st.session_state.crawl_count += 1
            st.write(f"Pokrećem crawl broj {st.session_state.crawl_count if not premium_status else 'Unlimited'} za {username}")
            def crawl_thread():
                global SEED_URLS, MAX_DEPTH
                SEED_URLS = [url]
                MAX_DEPTH = depth
                main()
            t = threading.Thread(target=crawl_thread, daemon=True)
            t.start()
            st.success("Crawl pokrenut!")

        # =========================
        # DOWNLOAD BUTTONS
        # =========================
        st.write("Folder output sadrži: seen.csv, seen.json i log fajl.")
        st.write("Klikni na dugme da preuzmeš fajlove:")
        def make_download_button(file_path, label):
            with open(file_path, "rb") as f:
                data = f.read()
            b64 = base64.b64encode(data).decode()
            href = f'<a href="data:application/octet-stream;base64,{b64}" download="{os.path.basename(file_path)}">{label}</a>'
            st.markdown(href, unsafe_allow_html=True)

        for file_path in glob.glob(os.path.join(OUTPUT_DIR, "*")):
            make_download_button(file_path, f"Preuzmi {os.path.basename(file_path)}")
        if os.path.exists(WARC_PATH):
            make_download_button(WARC_PATH, f"Preuzmi {os.path.basename(WARC_PATH)}")

        # =========================
        # SEARCH FILTER
        # =========================
        st.write("Pretraga fajlova u CSV/JSON:")
        search_query = st.text_input("Pretraži URL ili hash")
        if search_query:
            csv_file = os.path.join(OUTPUT_DIR, "seen.csv")
            if os.path.exists(csv_file):
                df = pd.read_csv(csv_file)
                filtered = df[df["url"].str.contains(search_query, case=False, na=False) |
                              df["hash"].str.contains(search_query, case=False, na=False)]
                st.write(f"Pronađeno {len(filtered)} rezultata:")
                st.dataframe(filtered)
                if st.button("Preuzmi filtrirane rezultate kao CSV"):
                    filtered_csv_path = os.path.join(OUTPUT_DIR, f"filtered_{int(time.time())}.csv")
                    filtered.to_csv(filtered_csv_path, index=False)
                    make_download_button(filtered_csv_path, f"Preuzmi filtrirani CSV")
            else:
                st.write("CSV fajl ne postoji. Pokreni crawl prvo.")

    except ImportError:
        print("Streamlit nije instaliran. Instaliraj preko 'pip install streamlit'.")

# =========================
# START SCRIPT
# =========================
if __name__ == "__main__":
    init_users_db()
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "web":
        run_web_interface()
    else:
        main()
