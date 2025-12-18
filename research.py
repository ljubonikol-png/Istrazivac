# =============================================
# Istrazivac Research Platform
# Streamlit Web Interface + Crawler
# Finalna verzija
# =============================================

import os
import time
import json
import base64
import queue
import threading
import hashlib
import requests
import warnings
from datetime import datetime
from urllib.parse import urljoin
from io import BytesIO

import streamlit as st
from bs4 import BeautifulSoup
from warcio.warcwriter import WARCWriter
from requests.packages.urllib3.exceptions import InsecureRequestWarning

# =============================================
# CONFIG
# =============================================

ROOT = os.getcwd()
PAYLOAD_DIR = os.path.join(ROOT, "payloads")
WARC_PATH = os.path.join(ROOT, "archive.warc.gz")

SEED_URLS_DEFAULT = ["https://www.gov.rs/"]
MAX_DEPTH_DEFAULT = 2
THREADS = 4
TIMEOUT = 20
USER_AGENT = "GarbageMan/1.0 (archival crawler; TLS relaxed)"

GITHUB_API = "https://api.github.com"
STORAGE_PATH = "data/storage.json"

os.makedirs(PAYLOAD_DIR, exist_ok=True)
warnings.simplefilter("ignore", InsecureRequestWarning)

# =============================================
# GITHUB STORAGE
# =============================================

def gh_headers():
    return {
        "Authorization": f"token {st.secrets['GITHUB_TOKEN']}",
        "Accept": "application/vnd.github.v3+json"
    }

def load_storage():
    url = f"{GITHUB_API}/repos/{st.secrets['GITHUB_REPO']}/contents/{STORAGE_PATH}"
    r = requests.get(url, headers=gh_headers())
    if r.status_code != 200:
        st.error(f"GitHub API error: {r.status_code} - provjeri token i repo path")
        st.stop()
    data = r.json()
    content = base64.b64decode(data["content"]).decode("utf-8")
    return json.loads(content), data["sha"]

def save_storage(storage, sha, message="Update storage"):
    url = f"{GITHUB_API}/repos/{st.secrets['GITHUB_REPO']}/contents/{STORAGE_PATH}"
    payload = {
        "message": message,
        "content": base64.b64encode(json.dumps(storage, indent=2).encode()).decode(),
        "sha": sha
    }
    r = requests.put(url, headers=gh_headers(), json=payload)
    if r.status_code not in (200, 201):
        st.error(f"Failed to write storage.json ({r.status_code})")
        st.stop()

# =============================================
# AUTH & USER TRACKING
# =============================================

def authenticate(username):
    storage, sha = load_storage()
    if username not in storage.get("users", {}):
        storage.setdefault("users", {})[username] = {
            "role": "free",
            "created_at": str(datetime.utcnow()),
            "crawl_count": 0
        }
        save_storage(storage, sha, "Add new user")
    st.session_state.user = username
    st.session_state.role = storage["users"][username]["role"]

def increment_crawl():
    storage, sha = load_storage()
    u = st.session_state.user
    storage["users"][u]["crawl_count"] += 1
    storage.setdefault("stats", {}).setdefault("total_crawls", 0)
    storage["stats"]["total_crawls"] += 1
    save_storage(storage, sha, "Increment crawl count")

def can_crawl():
    storage, _ = load_storage()
    u = st.session_state.user
    role = storage["users"][u]["role"]
    count = storage["users"][u]["crawl_count"]
    if role in ("admin", "premium"):
        return True
    return count < 2

# =============================================
# CRAWLER CORE
# =============================================

session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT})

task_queue = queue.Queue()
warc_lock = threading.Lock()

def fetch(url):
    try:
        r = session.get(url, timeout=TIMEOUT, verify=False, allow_redirects=True)
        return r.content, r.headers.get("Content-Type", "")
    except Exception:
        return None, None

def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def process_url(url, depth, max_depth, warc_writer):
    data, ctype = fetch(url)
    if not data:
        return
    h = sha256(data)
    with open(os.path.join(PAYLOAD_DIR, h), "wb") as f:
        f.write(data)
    with warc_lock:
        record = warc_writer.create_warc_record(url, "resource", payload=BytesIO(data))
        warc_writer.write_record(record)
    if depth >= max_depth or "html" not in (ctype or "").lower():
        return
    soup = BeautifulSoup(data, "lxml")
    for a in soup.find_all("a", href=True):
        link = urljoin(url, a["href"])
        if link.startswith("http"):
            task_queue.put((link, depth + 1))

def worker(max_depth, warc_writer):
    while True:
        try:
            url, depth = task_queue.get(timeout=2)
        except queue.Empty:
            break
        try:
            process_url(url, depth, max_depth, warc_writer)
        finally:
            task_queue.task_done()

def run_crawler(seeds, max_depth):
    increment_crawl()
    with open(WARC_PATH, "ab") as wf:
        warc_writer = WARCWriter(wf, gzip=True)
        for u in seeds:
            task_queue.put((u, 0))
        threads = []
        for _ in range(THREADS):
            t = threading.Thread(target=worker, args=(max_depth, warc_writer), daemon=True)
            t.start()
            threads.append(t)
        task_queue.join()

# =============================================
# STREAMLIT UI
# =============================================

def main():
    st.set_page_config(page_title="GarbageMan Web Crawler")
    st.title("GarbageMan Web Crawler")
    
    # LOGIN
    if "user" not in st.session_state:
        username = st.text_input("Username")
        if st.button("Login") and username:
            authenticate(username)

    if "user" in st.session_state:
        st.success(f"Logged in as {st.session_state.user} ({st.session_state.role})")
        
        if not can_crawl():
            st.error("Free limit reached. Contact admin for upgrade.")
            return
        
        seed_url = st.text_input("Seed URL", SEED_URLS_DEFAULT[0])
        max_depth = st.slider("Max depth", 1, 5, MAX_DEPTH_DEFAULT)
        
        if st.button("Start Crawl"):
            run_crawler([seed_url], max_depth)
            st.success("Crawl complete")
        
        if st.session_state.role == "admin":
            st.subheader("Admin panel")
            storage, _ = load_storage()
            st.json(storage)

if __name__ == "__main__":
    main()
