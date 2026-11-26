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
import socket
import ipaddress
from datetime import datetime
from urllib.parse import urljoin
from io import BytesIO

import streamlit as st
from bs4 import BeautifulSoup
from warcio.warcwriter import WARCWriter


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
USER_AGENT = "GarbageMan/1.0 (archival crawler; TLS verified)"

GITHUB_API = "https://api.github.com"
STORAGE_PATH = "data/storage.json"

os.makedirs(PAYLOAD_DIR, exist_ok=True)


# =============================================
# =============================================
# AUTHENTICATION
# =============================================

def authenticate(password):
    expected = os.environ.get("ISTRAZIVAC_ADMIN_PASSWORD", "")
    if not expected or len(expected) < 16:
        st.error("Admin authentication is not configured (password minimum 16 characters).")
        return False
    import hmac
    if not hmac.compare_digest(password, expected):
        st.error("Invalid password.")
        return False
    st.session_state.user = "admin"
    st.session_state.role = "admin"
    return True


# CRAWLER CORE
# =============================================

session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT})
task_queue = queue.Queue()
warc_lock = threading.Lock()
visited_lock = threading.Lock()
visited_urls = set()
MAX_PAGES_DEFAULT = 100


def normalize_url(url):
    try:
        parsed = requests.utils.urlparse(url.strip())
        if parsed.scheme not in ("http", "https") or not parsed.hostname or parsed.username or parsed.password:
            return None
        if parsed.port not in (None, 80, 443):
            return None
        host = parsed.hostname.rstrip(".").lower()
        if not host:
            return None
        normalized = parsed._replace(fragment="", netloc=host if parsed.port is None else f"{host}:{parsed.port}")
        return requests.utils.urlunparse(normalized)
    except (ValueError, AttributeError):
        return None


def is_public_host(hostname):
    try:
        addresses = {item[4][0] for item in socket.getaddrinfo(hostname, None, type=socket.SOCK_STREAM)}
        if not addresses:
            return False
        for address in addresses:
            ip = ipaddress.ip_address(address.split("%", 1)[0])
            if not ip.is_global or ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved or ip.is_multicast:
                return False
        return True
    except (OSError, ValueError):
        return False


def enqueue_url(url, depth, max_pages):
    normalized = normalize_url(url)
    if not normalized:
        return False
    with visited_lock:
        if normalized in visited_urls or len(visited_urls) >= max_pages:
            return False
        visited_urls.add(normalized)
        task_queue.put((normalized, depth))
    return True


def fetch(url):
    current = normalize_url(url)
    if not current:
        return None, None
    for _ in range(6):
        host = requests.utils.urlparse(current).hostname
        if not host or not is_public_host(host):
            return None, None
        try:
            response = requests.get(current, timeout=TIMEOUT, verify=True, allow_redirects=False, headers={"User-Agent": USER_AGENT})
            if response.is_redirect:
                location = response.headers.get("Location")
                if not location:
                    return None, None
                current = normalize_url(urljoin(current, location))
                if not current:
                    return None, None
                continue
            response.raise_for_status()
            return response.content, response.headers.get("Content-Type", "")
        except requests.RequestException:
            return None, None
    return None, None


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()



def process_url(url, depth, max_depth, max_pages, warc_writer):
    data, ctype = fetch(url)
    if not data:
        return
    h = sha256(data)
    payload_path = os.path.join(PAYLOAD_DIR, h)
    if not os.path.exists(payload_path):
        with open(payload_path, "wb") as f:
            f.write(data)
    with warc_lock:
        record = warc_writer.create_warc_record(url, "resource", payload=BytesIO(data))
        warc_writer.write_record(record)
    if depth >= max_depth or "html" not in (ctype or "").lower():
        return
    soup = BeautifulSoup(data, "lxml")
    for anchor in soup.find_all("a", href=True):
        link = urljoin(url, anchor["href"])
        enqueue_url(link, depth + 1, max_pages)


def worker(max_depth, max_pages, warc_writer):
    while True:
        try:
            url, depth = task_queue.get(timeout=2)
        except queue.Empty:
            break
        try:
            process_url(url, depth, max_depth, max_pages, warc_writer)
        finally:
            task_queue.task_done()


def run_crawler(seeds, max_depth, max_pages=MAX_PAGES_DEFAULT):
    with visited_lock:
        visited_urls.clear()
    while not task_queue.empty():
        try:
            task_queue.get_nowait()
            task_queue.task_done()
        except queue.Empty:
            break
    with open(WARC_PATH, "ab") as wf:
        warc_writer = WARCWriter(wf, gzip=True)
        for seed in seeds:
            enqueue_url(seed, 0, max_pages)
        threads = []
        for _ in range(min(THREADS, max_pages)):
            t = threading.Thread(target=worker, args=(max_depth, max_pages, warc_writer), daemon=True)
            t.start()
            threads.append(t)
        task_queue.join()
        for t in threads:
            t.join(timeout=3)
# STREAMLIT UI
# =============================================

def main():
    st.set_page_config(page_title="GarbageMan Web Crawler")
    st.title("GarbageMan Web Crawler")
    
    # Admin-only password authentication; no username-only access.
    if "user" not in st.session_state:
        password = st.text_input("Admin password", type="password")
        if st.button("Login") and password:
            authenticate(password)

    if "user" in st.session_state:
        st.success(f"Logged in as {st.session_state.user} ({st.session_state.role})")
        
        seed_url = st.text_input("Seed URL", SEED_URLS_DEFAULT[0])
        max_depth = st.slider("Max depth", 1, 5, MAX_DEPTH_DEFAULT)
        max_pages = st.slider("Maximum pages", 10, 500, MAX_PAGES_DEFAULT, step=10)
        
        if st.button("Start Crawl"):
            if not normalize_url(seed_url):
                st.error("Seed URL must be a valid HTTP/HTTPS URL without embedded credentials.")
            else:
                run_crawler([seed_url], max_depth, max_pages)
                st.success("Crawl complete")
        

if __name__ == "__main__":
    main()
