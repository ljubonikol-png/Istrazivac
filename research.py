# ======= BYTE-TO-BYTE ispravljena research.py =======
import os,time,threading,queue,hashlib,sqlite3,requests,warnings
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from warcio.warcwriter import WARCWriter
from io import BytesIO
from requests.packages.urllib3.exceptions import InsecureRequestWarning

global ROOT, DB_PATH, WARC_PATH, PAYLOAD_DIR, SEED_URLS, MAX_DEPTH, THREADS, TIMEOUT, USER_AGENT

ROOT = r'E:\GarbageMan'
DB_PATH = os.path.join(ROOT,'data','crawler.db')
WARC_PATH = os.path.join(ROOT,'data','archive.warc.gz')
PAYLOAD_DIR = os.path.join(ROOT,'data','payloads')
SEED_URLS = ['https://www.gov.rs/']
MAX_DEPTH = 2
THREADS = 4
TIMEOUT = 20
USER_AGENT = 'GarbageMan/1.0 (archival crawler; TLS relaxed)'

os.makedirs(PAYLOAD_DIR,exist_ok=True)

def log(msg): print(time.strftime('[%H:%M:%S]'),msg,flush=True)
def init_db(): conn=sqlite3.connect(DB_PATH);c=conn.cursor();c.execute("PRAGMA journal_mode=WAL;");c.execute("CREATE TABLE IF NOT EXISTS seen (url TEXT PRIMARY KEY,hash TEXT)");conn.commit();conn.close()
def db_connection(): conn=sqlite3.connect(DB_PATH,check_same_thread=False);conn.execute("PRAGMA journal_mode=WAL;");return conn

USERS_DB_PATH=os.path.join(ROOT,'data','users.db')
def init_users_db():
    global USERS_DB_PATH
    conn=sqlite3.connect(USERS_DB_PATH)
    c=conn.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY,password TEXT,role TEXT,crawl_count INTEGER DEFAULT 0,created_at TEXT)")
    import time
    default_users=[("admin","MiraAndjaDiki.,.,1234567890,.,.","admin"),("freeuser","freepass","free"),("premiumuser","1234","premium")]
    for u,p,r in default_users:
        c.execute("SELECT 1 FROM users WHERE username=?",(u,))
        if not c.fetchone():
            c.execute("INSERT INTO users (username,password,role,crawl_count,created_at) VALUES (?,?,?,?,?)",(u,p,r,0,time.ctime()))
    conn.commit()
    conn.close()

warnings.simplefilter("ignore",InsecureRequestWarning)
session=requests.Session()
session.headers.update({"User-Agent":USER_AGENT})
def fetch(url):
    try: r=session.get(url,timeout=TIMEOUT,verify=False,allow_redirects=True); return r.content,r.headers.get("Content-Type","")
    except Exception as e: log(f"fetch error {url} : {e}"); return None,None
def sha256(data:bytes)->str: return hashlib.sha256(data).hexdigest()
warc_lock=threading.Lock()
warc_file=open(WARC_PATH,"ab")
warc_writer=WARCWriter(warc_file,gzip=True)
def write_warc(url,payload:bytes):
    with warc_lock:
        record=warc_writer.create_warc_record(url,record_type="resource",payload=BytesIO(payload))
        warc_writer.write_record(record)

task_queue=queue.Queue()
seen_lock=threading.Lock()

def process_url(url,depth,conn):
    global MAX_DEPTH
    c=conn.cursor()
    c.execute("SELECT 1 FROM seen WHERE url=?",(url,))
    if c.fetchone(): return
    log(f"fetching d={depth} {url}")
    data,ctype=fetch(url)
    if not data: return
    h=sha256(data)
    with seen_lock: c.execute("INSERT OR IGNORE INTO seen(url,hash) VALUES (?,?)",(url,h));conn.commit()
    with open(os.path.join(PAYLOAD_DIR,h),"wb") as f: f.write(data)
    write_warc(url,data)
    if depth>=MAX_DEPTH: return
    if "html" not in (ctype or "").lower(): return
    try: soup=BeautifulSoup(data,"lxml");[task_queue.put((urljoin(url,a['href']),depth+1)) for a in soup.find_all('a',href=True) if urljoin(url,a['href']).startswith("http")]
    except Exception as e: log(f"parse error {url} : {e}")

def worker():
    conn=db_connection()
    while True:
        try: url,depth=task_queue.get(timeout=3)
        except queue.Empty: break
        try: process_url(url,depth,conn)
        finally: task_queue.task_done()
    conn.close()

def main():
    log("initializing database")
    init_db()
    log("initializing users database")
    init_users_db()
    log("seeding crawl")
    [task_queue.put((u,0)) for u in SEED_URLS]
    log("starting workers")
    threads=[threading.Thread(target=worker,daemon=True) for _ in range(THREADS)]
    [t.start() for t in threads]
    task_queue.join()
    warc_file.close()
    log("crawl complete")

if __name__=="__main__":
    main()
