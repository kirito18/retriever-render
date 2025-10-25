# retriever_server.py  (drop-in)
import os, re, json, time, argparse, queue
from urllib.parse import urljoin, urlparse
from pathlib import Path
from collections import Counter

import requests
import trafilatura
from bs4 import BeautifulSoup
from unidecode import unidecode
from rapidfuzz import fuzz

from fastapi import FastAPI, Query

# -----------------------
# Archivos de la KB
# -----------------------
BUILD_DIR   = Path("./build")
CHUNKS_FILE = BUILD_DIR / "chunks.jsonl"
INDEX_FILE  = BUILD_DIR / "kb.index"
MODEL_FILE  = BUILD_DIR / "model.json"

app = FastAPI(title="ByCariola Retriever", version="2.0.0")

# -----------------------
# Config vía entorno
# -----------------------
DEFAULT_INCLUDE = r"^https://by-cariola\.com/(products|collections|pages)/.*"
DEFAULT_EXCLUDE = r"(/blogs/|/cart|/account|/search|\?variant=|\.json$)"

INCLUDE_PATTERNS = os.getenv("INCLUDE_PATTERNS", DEFAULT_INCLUDE).split("||")
EXCLUDE_PATTERNS = os.getenv("EXCLUDE_PATTERNS", DEFAULT_EXCLUDE).split("||")

SEED_URLS       = [s.strip() for s in os.getenv("SEED_URLS",
                     "https://by-cariola.com/collections/all,https://by-cariola.com/products").split(",") if s.strip()]
ALLOWED_DOMAINS = [d.strip() for d in os.getenv("ALLOWED_DOMAINS", "by-cariola.com").split(",") if d.strip()]
MAX_PAGES       = int(os.getenv("MAX_PAGES", "600"))

UA = {"User-Agent": "ByCariola-Retriever/2.0 (+https://by-cariola.com)"}

# -----------------------
# Utilidades de limpieza
# -----------------------
def norm_text(s: str) -> str:
    s = s or ""
    s = unidecode(s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def same_domain(u, allowed):
    try:
        net = urlparse(u).netloc
    except:
        return False
    return any(net.endswith(d) for d in allowed)

def is_asset(url: str) -> bool:
    return bool(re.search(r"\.(png|jpe?g|gif|svg|webp|pdf|mp4|css|js|woff2?)($|\?)", url, re.I))

def is_included(url: str) -> bool:
    if any(re.search(p, url) for p in EXCLUDE_PATTERNS):
        return False
    return any(re.search(p, url) for p in INCLUDE_PATTERNS)

def extract_links(html: str, base: str):
    soup = BeautifulSoup(html, "lxml")
    out = set()
    for a in soup.select("a[href]"):
        href = a.get("href")
        if not href:
            continue
        u = urljoin(base, href).split("#")[0]
        out.add(u)
    return out

def fetch_html(url: str):
    try:
        r = requests.get(url, timeout=15, headers=UA)
        if r.status_code >= 400:
            return None
        return r.text
    except:
        return None

def fetch_clean_text(url: str):
    try:
        downloaded = trafilatura.fetch_url(url)
        if not downloaded:
            return ""
        # extrae texto limpio (sin tablas/comentarios)
        text = trafilatura.extract(downloaded, include_comments=False, include_tables=False) or ""
        return text
    except:
        return ""

# -----------------------
# Crawler + build de KB
# -----------------------
def crawl_and_build(seeds, allowed_domains, max_pages=200):
    visited = set()
    encola   = queue.Queue()
    for s in seeds:
        encola.put(s)

    docs = []  # [{doc_id,title,body,url}]
    while not encola.empty() and len(visited) < max_pages:
        url = encola.get()
        if url in visited: 
            continue
        visited.add(url)

        if not same_domain(url, allowed_domains): 
            continue
        if is_asset(url): 
            continue
        if not is_included(url):
            # si no cumple include, no lo indexamos ni expandimos
            continue

        html = fetch_html(url)
        if not html:
            continue

        # texto principal
        text = fetch_clean_text(url)
        if not text:
            soup_raw = BeautifulSoup(html, "lxml")
            text = soup_raw.get_text(" ", strip=True)

        soup = BeautifulSoup(html, "lxml")
        title = soup.title.get_text(strip=True) if soup.title else url

        body = norm_text(text)
        title = norm_text(title)

        if not body or len(body) < 120:
            continue

        docs.append({
            "doc_id": f"doc_{len(docs)}",
            "title" : title,
            "body"  : body,
            "url"   : url
        })

        # seguimos crawleando SOLO los enlaces dentro del include
        for link in extract_links(html, url):
            if link in visited: 
                continue
            if not same_domain(link, allowed_domains):
                continue
            if is_asset(link):
                continue
            if is_included(link):
                encola.put(link)

    BUILD_DIR.mkdir(parents=True, exist_ok=True)

    with CHUNKS_FILE.open("w", encoding="utf-8") as f:
        for d in docs:
            f.write(json.dumps(d, ensure_ascii=False) + "\n")

    vocab = Counter()
    for d in docs:
        tokens = re.findall(r"[a-zA-Z0-9áéíóúñ]+", d["title"] + " " + d["body"], flags=re.I)
        for t in tokens:
            vocab[unidecode(t.lower())] += 1

    with INDEX_FILE.open("w", encoding="utf-8") as f:
        f.write(json.dumps({"size": len(docs), "vocab_size": len(vocab)}, ensure_ascii=False))

    with MODEL_FILE.open("w", encoding="utf-8") as f:
        f.write(json.dumps({"built_at": int(time.time())}, ensure_ascii=False))

    return len(docs)

# -----------------------
# Búsqueda en memoria
# -----------------------
_DOCS = []

def load_docs():
    global _DOCS
    _DOCS = []
    if not CHUNKS_FILE.exists():
        return
    with CHUNKS_FILE.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                _DOCS.append(json.loads(line))
            except:
                pass

def score_doc(q: str, d: dict) -> float:
    ql = q.lower()
    title = d["title"].lower()
    body  = d["body"].lower()

    s1 = fuzz.partial_ratio(ql, title)
    s2 = fuzz.partial_ratio(ql, body)
    score = s1 * 1.5 + s2 * 0.7

    url = d.get("url", "")
    if "/products/" in url:
        score += 6.0     # BOOST productos
    if "/collections/" in url:
        score += 3.0
    if "/blogs/" in url:
        score -= 4.0     # PENALIZA blog

    return score

def search_docs(query, top_k=6):
    q = norm_text(query)
    scored = [(score_doc(q, d), d) for d in _DOCS]
    scored.sort(key=lambda x: x[0], reverse=True)
    hits = [d for _, d in scored[:top_k]]
    for h in hits:
        h["body"] = h["body"][:900]
    return hits

# -----------------------
# Endpoints FastAPI
# -----------------------
@app.get("/healthz")
def healthz():
    ok = CHUNKS_FILE.exists() and INDEX_FILE.exists() and MODEL_FILE.exists()
    return {"ok": ok, "docs": len(_DOCS)}

@app.get("/search")
def search(q: str = Query(..., min_length=2), top_k: int = 6):
    return {"results": search_docs(q, top_k=top_k)}

@app.post("/rebuild")
def rebuild():
    n = crawl_and_build(SEED_URLS, ALLOWED_DOMAINS, MAX_PAGES)
    load_docs()
    return {"rebuilt": True, "docs": n}

# -----------------------
# CLI Build (Render)
# -----------------------
def cli_build():
    p = argparse.ArgumentParser()
    p.add_argument("--build", action="store_true", help="Crawl + build KB and exit")
    p.add_argument("--seeds",   type=str, default=",".join(SEED_URLS))
    p.add_argument("--allowed", type=str, default=",".join(ALLOWED_DOMAINS))
    p.add_argument("--max",     type=int, default=MAX_PAGES)
    args = p.parse_args()

    if args.build:
        seeds   = [s.strip() for s in args.seeds.split(",") if s.strip()]
        allowed = [a.strip() for a in args.allowed.split(",") if a.strip()]
        n = crawl_and_build(seeds, allowed, max_pages=args.max)
        print(f"[BUILD] Indexed docs: {n}")
        return True
    return False

if __name__ == "__main__":
    ran = cli_build()
    if not ran:
        load_docs()
        import uvicorn
        uvicorn.run("retriever_server:app", host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
else:
    load_docs()
