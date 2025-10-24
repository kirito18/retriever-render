# retriever_server.py
import os, re, json, time, argparse, queue
from urllib.parse import urljoin, urlparse
from pathlib import Path
from collections import defaultdict, Counter

import trafilatura
import requests
from bs4 import BeautifulSoup
from unidecode import unidecode
from rapidfuzz import fuzz, process

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

BUILD_DIR = Path("./build")
CHUNKS_FILE = BUILD_DIR / "chunks.jsonl"
INDEX_FILE  = BUILD_DIR / "kb.index"
MODEL_FILE  = BUILD_DIR / "model.json"

app = FastAPI(title="ByCariola Retriever", version="1.0.0")

# -----------------------
# Normalización utilities
# -----------------------
def norm_text(s: str) -> str:
    s = s or ""
    s = unidecode(s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def same_domain(u, allowed):
    try:
        netloc = urlparse(u).netloc
    except:
        return False
    return any(netloc.endswith(d) for d in allowed)

# -----------------------
# Crawl
# -----------------------
def extract_links(html, base):
    soup = BeautifulSoup(html, "lxml")
    out = []
    for a in soup.select("a[href]"):
        href = a.get("href")
        if not href: 
            continue
        out.append(urljoin(base, href))
    return out

def fetch_html(url):
    try:
        r = requests.get(url, timeout=12, headers={"User-Agent": "RetrieverBot/1.0"})
        if r.status_code >= 400:
            return None
        return r.text
    except:
        return None

def fetch_clean_text(url):
    # trafilatura devuelve texto legible (mejor que raw HTML)
    try:
        downloaded = trafilatura.fetch_url(url)
        if not downloaded:
            return ""
        text = trafilatura.extract(downloaded, include_comments=False, include_tables=False) or ""
        return text
    except:
        return ""

def is_indexable(url):
    # ignora assets comunes
    return not re.search(r"\.(png|jpe?g|gif|svg|webp|pdf|mp4|zip|rar|css|js|woff2?)$", url, re.I)

def crawl_and_build(seeds, allowed_domains, max_pages=200):
    visited = set()
    q = queue.Queue()
    for s in seeds: 
        q.put(s)

    docs = []  # [{"doc_id","title","body","url"}]
    while not q.empty() and len(visited) < max_pages:
        url = q.get()
        if url in visited: 
            continue
        visited.add(url)

        if not same_domain(url, allowed_domains): 
            continue
        if not is_indexable(url): 
            continue

        html = fetch_html(url)
        if not html: 
            continue

        # texto principal
        text = fetch_clean_text(url)
        if not text:
            # fallback: título simple
            soup = BeautifulSoup(html, "lxml")
            text = soup.get_text(" ", strip=True)

        soup = BeautifulSoup(html, "lxml")
        title = soup.title.get_text(strip=True) if soup.title else url

        body = norm_text(text)
        title = norm_text(title)

        if not body or len(body) < 100:
            continue

        docs.append({
            "doc_id": f"doc_{len(docs)}",
            "title": title,
            "body": body,
            "url": url
        })

        # nuevos links
        for link in extract_links(html, url):
            if link not in visited and same_domain(link, allowed_domains) and is_indexable(link):
                q.put(link)

    BUILD_DIR.mkdir(parents=True, exist_ok=True)

    # muy simple "indexado": guardamos docs y un vocabulario de frecuencias
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
# Search
# -----------------------
_DOCS = []  # cargados en memoria al arrancar

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

def search_docs(query, top_k=6):
    q = norm_text(query).lower()
    # fusión: título + body con fuzzy; prioriza matches en título
    scored = []
    for d in _DOCS:
        title = d["title"].lower()
        body  = d["body"].lower()
        s1 = fuzz.partial_ratio(q, title)
        s2 = fuzz.partial_ratio(q, body)
        score = s1 * 1.5 + s2 * 0.7
        scored.append((score, d))
    scored.sort(key=lambda x: x[0], reverse=True)
    hits = [h[1] for h in scored[:top_k]]
    # achicar el body
    for h in hits:
        h["body"] = h["body"][:900]
    return hits

# -----------------------
# FastAPI endpoints
# -----------------------
@app.get("/healthz")
def healthz():
    ok = CHUNKS_FILE.exists() and INDEX_FILE.exists() and MODEL_FILE.exists()
    return {"ok": ok, "docs": len(_DOCS)}

@app.get("/search")
def search(q: str = Query(..., min_length=2), top_k: int = 6):
    hits = search_docs(q, top_k=top_k)
    return {"results": hits}

@app.post("/rebuild")
def rebuild():
    # opcional: protege con token de entorno
    token_env = os.getenv("REBUILD_TOKEN", "")
    # si quieres, valida token en JSON body o header; aquí lo omitimos para simplificar
    seeds = os.getenv("SEED_URLS", "https://by-cariola.com").split(",")
    allowed = os.getenv("ALLOWED_DOMAINS", "by-cariola.com").split(",")
    max_pages = int(os.getenv("MAX_PAGES", "220"))
    n = crawl_and_build(seeds, allowed, max_pages=max_pages)
    load_docs()
    return {"rebuilt": True, "docs": n}

# -----------------------
# CLI Build (para Render)
# -----------------------
def cli_build():
    parser = argparse.ArgumentParser()
    parser.add_argument("--build", action="store_true", help="Run crawler + build index and exit")
    parser.add_argument("--seeds", type=str, default=os.getenv("SEED_URLS", "https://by-cariola.com"))
    parser.add_argument("--allowed", type=str, default=os.getenv("ALLOWED_DOMAINS", "by-cariola.com"))
    parser.add_argument("--max", type=int, default=int(os.getenv("MAX_PAGES", "220")))
    args = parser.parse_args()

    if args.build:
        seeds = [s.strip() for s in args.seeds.split(",") if s.strip()]
        allowed = [a.strip() for a in args.allowed.split(",") if a.strip()]
        n = crawl_and_build(seeds, allowed, max_pages=args.max)
        print(f"[BUILD] Indexed docs: {n}")
        return True
    return False

if __name__ == "__main__":
    # si viene con --build, ejecuta crawling y termina
    ran_build = cli_build()
    if not ran_build:
        # modo servidor local
        load_docs()
        import uvicorn
        port = int(os.getenv("PORT", "10000"))
        uvicorn.run("retriever_server:app", host="0.0.0.0", port=port, reload=False)
else:
    # cuando Render arranca el web service
    load_docs()
