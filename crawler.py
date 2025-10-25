# crawler.py
import re, json, time, pathlib, urllib.parse, requests
from trafilatura import fetch_url, extract

BASE     = "https://by-cariola.com"
ALLOWED  = (r"^https://by-cariola\.com/products/.*",
            r"^https://by-cariola\.com/collections/.*",
            r"^https://by-cariola\.com/pages/.*")
EXCLUDE  = (r"/blogs/", r"\?variant=", r"/cart", r"/account", r"/search")

OUT_DIR  = pathlib.Path("build")
OUT_DIR.mkdir(exist_ok=True)
chunks   = []

seen = set()
queue = set([
    f"{BASE}/collections/all",
    f"{BASE}/collections",
    f"{BASE}/products"
])

def allowed(u:str)->bool:
    if any(re.search(x,u) for x in EXCLUDE): return False
    return any(re.search(x,u) for x in ALLOWED)

def extract_links(html, base):
    # muy simple… si quieres puedes usar BeautifulSoup
    hrefs = set()
    for m in re.finditer(r'href="([^"]+)"', html):
        u = urllib.parse.urljoin(base, m.group(1))
        if u.startswith(BASE):
            hrefs.add(u.split("#")[0])
    return hrefs

def fetch_and_clean(url):
    html = fetch_url(url)
    if not html: 
        return None, []
    text = extract(html, include_formatting=False, include_tables=False) or ""
    text = re.sub(r"\s+", " ", text).strip()
    links = extract_links(html, url)
    return text, links

def add_chunk(url, title, body):
    if not body: return
    chunks.append({
        "doc_id": url,
        "title": title or url.replace(BASE,'').strip('/'),
        "body": body[:4000],  # trozos razonables
        "url": url
    })

print(">> crawling…")
while queue:
    url = queue.pop()
    if url in seen or not allowed(url): 
        continue
    seen.add(url)

    txt, links = fetch_and_clean(url)
    if not txt: 
        continue

    # título aproximado
    m = re.search(r"(?i)<title>(.*?)</title>", txt)
    title = None
    if m: title = m.group(1)

    add_chunk(url, title, txt)

    for u in links:
        if u not in seen and allowed(u):
            queue.add(u)

# guardar resultados
(OUT_DIR / "chunks.jsonl").write_text(
    "\n".join(json.dumps(c, ensure_ascii=False) for c in chunks),
    encoding="utf-8"
)
print(f">> guardado: {len(chunks)} chunks")

# índices mínimos
(OUT_DIR / "kb.index").write_text(json.dumps({"count": len(chunks)}), encoding="utf-8")
(OUT_DIR / "model.json").write_text(json.dumps({"built_at": int(time.time())}), encoding="utf-8")
