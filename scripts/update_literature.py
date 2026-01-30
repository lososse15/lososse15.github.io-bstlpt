import os
import re
import time
import html
import json
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

LITERATURE_FILE = "literature.html"

# PubMed / NCBI E-utilities base
EUTILS = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"

# NCBI recommends identifying yourself with email; API key is optional but helps rate limits.
NCBI_EMAIL = os.environ.get("NCBI_EMAIL", "example@example.com")
NCBI_API_KEY = os.environ.get("NCBI_API_KEY")  # optional

# How far back to look
DAYS_BACK = int(os.environ.get("DAYS_BACK", "60"))
RETMAX = int(os.environ.get("RETMAX", "12"))

# Queries you care about (edit these anytime)
QUERIES = [
    {
        "section": "Orthopedic & Sports Rehab",
        "query": '("physical therapy"[tiab] OR physiotherapy[tiab] OR rehabilitation[tiab]) '
                 'AND (ACL[tiab] OR "anterior cruciate"[tiab] OR rotator cuff[tiab] OR tendinopathy[tiab] OR knee[tiab])',
    },
    {
        "section": "Low Back Pain & Pain Science",
        "query": '("physical therapy"[tiab] OR physiotherapy[tiab] OR rehabilitation[tiab]) '
                 'AND ("low back pain"[tiab] OR lumbar[tiab] OR chronic pain[tiab])',
    },
    {
        "section": "Neuro / Vestibular / Balance",
        "query": '("physical therapy"[tiab] OR physiotherapy[tiab] OR rehabilitation[tiab]) '
                 'AND (vestibular[tiab] OR concussion[tiab] OR stroke[tiab] OR balance[tiab] OR gait[tiab])',
    },
    {
        "section": "Strength Training & Exercise Prescription",
        "query": '("physical therapy"[tiab] OR physiotherapy[tiab] OR rehabilitation[tiab]) '
                 'AND ("resistance training"[tiab] OR strengthening[tiab] OR "blood flow restriction"[tiab] OR exercise[tiab])',
    },
]

def http_get(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "BSTL-Literature-Updater/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read().decode("utf-8", errors="replace")

def build_params(params: dict) -> str:
    if NCBI_API_KEY:
        params["api_key"] = NCBI_API_KEY
    # identify requester
    params["email"] = NCBI_EMAIL
    return urllib.parse.urlencode(params)

def esearch(term: str, mindate: str, maxdate: str, retmax: int) -> list[str]:
    params = {
        "db": "pubmed",
        "term": term,
        "sort": "date",
        "retmode": "json",
        "retmax": str(retmax),
        "mindate": mindate,
        "maxdate": maxdate,
        "datetype": "pdat",
    }
    url = f"{EUTILS}/esearch.fcgi?{build_params(params)}"
    data = json.loads(http_get(url))
    return data.get("esearchresult", {}).get("idlist", [])

def esummary(ids: list[str]) -> list[dict]:
    if not ids:
        return []
    params = {
        "db": "pubmed",
        "id": ",".join(ids),
        "retmode": "json",
    }
    url = f"{EUTILS}/esummary.fcgi?{build_params(params)}"
    data = json.loads(http_get(url))
    result = data.get("result", {})
    uids = result.get("uids", [])
    items = []
    for uid in uids:
        it = result.get(uid, {})
        items.append(it)
    return items

def pubmed_link(pmid: str) -> str:
    return f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/"

def safe(s: str) -> str:
    return html.escape(s or "")

def format_item(it: dict) -> str:
    pmid = str(it.get("uid", "")).strip()
    title = safe(it.get("title", "")).rstrip(".")
    journal = safe(it.get("fulljournalname", it.get("source", "")))
    pubdate = safe(it.get("pubdate", ""))
    authors = it.get("authors", [])
    first_author = safe(authors[0]["name"]) if authors else ""
    link = pubmed_link(pmid) if pmid else "#"
    return (
        f'<li>'
        f'<strong><a href="{link}" target="_blank" rel="noopener noreferrer">{title}</a></strong>'
        f'<div class="small">{journal} • {pubdate}' + (f' • {first_author}' if first_author else "") + f'</div>'
        f'</li>'
    )

def build_html() -> str:
    now = datetime.now(timezone.utc)
    start = (now - timedelta(days=DAYS_BACK)).strftime("%Y/%m/%d")
    end = now.strftime("%Y/%m/%d")

    blocks = []
    updated_line = now.astimezone(timezone.utc).strftime("%b %d, %Y (UTC)")
    blocks.append(f'<p class="small"><strong>Auto-updated:</strong> {updated_line} • Source: PubMed</p>')

    for q in QUERIES:
        section = safe(q["section"])
        term = q["query"]

        ids = esearch(term=term, mindate=start, maxdate=end, retmax=RETMAX)
        time.sleep(0.34)  # be polite

        summaries = esummary(ids)
        time.sleep(0.34)

        items_html = "\n".join(format_item(it) for it in summaries if it)
        if not items_html:
            items_html = '<li><em>No recent results found for this topic window.</em></li>'

        blocks.append(
            f'<div class="card">'
            f'<h2>{section}</h2>'
            f'<ul class="list">{items_html}</ul>'
            f'</div>'
        )

    # Wrap in grid to match your existing CSS
    return '<div class="grid">\n' + "\n".join(blocks) + "\n</div>"

def inject_into_file(path: str, new_inner_html: str) -> bool:
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()

    pattern = r"<!-- AUTO-LITERATURE:START -->(.*?)<!-- AUTO-LITERATURE:END -->"
    m = re.search(pattern, content, flags=re.DOTALL)
    if not m:
        raise RuntimeError("Markers not found. Add AUTO-LITERATURE markers to literature.html")

    replacement = f"<!-- AUTO-LITERATURE:START -->\n{new_inner_html}\n<!-- AUTO-LITERATURE:END -->"
    updated = re.sub(pattern, replacement, content, flags=re.DOTALL)

    changed = (updated != content)
    if changed:
        with open(path, "w", encoding="utf-8") as f:
            f.write(updated)
    return changed

def main():
    html_block = build_html()
    changed = inject_into_file(LITERATURE_FILE, html_block)
    print("Updated:", changed)

if __name__ == "__main__":
    main()


