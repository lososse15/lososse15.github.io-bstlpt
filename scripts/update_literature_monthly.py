import os
import re
import time
import html
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone

LITERATURE_FILE = "literature.html"
EUTILS = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"

NCBI_EMAIL = os.environ.get("NCBI_EMAIL", "example@example.com")
NCBI_API_KEY = os.environ.get("NCBI_API_KEY")  # optional

# Past 2 years window
DAYS_BACK = 365 * 2

# One new article per section per run (monthly)
RETMAX = 1

SECTIONS = [
    {
        "name": "Orthopedics",
        # PT + ortho keywords
        "query": (
            '("physical therapy"[tiab] OR physiotherapy[tiab] OR rehabilitation[tiab]) '
            'AND (knee[tiab] OR hip[tiab] OR shoulder[tiab] OR rotator cuff[tiab] OR '
            'tendinopathy[tiab] OR osteoarthritis[tiab] OR meniscus[tiab] OR post-operative[tiab])'
        ),
    },
    {
        "name": "Geriatrics",
        "query": (
            '("physical therapy"[tiab] OR physiotherapy[tiab] OR rehabilitation[tiab]) '
            'AND (older adults[tiab] OR geriatric[tiab] OR frailty[tiab] OR falls[tiab] OR '
            'sarcopenia[tiab] OR "nursing home"[tiab] OR "hip fracture"[tiab])'
        ),
    },
    {
        "name": "Neurological",
        "query": (
            '("physical therapy"[tiab] OR physiotherapy[tiab] OR rehabilitation[tiab]) '
            'AND (stroke[tiab] OR parkinson*[tiab] OR multiple sclerosis[tiab] OR '
            'spinal cord[tiab] OR concussion[tiab] OR vestibular[tiab] OR balance[tiab] OR gait[tiab])'
        ),
    },
    {
        "name": "Sports",
        "query": (
            '("physical therapy"[tiab] OR physiotherapy[tiab] OR rehabilitation[tiab]) '
            'AND ("return to sport"[tiab] OR athlete*[tiab] OR sports[tiab] OR ACL[tiab] OR '
            '"anterior cruciate"[tiab] OR hamstring[tiab] OR groin[tiab] OR ankle[tiab])'
        ),
    },
]

def http_get(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "BSTL-Literature-Updater/1.0"})
    with urllib.request.urlopen(req, timeout=40) as resp:
        return resp.read().decode("utf-8", errors="replace")

def build_params(params: dict) -> str:
    if NCBI_API_KEY:
        params["api_key"] = NCBI_API_KEY
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
    import json
    data = json.loads(http_get(url))
    return data.get("esearchresult", {}).get("idlist", [])

def esummary(pmid: str) -> dict:
    params = {
        "db": "pubmed",
        "id": pmid,
        "retmode": "json",
    }
    url = f"{EUTILS}/esummary.fcgi?{build_params(params)}"
    import json
    data = json.loads(http_get(url))
    result = data.get("result", {})
    # result includes 'uids' + keyed item
    uids = result.get("uids", [])
    if not uids:
        return {}
    return result.get(uids[0], {})

def efetch_abstract(pmid: str) -> str:
    params = {
        "db": "pubmed",
        "id": pmid,
        "retmode": "xml",
    }
    url = f"{EUTILS}/efetch.fcgi?{build_params(params)}"
    xml_text = http_get(url)

    # Parse XML and extract abstract text
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return ""

    # AbstractText can appear multiple times
    abstract_parts = []
    for abst in root.findall(".//Abstract/AbstractText"):
        # Some have "Label" attribute
        label = abst.attrib.get("Label", "").strip()
        text = "".join(abst.itertext()).strip()
        if not text:
            continue
        if label:
            abstract_parts.append(f"{label}: {text}")
        else:
            abstract_parts.append(text)

    return " ".join(abstract_parts).strip()

def pubmed_link(pmid: str) -> str:
    return f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/"

def safe(s: str) -> str:
    return html.escape(s or "")

def extract_stats(abstract: str) -> str:
    """
    Pulls numeric snippets (n=, %, p=, CI, OR/RR/HR, mean±sd) when present.
    Not perfect, but useful and honest.
    """
    if not abstract:
        return ""

    patterns = [
        r"\bn\s*=\s*\d+\b",
        r"\b\d+(\.\d+)?\s*%\b",
        r"\bp\s*[<=>]\s*0\.\d+\b",
        r"\bCI\s*[: ]\s*\(?\d+(\.\d+)?\s*[-–]\s*\d+(\.\d+)?\)?",
        r"\b(OR|RR|HR)\s*[:=]?\s*\d+(\.\d+)?",
        r"\b\d+(\.\d+)?\s*(weeks|week|months|month|days|day)\b",
    ]
    hits = []
    for pat in patterns:
        hits.extend(re.findall(pat, abstract, flags=re.IGNORECASE))
    # re.findall can return tuples if pattern has groups; normalize
    cleaned = []
    for h in hits:
        if isinstance(h, tuple):
            # take first non-empty string from tuple
            for part in h:
                if part and isinstance(part, str):
                    cleaned.append(part)
                    break
        else:
            cleaned.append(h)

    # Remove duplicates while preserving order
    seen = set()
    out = []
    for x in cleaned:
        x = str(x).strip()
        if not x:
            continue
        if x.lower() in seen:
            continue
        seen.add(x.lower())
        out.append(x)

    # Don’t spam: limit
    if not out:
        return ""
    return ", ".join(out[:10])

def two_paragraph_summary(abstract: str) -> tuple[str, str]:
    """
    Builds two short paragraphs from the abstract.
    - Paragraph 1: problem/objective/methods-ish
    - Paragraph 2: results/conclusion-ish + stats excerpt if available
    """
    if not abstract:
        return ("Summary unavailable (no abstract provided in PubMed record).",
                "Consider reviewing the full text for methods, results, and clinical applicability.")

    # Split into sentences (simple)
    sents = re.split(r"(?<=[.!?])\s+", abstract.strip())
    sents = [s.strip() for s in sents if s.strip()]

    # Heuristics: pick early sentences for p1
    p1_sents = sents[:3] if len(sents) >= 3 else sents[:1]

    # p2 tries to capture later results/conclusion
    tail = sents[3:] if len(sents) > 3 else []
    p2_sents = []
    for s in tail:
        if re.search(r"\b(result|results|conclusion|conclude|found|significant|improved|difference|effect)\b", s, re.I):
            p2_sents.append(s)
        if len(p2_sents) >= 3:
            break
    if not p2_sents:
        # fallback to last 2 sentences
        p2_sents = sents[-2:] if len(sents) >= 2 else sents

    stats = extract_stats(abstract)
    if stats:
        p2_sents.append(f"Reported stats (from abstract): {stats}.")

    p1 = " ".join(p1_sents)
    p2 = " ".join(p2_sents)
    return (p1, p2)

def build_section_card(section_name: str, pmid: str, meta: dict, abstract: str) -> str:
    title = safe((meta.get("title") or "").rstrip("."))
    journal = safe(meta.get("fulljournalname") or meta.get("source") or "Journal")
    pubdate = safe(meta.get("pubdate") or "Date not listed")
    link = pubmed_link(pmid)

    p1, p2 = two_paragraph_summary(abstract)

    return f"""
    <div class="card">
      <h2>{safe(section_name)}</h2>
      <p><strong><a href="{link}" target="_blank" rel="noopener noreferrer">{title}</a></strong></p>
      <p class="small">{journal} • {pubdate} • PMID: {safe(pmid)}</p>
      <p>{safe(p1)}</p>
      <p>{safe(p2)}</p>
    </div>
    """.strip()

def inject_into_literature(new_html: str) -> bool:
    with open(LITERATURE_FILE, "r", encoding="utf-8") as f:
        content = f.read()

    pattern = r"<!-- AUTO-LITERATURE:START -->(.*?)<!-- AUTO-LITERATURE:END -->"
    if not re.search(pattern, content, flags=re.DOTALL):
        raise RuntimeError("Markers not found in literature.html")

    replacement = f"<!-- AUTO-LITERATURE:START -->\n{new_html}\n<!-- AUTO-LITERATURE:END -->"
    updated = re.sub(pattern, replacement, content, flags=re.DOTALL)
    changed = (updated != content)

    if changed:
        with open(LITERATURE_FILE, "w", encoding="utf-8") as f:
            f.write(updated)

    return changed

def main():
    now = datetime.now(timezone.utc)
    start = (now - timedelta(days=DAYS_BACK)).strftime("%Y/%m/%d")
    end = now.strftime("%Y/%m/%d")

    header = f'<p class="small"><strong>Auto-updated:</strong> {now.strftime("%b %d, %Y")} (UTC) • Source: PubMed • Window: past 2 years</p>'
    cards = [header, '<div class="grid">']

    for sec in SECTIONS:
        name = sec["name"]
        term = sec["query"]

        ids = esearch(term=term, mindate=start, maxdate=end, retmax=RETMAX)
        time.sleep(0.35)

        if not ids:
            cards.append(f"""
            <div class="card">
              <h2>{safe(name)}</h2>
              <p><em>No recent results found in the past 2 years for this topic query.</em></p>
            </div>
            """.strip())
            continue

        pmid = ids[0]
        meta = esummary(pmid)
        time.sleep(0.35)

        abstract = efetch_abstract(pmid)
        time.sleep(0.35)

        cards.append(build_section_card(name, pmid, meta, abstract))

    cards.append("</div>")
    new_block = "\n".join(cards)

    changed = inject_into_literature(new_block)
    print("Updated:", changed)

if __name__ == "__main__":
    main()
