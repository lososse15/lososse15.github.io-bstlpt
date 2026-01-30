import os
import re
import time
import html
import json
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone

# -----------------------------
# Settings
# -----------------------------
LITERATURE_FILE = "literature.html"
EUTILS = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"

NCBI_EMAIL = os.environ.get("NCBI_EMAIL", "example@example.com")
NCBI_API_KEY = os.environ.get("NCBI_API_KEY")  # optional but recommended

# Past 2 years window
DAYS_BACK = 365 * 2

# Pull multiple candidates so we can pick the best match
RETMAX = 40

# Limit how many candidates we score deeply per section (keeps it fast)
SCORE_TOP_N = 18

# Gentle delay (set to 0.00 if you add NCBI_API_KEY)
SLEEP = float(os.environ.get("SLEEP", "0.05"))

# -----------------------------
# Category setup
# Notes:
# - We treat your “example topics” as anchors, but still allow other relevant topics.
# - We bias toward journals you want by scoring journals higher.
# -----------------------------
SECTIONS = [
    {
        "name": "Orthopedics",
        "topic_query": (
            '("physical therapy"[tiab] OR physiotherapy[tiab] OR rehabilitation[tiab] OR "exercise therapy"[tiab]) '
            'AND (orthopedic*[tiab] OR musculoskeletal[tiab] OR "low back pain"[tiab] OR lumbar[tiab] OR '
            'shoulder[tiab] OR "rotator cuff"[tiab] OR knee[tiab] OR hip[tiab] OR osteoarthritis[tiab] OR '
            '"post-operative"[tiab] OR postoperative[tiab] OR post-op[tiab] OR "total knee"[tiab] OR "total hip"[tiab]) '
            'NOT (thrombectomy[tiab] OR endovascular[tiab] OR catheter[tiab] OR hospice[tiab] OR audiology[tiab] OR hearing[tiab])'
        ),
        "preferred_journals": [
            "J Orthop Sports Phys Ther",  # JOSPT (PubMed abbreviation)
            "JOSPT",                      # sometimes appears
            "Phys Ther",                  # PTJ / Physical Therapy
            "JOSPT Open",
        ],
        "must_terms": ["physical therapy", "physiotherapy", "rehabilitation", "exercise"],
        "boost_terms": [
            "outpatient", "manual therapy", "exercise therapy",
            "low back pain", "lumbar",
            "rotator cuff", "shoulder",
            "osteoarthritis", "knee", "hip",
            "postoperative", "post-op", "total knee", "total hip"
        ],
        "ban_terms": ["thrombectomy", "endovascular", "catheter", "audiology", "hearing", "hospice", "cost", "disposition"],
    },
    {
        "name": "Sports",
        "topic_query": (
            '("physical therapy"[tiab] OR physiotherapy[tiab] OR rehabilitation[tiab]) '
            'AND (sports[tiab] OR athlete*[tiab] OR "return to sport"[tiab] OR ACL[tiab] OR "anterior cruciate"[tiab] OR '
            'tendinopathy[tiab] OR "running injury"[tiab] OR running[tiab] OR "Achilles"[tiab] OR patellar[tiab] OR '
            'plyometric*[tiab] OR hop[tiab] OR "strength training"[tiab]) '
            'NOT (thrombectomy[tiab] OR endovascular[tiab] OR hospice[tiab] OR audiology[tiab] OR hearing[tiab] OR cost[tiab])'
        ),
        "preferred_journals": [
            "Am J Sports Med",             # AJSM (PubMed abbreviation)
            "J Orthop Sports Phys Ther",   # JOSPT
            "Br J Sports Med",             # good sports rehab source (extra, still professional)
        ],
        "must_terms": ["sports", "athlete", "return to sport", "acl", "tendinopathy", "running"],
        "boost_terms": [
            "rehabilitation", "reinjury", "plyometric", "hop test", "graft",
            "eccentric", "achilles", "patellar", "load management", "strength"
        ],
        "ban_terms": ["thrombectomy", "endovascular", "catheter", "hospice", "cost", "disposition", "stroke unit"],
    },
    {
        "name": "Geriatrics",
        "topic_query": (
            '("physical therapy"[tiab] OR physiotherapy[tiab] OR rehabilitation[tiab] OR exercise[tiab]) '
            'AND (geriatric*[tiab] OR "older adult"[tiab] OR older[tiab] OR frailty[tiab] OR '
            'falls[tiab] OR fall[tiab] OR balance[tiab] OR sarcopenia[tiab] OR "hip fracture"[tiab] OR osteoporosis[tiab]) '
            'NOT (audiology[tiab] OR hearing[tiab] OR cochlear[tiab] OR thrombectomy[tiab] OR endovascular[tiab])'
        ),
        "preferred_journals": [
            "Phys Ther",                   # PTJ / Physical Therapy
            "J Geriatr Phys Ther",         # geriatric PT journal
            "J Orthop Sports Phys Ther",   # sometimes has older adult rehab topics
        ],
        "must_terms": ["older", "falls", "balance", "frailty", "sarcopenia", "hip fracture"],
        "boost_terms": [
            "exercise", "strength", "multicomponent", "home-based", "community",
            "gait speed", "timed up and go", "tug", "sit-to-stand"
        ],
        "ban_terms": ["audiology", "hearing", "cochlear", "thrombectomy", "endovascular", "catheter", "hospice"],
    },
    {
        "name": "Neurological",
        "topic_query": (
            '("physical therapy"[tiab] OR physiotherapy[tiab] OR rehabilitation[tiab]) '
            'AND (stroke[tiab] OR poststroke[tiab] OR parkinson*[tiab] OR vestibular[tiab] OR dizziness[tiab] OR '
            'gait[tiab] OR walking[tiab] OR balance[tiab] OR "neurorehabilitation"[tiab]) '
            'NOT (thrombectomy[tiab] OR endovascular[tiab] OR catheter[tiab] OR hospice[tiab] OR cost[tiab])'
        ),
        "preferred_journals": [
            "J Neurol Phys Ther",          # JNPT (PubMed abbreviation)
            "Neurorehabil Neural Repair",  # extra good neuro rehab journal
            "Phys Ther",
        ],
        "must_terms": ["stroke", "parkinson", "vestibular", "gait", "walking", "balance"],
        "boost_terms": [
            "task-specific", "gait training", "treadmill", "cueing",
            "vestibular rehabilitation", "habituation", "vorr"
        ],
        "ban_terms": ["thrombectomy", "endovascular", "catheter", "hospice", "cost", "disposition"],
    },
]

# -----------------------------
# HTTP helpers
# -----------------------------
def http_get(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "BSTL-Literature-Updater/2.0"})
    with urllib.request.urlopen(req, timeout=40) as resp:
        return resp.read().decode("utf-8", errors="replace")

def build_params(params: dict) -> str:
    if NCBI_API_KEY:
        params["api_key"] = NCBI_API_KEY
    params["email"] = NCBI_EMAIL
    return urllib.parse.urlencode(params)

# -----------------------------
# PubMed calls
# -----------------------------
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

def esummary_batch(pmids: list[str]) -> dict[str, dict]:
    if not pmids:
        return {}
    params = {
        "db": "pubmed",
        "id": ",".join(pmids),
        "retmode": "json",
    }
    url = f"{EUTILS}/esummary.fcgi?{build_params(params)}"
    data = json.loads(http_get(url))
    result = data.get("result", {})
    out = {}
    for uid in result.get("uids", []):
        out[str(uid)] = result.get(uid, {})
    return out

def efetch_abstracts(pmids: list[str]) -> dict[str, str]:
    if not pmids:
        return {}
    params = {
        "db": "pubmed",
        "id": ",".join(pmids),
        "retmode": "xml",
    }
    url = f"{EUTILS}/efetch.fcgi?{build_params(params)}"
    xml_text = http_get(url)

    abstracts = {}
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return abstracts

    for article in root.findall(".//PubmedArticle"):
        pmid_el = article.find(".//MedlineCitation/PMID")
        if pmid_el is None or not (pmid_el.text or "").strip():
            continue
        pmid = pmid_el.text.strip()

        parts = []
        for abst in article.findall(".//Abstract/AbstractText"):
            label = (abst.attrib.get("Label", "") or "").strip()
            text = "".join(abst.itertext()).strip()
            if not text:
                continue
            parts.append(f"{label}: {text}" if label else text)

        abstracts[pmid] = " ".join(parts).strip()

    return abstracts

# -----------------------------
# Formatting + scoring
# -----------------------------
def pubmed_link(pmid: str) -> str:
    return f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/"

def safe(s: str) -> str:
    return html.escape(s or "")

def normalize_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def extract_stats(abstract: str) -> str:
    """
    Pull useful numeric snippets if present (only from abstract text).
    """
    if not abstract:
        return ""

    patterns = [
        r"\bn\s*=\s*\d+\b",
        r"\b\d+(\.\d+)?\s*%\b",
        r"\bp\s*[<=>]\s*0\.\d+\b",
        r"\b95%\s*CI\s*[: ]\s*\[?\(?\s*\d+(\.\d+)?\s*[-–]\s*\d+(\.\d+)?\s*\)?\]?\b",
        r"\b(OR|RR|HR)\s*[:=]?\s*\d+(\.\d+)?\b",
        r"\b\d+(\.\d+)?\s*(weeks|week|months|month|days|day)\b",
    ]

    hits = []
    for pat in patterns:
        for m in re.finditer(pat, abstract, flags=re.IGNORECASE):
            hits.append(m.group(0))

    # de-dup
    seen = set()
    out = []
    for x in hits:
        x = x.strip()
        if not x:
            continue
        key = x.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(x)

    return ", ".join(out[:10])

def score_relevance(blob: str, sec: dict, journal: str) -> int:
    t = (blob or "").lower()
    j = (journal or "").lower()

    # hard bans
    for w in sec["ban_terms"]:
        if w.lower() in t:
            return -999

    score = 0

    # prefer target journals
    for pj in sec["preferred_journals"]:
        if pj.lower() in j:
            score += 25

    # must terms (strong signals)
    for w in sec["must_terms"]:
        if w.lower() in t:
            score += 8

    # topic boosts
    for w in sec["boost_terms"]:
        if w.lower() in t:
            score += 3

    # modest boost if looks like clinical rehab paper
    if any(k in t for k in ["randomized", "trial", "systematic review", "meta-analysis", "guideline", "cohort"]):
        score += 4

    return score

def structured_summary(abstract: str) -> dict:
    """
    Background / Results / Conclusion / How to apply this (PT practice).
    Uses labeled abstracts when present; otherwise uses heuristics.
    """
    if not abstract:
        return {
            "background": "No abstract was available in the PubMed record for this article.",
            "results": "Key outcomes were not available from the abstract.",
            "conclusion": "Conclusion was not available from the abstract.",
            "apply": (
                "If relevant to your setting and patient population, review the full text when possible and "
                "translate the intervention into a measurable plan (dose, frequency, progression) while tracking outcomes."
            ),
        }

    txt = abstract.strip()

    def grab(label: str) -> str:
        # matches LABEL: ... until next ALLCAPS LABEL: or end
        m = re.search(
            rf"{label}\s*:\s*(.*?)(?=\n?[A-Z][A-Z \-]{{2,}}\s*:|$)",
            txt,
            flags=re.IGNORECASE | re.DOTALL
        )
        return normalize_space(m.group(1)) if m else ""

    background = grab("BACKGROUND") or grab("OBJECTIVE") or grab("PURPOSE")
    results = grab("RESULTS")
    conclusion = grab("CONCLUSION") or grab("CONCLUSIONS")

    # fallback if not labeled
    if not (background and results and conclusion):
        sents = re.split(r"(?<=[.!?])\s+", normalize_space(txt))
        sents = [s for s in sents if s]
        if not background:
            background = " ".join(sents[:2]) if len(sents) >= 2 else (sents[0] if sents else normalize_space(txt))
        if not results:
            res_like = [s for s in sents if re.search(r"\b(result|significant|improv|effect|difference|odds|risk|CI|p\s*[<=>])\b", s, re.I)]
            results = " ".join(res_like[:3]) if res_like else (" ".join(sents[2:5]) if len(sents) > 3 else normalize_space(txt))
        if not conclusion:
            conclusion = sents[-1] if sents else normalize_space(txt)

    stats = extract_stats(txt)
    if stats:
        results = normalize_space(results) + f" Key stats reported in the abstract: {stats}."

    apply = (
        "How to apply this (Physical Therapy): Confirm the population and setting match your caseload. "
        "If applicable, implement the main intervention principles (dose, intensity, frequency, progression) "
        "and track response with objective outcomes (pain scale, PSFS/ODI/LEFS/QuickDASH as appropriate, strength/ROM, "
        "gait speed, balance measures). Educate on expectations, monitor tolerance and safety, and individualize based on "
        "goals, comorbidities, and baseline function."
    )

    return {
        "background": background,
        "results": results,
        "conclusion": conclusion,
        "apply": apply,
    }

def build_section_card(section_name: str, pmid: str, meta: dict, abstract: str) -> str:
    title = safe((meta.get("title") or "").rstrip("."))
    journal = safe(meta.get("source") or meta.get("fulljournalname") or "Journal")
    pubdate = safe(meta.get("pubdate") or "Date not listed")
    link = pubmed_link(pmid)

    summ = structured_summary(abstract)

    return f"""
    <div class="card">
      <h2>{safe(section_name)}</h2>
      <p><strong><a href="{link}" target="_blank" rel="noopener noreferrer">{title}</a></strong></p>
      <p class="small">{journal} • {pubdate} • PMID: {safe(pmid)}</p>

      <p><strong>Background:</strong> {safe(summ["background"])}</p>
      <p><strong>Results:</strong> {safe(summ["results"])}</p>
      <p><strong>Conclusion:</strong> {safe(summ["conclusion"])}</p>
      <p><strong>How to apply this:</strong> {safe(summ["apply"])}</p>
    </div>
    """.strip()

def inject_into_literature(new_html: str) -> bool:
    with open(LITERATURE_FILE, "r", encoding="utf-8") as f:
        content = f.read()

    pattern = r"<!-- AUTO-LITERATURE:START -->(.*?)<!-- AUTO-LITERATURE:END -->"
    if not re.search(pattern, content, flags=re.DOTALL):
        raise RuntimeError("Markers not found in literature.html (AUTO-LITERATURE:START/END).")

    replacement = f"<!-- AUTO-LITERATURE:START -->\n{new_html}\n<!-- AUTO-LITERATURE:END -->"
    updated = re.sub(pattern, replacement, content, flags=re.DOTALL)
    changed = (updated != content)

    if changed:
        with open(LITERATURE_FILE, "w", encoding="utf-8") as f:
            f.write(updated)

    return changed

# -----------------------------
# Main
# -----------------------------
def main():
    now = datetime.now(timezone.utc)
    start = (now - timedelta(days=DAYS_BACK)).strftime("%Y/%m/%d")
    end = now.strftime("%Y/%m/%d")

    header = (
        f'<p class="small"><strong>Auto-updated:</strong> {now.strftime("%b %d, %Y")} (UTC) • '
        f'Source: PubMed-indexed journals (incl. JOSPT/JNPT/AJSM) • Window: past 2 years</p>'
    )

    cards = [header, '<div class="grid">']

    for sec in SECTIONS:
        name = sec["name"]

        ids = esearch(sec["topic_query"], mindate=start, maxdate=end, retmax=RETMAX)
        if SLEEP:
            time.sleep(SLEEP)

        if not ids:
            cards.append(f"""
            <div class="card">
              <h2>{safe(name)}</h2>
              <p><em>No recent results found (past 2 years) for this category query.</em></p>
            </div>
            """.strip())
            continue

        # Score only top N candidates to keep it fast
        candidate_pmids = ids[:SCORE_TOP_N]

        # Batch fetch metadata + abstracts
        meta_map = esummary_batch(candidate_pmids)
        if SLEEP:
            time.sleep(SLEEP)

        abstract_map = efetch_abstracts(candidate_pmids)
        if SLEEP:
            time.sleep(SLEEP)

        best = {"pmid": None, "meta": None, "abstract": "", "score": -999}

        for pmid in candidate_pmids:
            meta = meta_map.get(str(pmid), {})
            abstract = abstract_map.get(str(pmid), "")

            title = meta.get("title", "")
            journal = meta.get("source", "") or meta.get("fulljournalname", "")
            blob = f"{title} {journal} {abstract}"

            s = score_relevance(blob, sec, journal)

            if s > best["score"]:
                best = {"pmid": str(pmid), "meta": meta, "abstract": abstract, "score": s}

        # Require a minimum relevance score so categories don't drift
        if not best["pmid"] or best["score"] < 18:
            cards.append(f"""
            <div class="card">
              <h2>{safe(name)}</h2>
              <p><em>No strong match found this month within the past 2 years. (Try broadening keywords.)</em></p>
            </div>
            """.strip())
        else:
            cards.append(build_section_card(name, best["pmid"], best["meta"], best["abstract"]))

    cards.append("</div>")

    changed = inject_into_literature("\n".join(cards))
    print("Updated:", changed)

if __name__ == "__main__":
    main()
