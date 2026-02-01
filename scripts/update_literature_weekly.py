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
LITERATURE_FILE = "literature.html"           # repo root
HISTORY_FILE = "literature_history.json"      # repo root
MAX_HISTORY = 120                             # per category

EUTILS = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
NCBI_EMAIL = os.environ.get("NCBI_EMAIL", "example@example.com")
NCBI_API_KEY = os.environ.get("NCBI_API_KEY")  # optional but recommended

# Past 2 years window
DAYS_BACK = 365 * 2

# Candidate pool and scoring
RETMAX = 60
SCORE_TOP_N = 25

# Delay to be nice to PubMed (set to 0 if you have NCBI_API_KEY)
SLEEP = float(os.environ.get("SLEEP", "0.05"))

# Manual APTA resource link (no scraping)
APTA_CPG_HUB_URL = "https://www.apta.org/patient-care/evidence-based-practice-resources/cpgs"

# PT / Rehab indexing filter (MeSH/Major Topic)
PT_FILTER = (
    '("Physical Therapy Modalities"[MeSH Terms] OR "Physical Therapists"[MeSH Terms] OR '
    '"Rehabilitation"[MeSH Major Topic] OR "Exercise Therapy"[MeSH Terms])'
)

# Enforce PT relevance even if something slips through
PT_REQUIRED_TERMS = [
    "physical therapy", "physiotherapy", "physical therapist",
    "rehabilitation", "exercise therapy", "therapeutic exercise",
    "gait training", "balance training"
]

NON_PT_RED_FLAGS = [
    "thrombectomy", "catheter", "endovascular", "stent",
    "hospice", "cost analysis", "disposition",
    "audiology", "hearing", "cochlear"
]

# -----------------------------
# Categories (PT-practice topics)
# -----------------------------
SECTIONS = [
    {
        "name": "Orthopedics",
        "topic_query": (
            f'({PT_FILTER}) AND ('
            '("physical therapy"[tiab] OR physiotherapy[tiab] OR rehabilitation[tiab] OR "exercise therapy"[tiab]) '
            'AND (("low back pain"[tiab] OR lumbar[tiab]) OR '
            '(shoulder[tiab] OR "rotator cuff"[tiab]) OR '
            '("knee osteoarthritis"[tiab] OR osteoarthritis[tiab] OR "hip osteoarthritis"[tiab]) OR '
            '(postoperative[tiab] OR "post-operative"[tiab] OR post-op[tiab] OR "total knee"[tiab] OR "total hip"[tiab])) '
            'NOT (thrombectomy[tiab] OR endovascular[tiab] OR catheter[tiab] OR audiology[tiab] OR hearing[tiab] OR hospice[tiab])'
            ')'
        ),
        "preferred_journals": ["J Orthop Sports Phys Ther", "Phys Ther", "Arch Phys Med Rehabil"],
        "must_terms": ["physical therapy", "physiotherapy", "rehabilitation", "exercise"],
        "boost_terms": [
            "low back pain", "lumbar", "manual therapy",
            "shoulder", "rotator cuff",
            "knee osteoarthritis", "osteoarthritis", "hip",
            "postoperative", "post-op", "total knee", "total hip",
            "outpatient", "exercise therapy"
        ],
        "ban_terms": ["thrombectomy", "endovascular", "catheter", "audiology", "hearing", "hospice", "cost", "disposition"],
    },
    {
        "name": "Sports",
        "topic_query": (
            f'({PT_FILTER}) AND ('
            '("physical therapy"[tiab] OR physiotherapy[tiab] OR rehabilitation[tiab]) '
            'AND (ACL[tiab] OR "anterior cruciate"[tiab] OR tendinopathy[tiab] OR achilles[tiab] OR patellar[tiab] OR '
            'running[tiab] OR "running injury"[tiab] OR "return to sport"[tiab] OR athlete*[tiab]) '
            'NOT (thrombectomy[tiab] OR endovascular[tiab] OR catheter[tiab] OR hospice[tiab] OR audiology[tiab] OR hearing[tiab])'
            ')'
        ),
        "preferred_journals": ["Am J Sports Med", "J Orthop Sports Phys Ther", "Br J Sports Med", "Sports Health"],
        "must_terms": ["rehabilitation", "return to sport", "athlete", "acl", "tendinopathy", "running"],
        "boost_terms": [
            "reinjury", "plyometric", "hop test", "landing",
            "eccentric", "achilles", "patellar",
            "load management", "strength", "performance"
        ],
        "ban_terms": ["thrombectomy", "endovascular", "catheter", "hospice", "cost", "disposition", "audiology", "hearing"],
    },
    {
        "name": "Geriatrics",
        "topic_query": (
            f'({PT_FILTER}) AND ('
            '("physical therapy"[tiab] OR physiotherapy[tiab] OR rehabilitation[tiab] OR exercise[tiab]) '
            'AND ("older adult"[tiab] OR older[tiab] OR geriatric*[tiab] OR frailty[tiab] OR falls[tiab] OR fall[tiab] OR '
            'balance[tiab] OR sarcopenia[tiab] OR "hip fracture"[tiab] OR osteoporosis[tiab]) '
            'NOT (audiology[tiab] OR hearing[tiab] OR cochlear[tiab] OR thrombectomy[tiab] OR endovascular[tiab])'
            ')'
        ),
        "preferred_journals": ["J Geriatr Phys Ther", "Phys Ther", "Arch Phys Med Rehabil"],
        "must_terms": ["older", "falls", "balance", "frailty", "sarcopenia", "hip fracture"],
        "boost_terms": [
            "exercise", "strength", "multicomponent", "home-based",
            "gait speed", "timed up and go", "tug", "sit-to-stand"
        ],
        "ban_terms": ["audiology", "hearing", "cochlear", "thrombectomy", "endovascular", "catheter", "hospice", "cost"],
    },
    {
        "name": "Neurological",
        "topic_query": (
            f'({PT_FILTER}) AND ('
            '("physical therapy"[tiab] OR physiotherapy[tiab] OR rehabilitation[tiab]) '
            'AND (stroke[tiab] OR poststroke[tiab] OR parkinson*[tiab] OR vestibular[tiab] OR dizziness[tiab] OR '
            'gait[tiab] OR walking[tiab] OR balance[tiab] OR neurorehabilitation[tiab]) '
            'NOT (thrombectomy[tiab] OR endovascular[tiab] OR catheter[tiab] OR hospice[tiab] OR audiology[tiab] OR hearing[tiab])'
            ')'
        ),
        "preferred_journals": ["J Neurol Phys Ther", "Neurorehabil Neural Repair", "Phys Ther", "Arch Phys Med Rehabil"],
        "must_terms": ["stroke", "parkinson", "vestibular", "gait", "walking", "balance", "rehabilitation"],
        "boost_terms": [
            "task-specific", "gait training", "treadmill",
            "cueing", "vestibular rehabilitation", "habituation"
        ],
        "ban_terms": ["thrombectomy", "endovascular", "catheter", "hospice", "cost", "disposition", "audiology", "hearing"],
    },
]

# -----------------------------
# HTTP helpers
# -----------------------------
def http_get(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "BSTL-Literature-Updater/weekly/5.0"})
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
    params = {"db": "pubmed", "id": ",".join(pmids), "retmode": "json"}
    url = f"{EUTILS}/esummary.fcgi?{build_params(params)}"
    data = json.loads(http_get(url))
    result = data.get("result", {})
    out: dict[str, dict] = {}
    for uid in result.get("uids", []):
        out[str(uid)] = result.get(uid, {})
    return out

def efetch_abstracts(pmids: list[str]) -> dict[str, str]:
    if not pmids:
        return {}
    params = {"db": "pubmed", "id": ",".join(pmids), "retmode": "xml"}
    url = f"{EUTILS}/efetch.fcgi?{build_params(params)}"
    xml_text = http_get(url)

    abstracts: dict[str, str] = {}
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
# Helpers
# -----------------------------
def pubmed_link(pmid: str) -> str:
    return f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/"

def doi_link(doi: str) -> str:
    return f"https://doi.org/{doi}"

def pmc_link(pmcid: str) -> str:
    pmcid = pmcid.strip()
    if not pmcid.upper().startswith("PMC"):
        pmcid = "PMC" + pmcid
    return f"https://pmc.ncbi.nlm.nih.gov/articles/{pmcid}/"

def safe(s: str) -> str:
    return html.escape(s or "")

def normalize_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def extract_stats(text: str) -> str:
    if not text:
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
        for m in re.finditer(pat, text, flags=re.IGNORECASE):
            hits.append(m.group(0))
    seen = set()
    out = []
    for x in hits:
        x = x.strip()
        key = x.lower()
        if x and key not in seen:
            seen.add(key)
            out.append(x)
    return ", ".join(out[:10])

def extract_dosage(text: str) -> str:
    """
    Extracts exercise dosage/frequency/time details ONLY if present in the abstract text.
    Returns a short human-readable string or "" if nothing found.
    """
    if not text:
        return ""
    t = normalize_space(text)

    patterns = [
        # frequency
        r"\b\d+\s*(x|×)\s*(/|per)\s*week\b",
        r"\b\d+\s*times\s*(a|per)\s*week\b",
        r"\b\d+\s*sessions?\s*(/|per)\s*week\b",
        r"\b(once|twice)\s*(a|per)\s*week\b",

        # session length
        r"\b\d+\s*(to|-)\s*\d+\s*min(ute)?s?\b",
        r"\b\d+\s*min(ute)?s?\b",
        r"\b\d+\s*(to|-)\s*\d+\s*hours?\b",
        r"\b\d+\s*hours?\b",

        # program duration
        r"\b\d+\s*(to|-)\s*\d+\s*weeks?\b",
        r"\b\d+\s*weeks?\b",
        r"\b\d+\s*(to|-)\s*\d+\s*months?\b",
        r"\b\d+\s*months?\b",

        # sets/reps
        r"\b\d+\s*sets?\s*of\s*\d+\s*reps?\b",
        r"\b\d+\s*(sets?|set)\b",
        r"\b\d+\s*(reps?|repetitions?)\b",
        r"\b\d+\s*(to|-)\s*\d+\s*reps?\b",

        # intensity
        r"\b\d+\s*RM\b",
        r"\b\d+\s*%\s*1RM\b",
        r"\bRPE\s*\d+(\.\d+)?\b",
        r"\bBorg\s*\d+(\.\d+)?\b",
    ]

    hits = []
    for pat in patterns:
        for m in re.finditer(pat, t, flags=re.IGNORECASE):
            hits.append(m.group(0))

    seen = set()
    out = []
    for h in hits:
        key = h.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(h)

    return ", ".join(out[:10])

def get_article_id(meta: dict, idtype: str) -> str:
    for item in meta.get("articleids", []) or []:
        if (item.get("idtype") or "").lower() == idtype.lower():
            return (item.get("value") or "").strip()
    return ""

def score_relevance(blob: str, sec: dict, journal: str) -> int:
    t = (blob or "").lower()

    # Hard reject: clearly not PT-related
    for bad in NON_PT_RED_FLAGS:
        if bad in t:
            return -999

    # Require at least one PT signal
    if not any(term in t for term in PT_REQUIRED_TERMS):
        return -200

    j = (journal or "").lower()

    for w in sec["ban_terms"]:
        if w.lower() in t:
            return -999

    score = 0

    # Preferred journals boost
    for pj in sec["preferred_journals"]:
        if pj.lower() in j:
            score += 25

    # Must terms
    for w in sec["must_terms"]:
        if w.lower() in t:
            score += 8

    # Boost terms
    for w in sec["boost_terms"]:
        if w.lower() in t:
            score += 3

    # Study type boost
    if any(k in t for k in ["randomized", "trial", "systematic review", "meta-analysis", "guideline", "cohort"]):
        score += 4

    return score

def structured_summary(abstract: str, section_name: str = "") -> dict:
    """
    Output:
      - summary: readable abstract summary (not just copied sentences)
      - eli5: 2–3 sentences explaining it to a 5-year-old
      - apply: practical PT implementation guidance + dosage if stated in abstract
    """
    if not abstract:
        return {
            "summary": (
                "No abstract was available in the PubMed record for this article. "
                "Review the full text (if available) for methods, results, and clinical takeaways."
            ),
            "eli5": (
                "This paper is about helping people move and feel better. "
                "We’d need the full paper to know exactly what they found."
            ),
            "apply": (
                "If this topic matches your caseload, review the full text when possible. "
                "Then select appropriate outcome measures and apply the intervention principles "
                "(dose, frequency, progression) while monitoring tolerance and safety."
            ),
        }

    txt = normalize_space(abstract)
    stats = extract_stats(txt)
    dosage = extract_dosage(txt)

    # Sentence split
    sents = re.split(r"(?<=[.!?])\s+", txt)
    sents = [s.strip() for s in sents if s.strip()]

    # Build a summary in a "real summary" voice:
    # - context/purpose from first 1–2 sentences
    # - add key findings sentence(s) if present
    opener = " ".join(sents[:2]) if len(sents) >= 2 else (sents[0] if sents else txt)

    key_findings = ""
    findings_sents = [
        s for s in sents
        if re.search(r"\b(significant|improv|reduc|increase|difference|effect|associated|odds|risk|CI|p\s*[<=>])\b", s, re.I)
    ]
    if findings_sents:
        key_findings = " ".join(findings_sents[:2])
    elif len(sents) > 2:
        key_findings = sents[-1]

    summary = normalize_space(f"{opener} {key_findings}".strip())
    if stats:
        summary = normalize_space(summary + f" Key numbers reported in the abstract include: {stats}.")

    # ELI5 (2–3 sentences)
    eli5 = (
        "They wanted to learn what kind of practice helps people move better. "
        "They tried a plan and checked if it helped. "
        "The results can help therapists choose better exercises for people."
    )

    # Practical application guidance (PT)
    apply_lines = [
        "Match the study population to your patient (diagnosis, age, stage of recovery, and goals).",
    ]
    if dosage:
        apply_lines.append(f"Study-reported dosage from the abstract: {dosage}.")
    apply_lines.extend([
        "Translate the main intervention into a measurable plan (frequency, intensity, time/sets/reps, and progression).",
        "Use objective measures to track change (pain + function + a performance test when appropriate).",
        "Educate on expectations and adjust based on symptom response, safety, and functional goals.",
    ])

    sec = (section_name or "").lower()

    if "orthoped" in sec:
        apply_lines.append(
            "For MSK care, use symptom-guided loading: progress volume/load while monitoring the 24-hour response and functional tolerance."
        )
        apply_lines.append(
            "Pair the approach with task practice relevant to goals (stairs, sit-to-stand, lifting, reaching) and reassess weekly."
        )

    if "sports" in sec:
        apply_lines.append(
            "For sport rehab, integrate this into criteria-based progression (strength symmetry, hop/landing quality, workload tolerance) before return-to-sport."
        )
        apply_lines.append(
            "Use graded exposure to sport-specific drills and monitor training load to reduce flare-ups and reinjury risk."
        )

    if "geri" in sec:
        apply_lines.append(
            "For older adults, prioritize fall-risk reduction: progressive strengthening + balance training (static → dynamic → dual-task) with home safety education."
        )
        apply_lines.append(
            "Tie outcomes to independence (gait speed, TUG, sit-to-stand, balance measures) and build confidence with safe repetitions."
        )

    if "neuro" in sec:
        apply_lines.append(
            "For neuro rehab, emphasize task-specific, high-repetition practice (walking, transfers, balance tasks) with appropriate cueing and safety setup."
        )
        apply_lines.append(
            "Progress difficulty by changing speed, environment, and cognitive load while monitoring fatigue and safety."
        )

    apply = " ".join(apply_lines)

    return {"summary": summary, "eli5": eli5, "apply": apply}

# -----------------------------
# HTML building blocks
# -----------------------------
def build_access_buttons(pmid: str, meta: dict) -> str:
    doi = get_article_id(meta, "doi")
    pmcid = get_article_id(meta, "pmcid")

    btns = [
        f'<a class="pill" href="{pubmed_link(pmid)}" target="_blank" rel="noopener noreferrer">PubMed</a>'
    ]
    if doi:
        btns.append(f'<a class="pill" href="{doi_link(doi)}" target="_blank" rel="noopener noreferrer">DOI</a>')
    if pmcid:
        btns.append(f'<a class="pill" href="{pmc_link(pmcid)}" target="_blank" rel="noopener noreferrer">PMC (Full text)</a>')

    return '<div class="pills">' + "\n".join(btns) + "</div>"

def build_previous_featured_list(prev_pmids: list[str], prev_meta_map: dict[str, dict]) -> str:
    if not prev_pmids:
        return '<p class="small"><em>No previous featured articles yet.</em></p>'

    items = []
    for pmid in prev_pmids:
        meta = prev_meta_map.get(str(pmid), {})
        title = (meta.get("title") or "").rstrip(".") or f"PMID {pmid}"
        items.append(
            f'<li><a href="{pubmed_link(pmid)}" target="_blank" rel="noopener noreferrer">{safe(title)}</a> '
            f'<span class="small">(PMID: {safe(str(pmid))})</span></li>'
        )
    return f'<ul class="list">{"".join(items)}</ul>'

def build_section_card(
    section_name: str,
    pmid: str,
    meta: dict,
    abstract: str,
    prev_pmids: list[str],
    prev_meta_map: dict[str, dict]
) -> str:
    title = safe((meta.get("title") or "").rstrip("."))
    journal = safe(meta.get("source") or meta.get("fulljournalname") or "Journal")
    pubdate = safe(meta.get("pubdate") or "Date not listed")

    summ = structured_summary(abstract, section_name=section_name)
    access = build_access_buttons(pmid, meta)
    prev_list = build_previous_featured_list(prev_pmids, prev_meta_map)

    return f"""
    <div class="card">
      <h2>{safe(section_name)}</h2>
      <p><strong>{title}</strong></p>
      <p class="small">{journal} • {pubdate} • PMID: {safe(pmid)}</p>

      <p><strong>Summary:</strong> {safe(summ["summary"])}</p>
      <p><strong>Explain like I’m 5:</strong> {safe(summ["eli5"])}</p>
      <p><strong>How to apply:</strong> {safe(summ["apply"])}</p>

      <p><strong>Access full article:</strong></p>
      {access}

      <p style="margin-top:14px;"><strong>Previously featured:</strong></p>
      {prev_list}
    </div>
    """.strip()

def build_apta_resources_card() -> str:
    return f"""
    <div class="card">
      <h2>APTA Resources</h2>
      <p><strong>Clinical Practice Guidelines (CPGs)</strong></p>
      <p class="small">
        Evidence-based practice resources from the American Physical Therapy Association (manual link; no scraping).
      </p>
      <div class="pills">
        <a class="pill" href="{APTA_CPG_HUB_URL}" target="_blank" rel="noopener noreferrer">APTA CPG Hub</a>
      </div>
      <p class="small">
        Tip: Use CPGs to support clinical decision-making and standardized outcome measures when appropriate.
      </p>
    </div>
    """.strip()

# -----------------------------
# History (no repeats)
# -----------------------------
def load_history() -> dict:
    if not os.path.exists(HISTORY_FILE):
        return {s["name"]: [] for s in SECTIONS}
    try:
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        for s in SECTIONS:
            data.setdefault(s["name"], [])
        for k in list(data.keys()):
            data[k] = [str(x) for x in (data.get(k) or [])]
        return data
    except Exception:
        return {s["name"]: [] for s in SECTIONS}

def save_history(hist: dict) -> None:
    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(hist, f, indent=2)

# -----------------------------
# HTML injection
# -----------------------------
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

    history = load_history()

    if not os.path.exists(HISTORY_FILE):
        init = {s["name"]: [] for s in SECTIONS}
        save_history(init)
        history = init

    chosen: dict[str, dict] = {}
    for sec in SECTIONS:
        name = sec["name"]

        ids = esearch(sec["topic_query"], mindate=start, maxdate=end, retmax=RETMAX)
        if SLEEP:
            time.sleep(SLEEP)

        if not ids:
            chosen[name] = {"pmid": "", "meta": {}, "abstract": "", "score": -999}
            continue

        used = set(history.get(name, []))
        filtered = [str(p) for p in ids if str(p) not in used]
        candidate_pmids = filtered[:SCORE_TOP_N] if filtered else [str(p) for p in ids[:SCORE_TOP_N]]

        meta_map = esummary_batch(candidate_pmids)
        if SLEEP:
            time.sleep(SLEEP)

        abstract_map = efetch_abstracts(candidate_pmids)
        if SLEEP:
            time.sleep(SLEEP)

        best = {"pmid": "", "meta": {}, "abstract": "", "score": -999}
        for pmid in candidate_pmids:
            meta = meta_map.get(str(pmid), {})
            abstract = abstract_map.get(str(pmid), "")
            title = meta.get("title", "")
            journal = meta.get("source", "") or meta.get("fulljournalname", "")
            blob = f"{title} {journal} {abstract}"
            s = score_relevance(blob, sec, journal)
            if s > best["score"]:
                best = {"pmid": str(pmid), "meta": meta, "abstract": abstract, "score": s}

        chosen[name] = best

        if best["pmid"] and best["score"] >= 18:
            history.setdefault(name, [])
            history[name].insert(0, best["pmid"])
            history[name] = history[name][:MAX_HISTORY]

    # Previous featured metas (titles)
    prev_pmids_all: list[str] = []
    prev_pmids_by_section: dict[str, list[str]] = {}

    for sec in SECTIONS:
        name = sec["name"]
        prev = history.get(name, [])[1:6]  # last 5 excluding newest
        prev_pmids_by_section[name] = prev
        prev_pmids_all.extend(prev)

    prev_unique = list(dict.fromkeys(prev_pmids_all))
    prev_meta_map = esummary_batch(prev_unique) if prev_unique else {}
    if prev_unique and SLEEP:
        time.sleep(SLEEP)

    header = (
        f'<p class="small"><strong>Auto-updated:</strong> {now.strftime("%b %d, %Y")} (UTC) • '
        f'Weekly articles: PT/rehab-focused (PubMed-indexed; incl. JOSPT/JNPT/AJSM) • Window: past 2 years • No repeats</p>'
    )

    cards = [header, '<div class="grid">']

    for sec in SECTIONS:
        name = sec["name"]
        best = chosen.get(name, {"pmid": "", "meta": {}, "abstract": "", "score": -999})

        if not best["pmid"] or best["score"] < 18:
            cards.append(f"""
            <div class="card">
              <h2>{safe(name)}</h2>
              <p><em>No strong PT-focused match found this week within the past 2 years.</em></p>
              <p class="small">This can happen when recent articles don’t match the PT/rehab filters or your topic constraints.</p>
            </div>
            """.strip())
        else:
            cards.append(
                build_section_card(
                    section_name=name,
                    pmid=best["pmid"],
                    meta=best["meta"],
                    abstract=best["abstract"],
                    prev_pmids=prev_pmids_by_section.get(name, []),
                    prev_meta_map=prev_meta_map
                )
            )

    cards.append(build_apta_resources_card())
    cards.append("</div>")

    changed = inject_into_literature("\n".join(cards))
    save_history(history)

    print("Updated:", changed)

if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("\n--- LITERATURE UPDATER FAILED ---")
        traceback.print_exc()
        raise
