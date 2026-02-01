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
RETMAX = 80
SCORE_TOP_N = 30

# Delay to be nice to PubMed (can be lower if you have NCBI_API_KEY)
SLEEP = float(os.environ.get("SLEEP", "0.05"))

# Manual APTA resource link (no scraping)
APTA_CPG_HUB_URL = "https://www.apta.org/patient-care/evidence-based-practice-resources/cpgs"

# PT / Rehab indexing filter (MeSH/Major Topic)
PT_FILTER = (
    '("Physical Therapy Modalities"[MeSH Terms] OR "Physical Therapists"[MeSH Terms] OR '
    '"Rehabilitation"[MeSH Major Topic] OR "Exercise Therapy"[MeSH Terms])'
)

# Strong PT signals (title/abstract)
PT_REQUIRED_TERMS = [
    "physical therapy", "physiotherapy", "physical therapist",
    "rehabilitation", "exercise therapy", "therapeutic exercise",
    "gait training", "balance training", "strength training"
]

# Red flags (reject)
NON_PT_RED_FLAGS = [
    "thrombectomy", "catheter", "endovascular", "stent",
    "hospice", "cost analysis", "disposition",
    "audiology", "hearing", "cochlear",
    "radiology", "chemotherapy", "dialysis"
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
            'AND (("low back pain"[tiab] OR lumbar[tiab] OR spine[ti]()
