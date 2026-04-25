import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time

BASE_URL = "https://greatlakesrex.wordpress.com/alphabetical-shipwreck-list/"

# WordPress uses a combined page for X, Y, Z
PAGES = list("abcdefghijklmnopqrstuvwx") 

field_labels = {
    "Other names": "other_names",
    "Official no.": "official_no",
    "Type at loss": "type_at_loss",
    "Build info": "build_info",
    "Specs": "specs",
    "Date of loss": "date_of_loss",
    "Place of loss": "place_of_loss",
    "Lake": "lake",
    "Type of loss": "type_of_loss",
    "Loss of life": "loss_of_life",
    "Carrying": "carrying",
    "Detail": "detail",
    "Sources": "sources",
}

extra_columns = ["media_notes", "source_page"]

COLUMN_ORDER = [
    "name",
    "other_names",
    "official_no",
    "type_at_loss",
    "build_info",
    "specs",
    "date_of_loss",
    "place_of_loss",
    "lake",
    "type_of_loss",
    "loss_of_life",
    "carrying",
    "detail",
    "sources",
    "media_notes",
    "source_page",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

def clean_line(line: str) -> str:
    line = line.replace("\xa0", " ")
    line = re.sub(r"\s+", " ", line).strip()
    return line

def normalize_label_line(line: str) -> str:
    # handles ".Sources :" and similar
    return re.sub(r"^[\.\*]+", "", line).strip()

def is_cross_reference(name_line: str) -> bool:
    lowered = name_line.lower()
    return (
        re.search(r"\bsee\b", lowered) is not None
        or "entry removed" in lowered
        or "entry eliminated" in lowered
    )

def is_header_or_footer_junk(line: str) -> bool:
    junk_patterns = [
        r"^Great Lakes Shipwreck Files$",
        r"^Info and data on more than 5,000 shipwreck losses on the Great Lakes$",
        r"^[A-Z]$",
        r"^X,Y,Z$",
        r"^[A-Z] – Great Lakes Shipwreck Files",
        r"^\d+ of \d+ ",
        r"^https?://",
        r"^$",
        r"^Blog at WordPress\.com",
        r"^Do Not Sell or Share My Personal Information$",
    ]
    return any(re.search(p, line) for p in junk_patterns)

def match_field(line: str):
    normalized = normalize_label_line(line)
    for label, key in field_labels.items():
        if normalized.startswith(label):
            parts = normalized.split(":", 1)
            value = parts[1].strip() if len(parts) > 1 else ""
            return key, value
    return None

def split_blocks(lines):
    blocks = []
    current = []

    for line in lines:
        if "≈≈≈" in line:
            if current:
                blocks.append(current)
                current = []
        else:
            current.append(line)

    if current:
        blocks.append(current)

    return blocks

def clean_detail_and_sources(detail: str, sources: str):
    detail = detail or ""
    sources = sources or ""

    # move embedded Sources out of detail
    m = re.search(r"\.?\s*Sources\s*:\s*(.*)$", detail)
    if m:
        embedded_sources = m.group(1).strip()
        detail = re.sub(r"\.?\s*Sources\s*:\s*.*$", "", detail).strip()
        if not sources:
            sources = embedded_sources
        elif embedded_sources not in sources:
            sources = (sources + " " + embedded_sources).strip()

    return detail, sources

def parse_page(page_slug: str):
    url = f"{BASE_URL}{page_slug}/"
    print(f"Scraping {url}")

    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    content = soup.find("main") or soup.find("article") or soup
    text = content.get_text("\n")

    lines = [clean_line(line) for line in text.splitlines()]
    lines = [line for line in lines if line]

    blocks = split_blocks(lines)
    page_records = []

    for block in blocks:
        block = [line for line in block if not is_header_or_footer_junk(line)]
        if not block:
            continue

        if not any(normalize_label_line(line).startswith("Other names") for line in block):
            continue

        record = {"name": None}
        for v in field_labels.values():
            record[v] = None
        for col in extra_columns:
            record[col] = None

        current_field = None

        for raw_line in block:
            line = raw_line.strip()
            if not line:
                continue

            normalized = normalize_label_line(line)

            # keep photo/image lines separate
            if re.match(r"^(Photo|Image)", normalized, flags=re.IGNORECASE):
                if record["media_notes"]:
                    record["media_notes"] += " " + normalized
                else:
                    record["media_notes"] = normalized
                continue

            # field start?
            field_match = match_field(line)
            if field_match:
                key, value = field_match
                record[key] = value if value else ""
                current_field = key
                continue

            # ship name?
            if record["name"] is None:
                possible_name = line.lstrip(".").strip()

                if is_header_or_footer_junk(possible_name):
                    continue

                record["name"] = possible_name
                current_field = None
                continue

            # continuation line
            if current_field:
                existing = record[current_field] or ""
                record[current_field] = (existing + " " + line).strip()

        if not record["name"]:
            continue
        if is_cross_reference(record["name"]):
            continue

        populated = sum(
            1 for k, v in record.items()
            if k not in {"name", "media_notes", "source_page"} and v not in (None, "")
        )
        if populated < 5:
            continue

        record["detail"], record["sources"] = clean_detail_and_sources(
            record["detail"], record["sources"]
        )
        record["source_page"] = page_slug

        page_records.append(record)

    return page_records

# -------------------------
# Run full scrape
# -------------------------
all_records = []

for page in PAGES:
    try:
        page_records = parse_page(page)
        print(f"  -> Parsed {len(page_records)} records from {page}")
        all_records.extend(page_records)
        time.sleep(1)  # be polite
    except Exception as e:
        print(f"  !! Error on page {page}: {e}")

df = pd.DataFrame(all_records)

# fill blanks / trim
for col in df.columns:
    if df[col].dtype == object:
        df[col] = df[col].fillna("").str.strip()

# force column order
df = df[[col for col in COLUMN_ORDER if col in df.columns]]

print("\nTotal rows parsed:", len(df))
print(df.head(10).to_string())

df.to_csv("GreatLakesShips_Complete.csv", index=False)