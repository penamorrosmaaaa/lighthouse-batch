#!/usr/bin/env python3
import re
import io
import gzip
import json
import requests
import datetime
import gspread
import time
import subprocess
from oauth2client.service_account import ServiceAccountCredentials
from lxml import etree

# ----------------------------------------------------------------
# Setup Google Sheets API
# ----------------------------------------------------------------
SHEET_NAME = "SITEMAPS.XML"
WORKSHEET_NAME = "Sheet1"
CREDENTIALS_FILE = "credentials.json"

scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
client = gspread.authorize(creds)
sheet = client.open(SHEET_NAME).worksheet(WORKSHEET_NAME)

today = datetime.datetime.today().strftime("%Y-%m-%d")

# Load all used URLs from the sheet (to avoid duplicates)
used_urls = set()
for row in sheet.get_all_values():
    for cell in row:
        if cell.startswith("http"):
            used_urls.add(cell.strip())

# ----------------------------------------------------------------
# HTTP Helpers
# ----------------------------------------------------------------
def safe_request(url, retries=3, delay=2):
    for attempt in range(retries):
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                return resp
        except Exception:
            pass
        time.sleep(delay)
    return None

def fetch_xml_root(url):
    resp = safe_request(url)
    if not resp:
        return None
    data = resp.content
    try:
        data = gzip.GzipFile(fileobj=io.BytesIO(data)).read()
    except (OSError, gzip.BadGzipFile):
        pass
    return etree.fromstring(data)

# ----------------------------------------------------------------
# Generic Extractors
# ----------------------------------------------------------------
def extract_urls_from_xml(url, include=None, exclude=None):
    root = fetch_xml_root(url)
    if root is None:
        return []
    ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    urls = [el.text.strip() for el in root.xpath("//ns:url/ns:loc", namespaces=ns)]
    if include:
        urls = [u for u in urls if any(i in u for i in include)]
    if exclude:
        urls = [u for u in urls if not any(e in u for e in exclude)]
    return list(dict.fromkeys(urls))

def extract_urls_from_txt(txt_url, include=None, exclude=None):
    resp = safe_request(txt_url)
    if not resp:
        return []
    lines = resp.text.strip().splitlines()
    urls = []
    for line in lines:
        text = line.strip()
        lower = text.lower()
        if exclude and any(e in lower for e in exclude):
            continue
        if include and not any(i in lower for i in include):
            continue
        urls.append(text)
    return list(dict.fromkeys(urls))

def get_latest_sitemap(index_url):
    root = fetch_xml_root(index_url)
    if root is None:
        return None
    ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    sitemaps = []
    for sm in root.xpath("//ns:sitemap", namespaces=ns):
        loc = sm.find("ns:loc", namespaces=ns)
        mod = sm.find("ns:lastmod", namespaces=ns)
        if loc is not None and mod is not None:
            sitemaps.append((loc.text.strip(), mod.text.strip()))
    if not sitemaps:
        return None
    return sorted(sitemaps, key=lambda x: x[1], reverse=True)[0][0]

# ----------------------------------------------------------------
# Brand‐specific extractors
# ----------------------------------------------------------------
def get_milenio():
    root = fetch_xml_root("https://www.milenio.com/sitemap/google-news/sitemap-google-news-current-1.xml")
    if root is None:
        return [], []
    ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    nota, video = [], []
    for loc in root.xpath("//ns:url/ns:loc", namespaces=ns):
        url = loc.text.strip()
        is_video = url.endswith("-video") or "/video/" in url or url.split("/")[-1] == "video"
        (video if is_video else nota).append(url)
    return nota, video

def get_as():
    resp = safe_request("https://feeds.as.com/mrss-s/pages/as/site/as.com/section/opinion/portada/")
    text = resp.text if resp else ""
    nota = re.findall(r'https?://as\.com[^\s"<]+', text)
    nota = [u for u in nota if "/opinion/" in u and "video" not in u]
    resp2 = safe_request("https://feeds.as.com/mrss-s/list/as/site/as.com/video/")
    text2 = resp2.text if resp2 else ""
    video = re.findall(r'https?://as\.com[^\s"<]+', text2)
    video = [u for u in video if "video" in u]
    return nota, video

def get_terra():
    resp = safe_request("https://www.terra.com.mx/rss/un_foto.html")
    if not resp:
        return [], []
    urls = re.findall(r"https?://www\.terra\.com\.mx[^\s\"<]+", resp.text)
    nota = [u for u in urls if "/nacionales/" in u]
    video = [u for u in urls if "/entretenimiento/" in u]
    return nota, video

def get_nytimes():
    nota = extract_urls_from_xml("https://www.nytimes.com/sitemaps/new/news.xml.gz")
    vid_index = get_latest_sitemap("https://www.nytimes.com/sitemaps/new/video.xml.gz")
    video = extract_urls_from_xml(vid_index) if vid_index else []
    return nota, video

def get_heraldo():
    index = get_latest_sitemap("https://heraldodemexico.com.mx/sitemaps/")
    nota = extract_urls_from_txt(index, exclude=["video"]) if index else []
    video = extract_urls_from_txt(index, include=["video"]) if index else []
    return nota, video

def get_infobae():
    nota = extract_urls_from_xml("https://www.infobae.com/arc/outboundfeeds/news-sitemap2/", exclude=["video"])
    video = extract_urls_from_xml("https://www.infobae.com/arc/outboundfeeds/news-sitemap2/category/teleshow/")
    return nota, video

def get_universal():
    all_urls = extract_urls_from_xml("https://www.eluniversal.com.mx/arc/outboundfeeds/general/?outputType=xml")
    nota = [u for u in all_urls if not any(v in u for v in ["/video/", "/videos/", "-video"])]
    video = [u for u in all_urls if any(v in u for v in ["/video/", "/videos/", "-video"])]
    return nota, video

def get_televisa():
    nota = extract_urls_from_xml("https://tools.nmas.live/rss/sitemap-news.xml")
    video = extract_urls_from_xml("https://www.nmas.com.mx/nmas-video/s3fs-public/sitemap-video-last.xml")
    return nota, video

def get_tvazteca(nota_url, video_url):
    nota = extract_urls_from_xml(nota_url, exclude=["video"])
    video = extract_urls_from_xml(video_url)
    return nota, video

# ----------------------------------------------------------------
# Company map
# ----------------------------------------------------------------
# ----------------------------------------------------------------
# Company map (store functions, not their outputs)
# ----------------------------------------------------------------
companies = {
    "Heraldo": get_heraldo,
    "Televisa": get_televisa,
    "Milenio": get_milenio,
    "Universal": get_universal,
    "As": get_as,
    "Infobae": get_infobae,
    "NyTimes": get_nytimes,
    "Terra": get_terra,
    "Azteca 7": lambda: get_tvazteca(
        "https://www.tvazteca.com/azteca7/newslatest-sitemap-latest.xml",
        "https://www.tvazteca.com/azteca7/video-sitemap-latest.xml"
    ),
    "Azteca UNO": lambda: get_tvazteca(
        "https://www.tvazteca.com/aztecauno/newslatest-sitemap-latest.xml",
        "https://www.tvazteca.com/aztecauno/video-sitemap-latest.xml"
    ),
    "ADN40": lambda: get_tvazteca(
        "https://www.adn40.mx/newslatest-sitemap-latest.xml",
        "https://www.adn40.mx/video-sitemap-latest.xml"
    ),
    "Deportes": lambda: get_tvazteca(
        "https://www.tvazteca.com/aztecadeportes/newslatest-sitemap-latest.xml",
        "https://www.tvazteca.com/aztecadeportes/video-sitemap-latest.xml"
    ),
    "A+": lambda: get_tvazteca(
        "https://www.tvazteca.com/amastv/newslatest-sitemap-latest.xml",
        "https://www.tvazteca.com/amastv/video-sitemap-latest.xml"
    ),
    "Noticias": lambda: get_tvazteca(
        "https://www.tvazteca.com/aztecanoticias/newslatest-sitemap-latest.xml",
        "https://www.tvazteca.com/aztecanoticias/video-sitemap-latest.xml"
    ),
    "Quintana Roo": lambda: get_tvazteca(
        "https://www.aztecaquintanaroo.com//newslatest-sitemap-latest.xml",
        "https://www.aztecaquintanaroo.com//video-sitemap-latest.xml"
    ),
    "Bajío": lambda: get_tvazteca(
        "https://www.aztecabajio.com//newslatest-sitemap-latest.xml",
        "https://www.aztecabajio.com//video-sitemap-latest.xml"
    ),
    "Ciudad Juárez": lambda: get_tvazteca(
        "https://www.aztecaciudadjuarez.com//newslatest-sitemap-latest.xml",
        "https://www.aztecaciudadjuarez.com//video-sitemap-latest.xml"
    ),
    "Yúcatan": lambda: get_tvazteca(
        "https://www.aztecayucatan.com//newslatest-sitemap-latest.xml",
        "https://www.aztecayucatan.com//video-sitemap-latest.xml"
    ),
    "Jalisco": lambda: get_tvazteca(
        "https://www.aztecajalisco.com//newslatest-sitemap-latest.xml",
        "https://www.aztecajalisco.com//video-sitemap-latest.xml"
    ),
    "Puebla": lambda: get_tvazteca(
        "https://www.aztecapuebla.com//newslatest-sitemap-latest.xml",
        "https://www.aztecapuebla.com//video-sitemap-latest.xml"
    ),
    "Veracruz": lambda: get_tvazteca(
        "https://www.aztecaveracruz.com//newslatest-sitemap-latest.xml",
        "https://www.aztecaveracruz.com//video-sitemap-latest.xml"
    ),
    "Baja California": lambda: get_tvazteca(
        "https://www.tvaztecabajacalifornia.com//newslatest-sitemap-latest.xml",
        "https://www.tvaztecabajacalifornia.com//video-sitemap-latest.xml"
    ),
    "Morelos": lambda: get_tvazteca(
        "https://www.aztecamorelos.com//newslatest-sitemap-latest.xml",
        "https://www.aztecamorelos.com//video-sitemap-latest.xml"
    ),
    "Guerrero": lambda: get_tvazteca(
        "https://www.aztecaguerrero.com//newslatest-sitemap-latest.xml",
        "https://www.aztecaguerrero.com//video-sitemap-latest.xml"
    ),
    "Chiapas": lambda: get_tvazteca(
        "https://www.aztecachiapas.com//newslatest-sitemap-latest.xml",
        "https://www.aztecachiapas.com//video-sitemap-latest.xml"
    ),
    "Sinaloa": lambda: get_tvazteca(
        "https://www.aztecasinaloa.com//newslatest-sitemap-latest.xml",
        "https://www.aztecasinaloa.com//video-sitemap-latest.xml"
    ),
    "Aguascalientes": lambda: get_tvazteca(
        "https://www.aztecaaguascalientes.com//newslatest-sitemap-latest.xml",
        "https://www.aztecaaguascalientes.com//video-sitemap-latest.xml"
    ),
    "Queretaro": lambda: get_tvazteca(
        "https://www.aztecaqueretaro.com//newslatest-sitemap-latest.xml",
        "https://www.aztecaqueretaro.com//video-sitemap-latest.xml"
    ),
    "Chihuahua": lambda: get_tvazteca(
        "https://www.aztecachihuahua.com//newslatest-sitemap-latest.xml",
        "https://www.aztecachihuahua.com//video-sitemap-latest.xml"
    ),
    "Laguna": lambda: get_tvazteca(
        "https://www.aztecalaguna.com//newslatest-sitemap-latest.xml",
        "https://www.aztecalaguna.com//video-sitemap-latest.xml"
    ),
}


# ----------------------------------------------------------------
# Gallery extractor for specific image sitemaps
# ----------------------------------------------------------------
def extract_gallery_urls(sitemap_url):
    resp = safe_request(sitemap_url)
    if not resp:
        return [], []
    root = etree.fromstring(resp.content)
    ns = {
        "ns": "http://www.sitemaps.org/schemas/sitemap/0.9",
        "image": "http://www.google.com/schemas/sitemap-image/1.1"
    }
    normal, gallery = [], []
    for url_node in root.xpath("//ns:url", namespaces=ns):
        loc = url_node.find("ns:loc", namespaces=ns)
        if loc is None or loc.text is None:
            continue
        images = url_node.findall("image:image", namespaces=ns)
        if len(images) > 1:
            gallery.append(loc.text.strip())
        else:
            normal.append(loc.text.strip())
    return normal, gallery

gallery_sitemaps = {
    "img.Azteca7":         "https://www.tvazteca.com/azteca7/image-sitemap-latest.xml",
    "img.AztecaUNO":       "https://www.tvazteca.com/aztecauno/image-sitemap-latest.xml",
    "img.AztecaNoticias":  "https://www.tvazteca.com/noticias/image-sitemap-latest.xml"
}

# Pre‐fetch gallery URLs
gallery_urls_map = {}
for key, url in gallery_sitemaps.items():
    _, galleries = extract_gallery_urls(url)
    gallery_urls_map[key] = galleries

# ----------------------------------------------------------------
# Push new unique rows (horizontal, 9 columns per entry)
# ----------------------------------------------------------------
company_counters = {}
company_cache = {}

for name, extractor in companies.items():
    try:
        nota, video = extractor()
        print(f"✅ {name}: {len(nota)} NOTA, {len(video)} VIDEO URLs extracted")
    except Exception as e:
        nota, video = [], []
        print(f"❌ {name}: Extraction failed – {e}")
    company_cache[name] = {"nota": nota, "video": video}
    company_counters[name] = 0

gallery_counters = {name: 0 for name in gallery_sitemaps}
max_new = 10

for content_type in ["nota", "video"]:
    for _ in range(max_new):
        row = []
        skip = True

        for comp in companies:
            urls = company_cache[comp][content_type]
            inserted = False
            while company_counters[comp] < len(urls):
                candidate = urls[company_counters[comp]]
                company_counters[comp] += 1
                if candidate not in used_urls:
                    print(f"➕ New {content_type.upper()} for {comp}: {candidate}")
                    row.extend([today, content_type, candidate, "", "", "", "", "", ""])
                    used_urls.add(candidate)
                    inserted = True
                    skip = False
                    break
            if not inserted:
                row.extend(["", "", "", "", "", "", "", "", ""])

        for gal_name, galleries in gallery_urls_map.items():
            if gallery_counters[gal_name] < len(galleries):
                candidate = galleries[gallery_counters[gal_name]]
                gallery_counters[gal_name] += 1
                if candidate not in used_urls:
                    print(f"➕ New GALLERY from {gal_name}: {candidate}")
                    row.extend([today, "img", candidate, "", "", "", "", "", ""])
                    used_urls.add(candidate)
                    skip = False
                else:
                    row.extend(["", "", "", "", "", "", "", "", ""])
            else:
                row.extend(["", "", "", "", "", "", "", "", ""])

        if not skip:
            sheet.append_row(row)

print("✅ Final rows pushed (no duplicates), including galleries.")



# ----------------------------------------------------------------
# Lighthouse test helpers
# ----------------------------------------------------------------
def extract_metrics(report):
    perf = report.get("categories", {}).get("performance", {}).get("score", 0) * 100
    audits = report.get("audits", {})
    return {
        "score": round(perf, 2),
        "cls": audits.get("cumulative-layout-shift", {}).get("numericValue", ""),
        "lcp": audits.get("largest-contentful-paint", {}).get("numericValue", ""),
        "si": audits.get("speed-index", {}).get("numericValue", ""),
        "tbt": audits.get("total-blocking-time", {}).get("numericValue", ""),
        "fcp": audits.get("first-contentful-paint", {}).get("numericValue", ""),
    }

def run_lighthouse(url, retries=3, delay=2):
    for attempt in range(retries):
        try:
            subprocess.run([
                "lighthouse", url,
                "--quiet",
                "--chrome-flags='--headless'",
                "--output=json", "--output-path=report.json"
            ], check=True)
            with open("report.json", "r") as f:
                report = json.load(f)
            return extract_metrics(report)
        except Exception:
            time.sleep(delay)
    return None

# ----------------------------------------------------------------
# Iterate through sheet rows and test pending URLs
# ----------------------------------------------------------------
all_rows = sheet.get_all_values()
for row_idx, row in enumerate(all_rows, start=1):
    if len(row) < 9:
        continue
    content_type = row[1].lower()
    url = row[2].strip()
    score_cell = row[3].strip()

    if content_type in ("nota", "video", "img") and url.startswith("http") and score_cell == "":
        print(f"🚀 Running Lighthouse test for {content_type.upper()} URL: {url}")
        metrics = run_lighthouse(url, retries=3, delay=2)
        if metrics:
            print(f"✅ Lighthouse Success for {url} — Score: {metrics['score']}")
            sheet.update_cell(row_idx, 4, metrics["score"])
            sheet.update_cell(row_idx, 5, metrics["cls"])
            sheet.update_cell(row_idx, 6, metrics["lcp"])
            sheet.update_cell(row_idx, 7, metrics["si"])
            sheet.update_cell(row_idx, 8, metrics["tbt"])
            sheet.update_cell(row_idx, 9, metrics["fcp"])
        else:
            print(f"❌ Lighthouse Failed for {url}")
        time.sleep(1)

print("✅ URLs written and Lighthouse tests completed.")


