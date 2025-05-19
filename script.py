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
import logging
from oauth2client.service_account import ServiceAccountCredentials
from lxml import etree

# ----------------------------------------------------------------
# Setup Logging
# ----------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sitemap_extractor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ----------------------------------------------------------------
# Setup Google Sheets API
# ----------------------------------------------------------------
SHEET_NAME = "SITEMAPS.XML"
WORKSHEET_NAME = "Sheet1"
CREDENTIALS_FILE = "credentials.json"

try:
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
    client = gspread.authorize(creds)
    sheet = client.open(SHEET_NAME).worksheet(WORKSHEET_NAME)
    logger.info("Successfully connected to Google Sheets")
except Exception as e:
    logger.error(f"Failed to connect to Google Sheets: {str(e)}")
    raise

today = datetime.datetime.today().strftime("%Y-%m-%d")

# Solo evitamos repetir URLs dentro de esta corrida, no de semanas anteriores
used_urls = set()

# ----------------------------------------------------------------
# HTTP Helpers
# ----------------------------------------------------------------
# ----------------------------------------------------------------
# HTTP Helpers (with longer waits & retries)
# ----------------------------------------------------------------
def safe_request(url, retries=5, delay=5, timeout=20):
    """
    Try up to `retries` times, waiting `delay` seconds between attempts,
    before giving up on fetching url.
    """
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, timeout=timeout)
            if resp.status_code == 200:
                logger.debug(f"[{attempt}/{retries}] Fetched {url}")
                return resp
            else:
                logger.warning(f"[{attempt}/{retries}] {url} returned {resp.status_code}")
        except Exception as e:
            logger.warning(f"[{attempt}/{retries}] Error fetching {url}: {e}")
        time.sleep(delay)
    logger.error(f"All {retries} attempts failed for {url}")
    return None

def fetch_xml_root(url, retries=5, delay=5):
    """
    Use safe_request to fetch, then attempt to parse XML—and if parse fails,
    retry the whole fetch→parse cycle up to `retries` times.
    """
    for attempt in range(1, retries + 1):
        resp = safe_request(url, retries=1, delay=0)
        if not resp:
            logger.warning(f"[{attempt}/{retries}] No response for XML at {url}")
        else:
            data = resp.content
            # try decompressing
            try:
                data = gzip.GzipFile(fileobj=io.BytesIO(data)).read()
                logger.debug("Gzipped feed, decompressed successfully")
            except (OSError, gzip.BadGzipFile):
                pass
            # try parse
            try:
                root = etree.fromstring(data)
                logger.info(f"[{attempt}/{retries}] Parsed XML from {url}")
                return root
            except Exception as e:
                logger.warning(f"[{attempt}/{retries}] Failed to parse XML: {e}")
        time.sleep(delay)
    logger.error(f"Failed to fetch & parse XML from {url} after {retries} attempts")
    return None


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
        url = loc.text.strip().lower()
        is_video = (
            url.endswith("-video") or 
            "/video/" in url or 
            url.split("/")[-1] == "video" or
            "videos" in url or
            "videogallery" in url
        )
        (video if is_video else nota).append(url)
    return nota, video

def get_as():
    """
    Parses the AS MRSS feeds:
     • Opinion feed for nota → <item><link>
     • Video feed for video → <item><link>
    """
    nota, video = [], []

    # 1) NOTA: opinion articles
    nota_feed = "https://feeds.as.com/mrss-s/pages/as/site/as.com/section/opinion/portada/"
    root_n = fetch_xml_root(nota_feed)
    if root_n is None:
        logger.error("Failed to fetch AS nota feed")
    else:
        for item in root_n.xpath("//item"):
            link = item.find("link")
            if link is not None and link.text:
                nota.append(link.text.strip())
        logger.info(f"AS NOTA: found {len(nota)} URLs")

    # 2) VIDEO: valid AS.com /videos/...-v/ links only
    video_feed = "https://feeds.as.com/mrss-s/list/as/site/as.com/video/"
    root_v = fetch_xml_root(video_feed)
    if root_v is None:
        logger.error("Failed to fetch AS video feed")
    else:
        ns = {"media": "http://search.yahoo.com/mrss/"}
        for item in root_v.xpath("//item"):
            link = item.find("link")
            if link is not None and link.text:
                url = link.text.strip()
                if (
                    url.startswith("https://as.com/") and
                    "/videos/" in url and
                    url.endswith("-v/") and
                    not any(blocked in url for blocked in ["youtube.com", "prisad.com", ".mp4", "vdmedia.as.com"])
                ):
                    video.append(url)

        logger.info(f"AS VIDEO: found {len(video)} clean AS.com URLs")

    return nota, video


def get_terra():
    """
    Fetch Terra’s un_foto feed and return two lists:
      - nota: URLs under /nacionales/
      - video: URLs under /entretenimiento/
    """
    feed_url = "https://www.terra.com.mx/rss/un_foto.html"
    resp = safe_request(feed_url)
    if not resp:
        logger.error("Failed to fetch Terra feed")
        return [], []

    text = resp.text
    # grab all terra.com.mx URLs
    all_urls = re.findall(r"https?://www\.terra\.com\.mx[^\s\"<]+", text)

    # filter by section
    nota  = [u for u in all_urls if "/nacionales/" in u]
    video = [u for u in all_urls if "/entretenimiento/" in u]

    # dedupe while preserving order
    nota  = list(dict.fromkeys(nota))
    video = list(dict.fromkeys(video))

    logger.info(f"Terra: {len(nota)} nota URLs, {len(video)} video URLs")
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
    nota = []
    video = []
    for url in all_urls:
        lower_url = url.lower()
        if any(v in lower_url for v in ["/video/", "/videos/", "-video", "videogaleria"]):
            video.append(url)
        else:
            nota.append(url)
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
companies = {
    "Heraldo": get_heraldo(),
    "Televisa": get_televisa(),
    "Milenio": get_milenio(),
    "Universal": get_universal(),
    "As": get_as(),
    "Infobae": get_infobae(),
    "NyTimes": get_nytimes(),
    "Terra": get_terra(),
    "Azteca 7": get_tvazteca(
        "https://www.tvazteca.com/azteca7/newslatest-sitemap-latest.xml",
        "https://www.tvazteca.com/azteca7/video-sitemap-latest.xml"
    ),
    "Azteca UNO": get_tvazteca(
        "https://www.tvazteca.com/aztecauno/newslatest-sitemap-latest.xml",
        "https://www.tvazteca.com/aztecauno/video-sitemap-latest.xml"
    ),
    "ADN40": get_tvazteca(
        "https://www.adn40.mx/newslatest-sitemap-latest.xml",
        "https://www.adn40.mx/video-sitemap-latest.xml"
    ),
    "Deportes": get_tvazteca(
        "https://www.tvazteca.com/aztecadeportes/newslatest-sitemap-latest.xml",
        "https://www.tvazteca.com/aztecadeportes/video-sitemap-latest.xml"
    ),
    "A+": get_tvazteca(
        "https://www.tvazteca.com/amastv/newslatest-sitemap-latest.xml",
        "https://www.tvazteca.com/amastv/video-sitemap-latest.xml"
    ),
    "Noticias": get_tvazteca(
        "https://www.tvazteca.com/aztecanoticias/newslatest-sitemap-latest.xml",
        "https://www.tvazteca.com/aztecanoticias/video-sitemap-latest.xml"
    ),
    "Quintana Roo": get_tvazteca(
        "https://www.aztecaquintanaroo.com//newslatest-sitemap-latest.xml",
        "https://www.aztecaquintanaroo.com//video-sitemap-latest.xml"
    ),
    "Bajío": get_tvazteca(
        "https://www.aztecabajio.com//newslatest-sitemap-latest.xml",
        "https://www.aztecabajio.com//video-sitemap-latest.xml"
    ),
    "Ciudad Juárez": get_tvazteca(
        "https://www.aztecaciudadjuarez.com//newslatest-sitemap-latest.xml",
        "https://www.aztecaciudadjuarez.com//video-sitemap-latest.xml"
    ),
    "Yúcatan": get_tvazteca(
        "https://www.aztecayucatan.com//newslatest-sitemap-latest.xml",
        "https://www.aztecayucatan.com//video-sitemap-latest.xml"
    ),
    "Jalisco": get_tvazteca(
        "https://www.aztecajalisco.com//newslatest-sitemap-latest.xml",
        "https://www.aztecajalisco.com//video-sitemap-latest.xml"
    ),
    "Puebla": get_tvazteca(
        "https://www.aztecapuebla.com//newslatest-sitemap-latest.xml",
        "https://www.aztecapuebla.com//video-sitemap-latest.xml"
    ),
    "Veracruz": get_tvazteca(
        "https://www.aztecaveracruz.com//newslatest-sitemap-latest.xml",
        "https://www.aztecaveracruz.com//video-sitemap-latest.xml"
    ),
    "Baja California": get_tvazteca(
        "https://www.tvaztecabajacalifornia.com//newslatest-sitemap-latest.xml",
        "https://www.tvaztecabajacalifornia.com//video-sitemap-latest.xml"
    ),
    "Morelos": get_tvazteca(
        "https://www.aztecamorelos.com//newslatest-sitemap-latest.xml",
        "https://www.aztecamorelos.com//video-sitemap-latest.xml"
    ),
    "Guerrero": get_tvazteca(
        "https://www.aztecaguerrero.com//newslatest-sitemap-latest.xml",
        "https://www.aztecaguerrero.com//video-sitemap-latest.xml"
    ),
    "Chiapas": get_tvazteca(
        "https://www.aztecachiapas.com//newslatest-sitemap-latest.xml",
        "https://www.aztecachiapas.com//video-sitemap-latest.xml"
    ),
    "Sinaloa": get_tvazteca(
        "https://www.aztecasinaloa.com//newslatest-sitemap-latest.xml",
        "https://www.aztecasinaloa.com//video-sitemap-latest.xml"
    ),
    "Aguascalientes": get_tvazteca(
        "https://www.aztecaaguascalientes.com//newslatest-sitemap-latest.xml",
        "https://www.aztecaaguascalientes.com//video-sitemap-latest.xml"
    ),
    "Queretaro": get_tvazteca(
        "https://www.aztecaqueretaro.com//newslatest-sitemap-latest.xml",
        "https://www.aztecaqueretaro.com//video-sitemap-latest.xml"
    ),
    "Chihuahua": get_tvazteca(
        "https://www.aztecachihuahua.com//newslatest-sitemap-latest.xml",
        "https://www.aztecachihuahua.com//video-sitemap-latest.xml"
    ),
    "Laguna": get_tvazteca(
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
    try:
        _, galleries = extract_gallery_urls(url)
        gallery_urls_map[key] = galleries
        logger.info(f"Found {len(galleries)} gallery URLs for {key}")
    except Exception as e:
        logger.error(f"Failed to extract gallery URLs for {key}: {str(e)}")
        gallery_urls_map[key] = []

# ----------------------------------------------------------------
# Push new rows, one horizontal entry per company (nota, video, img)
# ----------------------------------------------------------------
try:
    max_rows = 10  # up to 10 entries of each type
    content_types = ["nota", "video"]

    for content_type in content_types:
        logger.info(f"⏩ Building up to {max_rows} rows for {content_type}")
        for i in range(max_rows):
            row = []
            any_url = False

            # 1) News/video URLs from each company
            for comp, (nota_list, video_list) in companies.items():
                urls = nota_list if content_type == "nota" else video_list

                if i < len(urls):
                    url = urls[i]
                    if url not in used_urls:
                        row.extend([today, content_type, url] + [""] * 6)
                        used_urls.add(url)
                        any_url = True
                    else:
                        # Ya se usó en esta corrida (no en semanas pasadas), se deja vacío
                        row.extend([""] * 9)
                else:
                    # No hay suficientes nuevas, repetir la última si existe
                    if len(urls) > 0:
                        last_url = urls[-1]
                        if last_url not in used_urls:
                            row.extend([today, content_type, last_url] + [""] * 6)
                            used_urls.add(last_url)
                            any_url = True
                        else:
                            row.extend([""] * 9)
                    else:
                        row.extend([""] * 9)

            # 2) Gallery URLs
            for gal_key, galleries in gallery_urls_map.items():
                if i < len(galleries):
                    url = galleries[i]
                    if url not in used_urls:
                        row.extend([today, "img", url] + [""] * 6)
                        used_urls.add(url)
                        any_url = True
                    else:
                        row.extend([""] * 9)
                else:
                    if len(galleries) > 0:
                        last_url = galleries[-1]
                        if last_url not in used_urls:
                            row.extend([today, "img", last_url] + [""] * 6)
                            used_urls.add(last_url)
                            any_url = True
                        else:
                            row.extend([""] * 9)
                    else:
                        row.extend([""] * 9)

            # Append the row
            if any_url:
                try:
                    sheet.append_row(row)
                    logger.info(f"✅ Appended row #{i+1} for {content_type}")
                except Exception as e:
                    logger.error(f"❌ Failed to append row #{i+1}: {e}")

except Exception as e:
    logger.error(f"🔥 Error in main processing loop: {str(e)}")
    raise




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
            # Clean up old Chrome instances
            subprocess.run("pkill -f chrome", shell=True)

            subprocess.run([
                "lighthouse", url,
                "--quiet",
                "--chrome-flags='--headless'",
                "--output=json", "--output-path=report.json"
            ], check=True)

            # Clean again just in case
            subprocess.run("pkill -f chrome", shell=True)

            with open("report.json", "r") as f:
                report = json.load(f)
            return extract_metrics(report)
        except Exception as e:
            logger.warning(f"Lighthouse attempt {attempt + 1} failed for {url}: {str(e)}")
            time.sleep(delay)
    return None


# ----------------------------------------------------------------
# Full Lighthouse loop (real runs + sheet updates)
# ----------------------------------------------------------------
try:
    all_rows = sheet.get_all_values()
    logger.info(f"Found {len(all_rows)} rows to process for Lighthouse")

    for row_idx, row in enumerate(all_rows, start=1):
        groups = len(row) // 9

        for g in range(groups):
            base = g * 9
            content_type = row[base + 1].strip().lower()
            url          = row[base + 2].strip()
            score_cell   = row[base + 3].strip()

            if content_type in ("nota", "video", "img") and url.startswith("http") and score_cell == "":
                logger.info(f"Running Lighthouse for {content_type} URL: {url}")
                metrics = run_lighthouse(url, retries=3, delay=2)

                if metrics:
                    try:
                        # base+4 is the “Score” column in this group
                        sheet.update_cell(row_idx, base + 4, metrics["score"])
                        sheet.update_cell(row_idx, base + 5, metrics["cls"])
                        sheet.update_cell(row_idx, base + 6, metrics["lcp"])
                        sheet.update_cell(row_idx, base + 7, metrics["si"])
                        sheet.update_cell(row_idx, base + 8, metrics["tbt"])
                        sheet.update_cell(row_idx, base + 9, metrics["fcp"])
                        logger.info(f"Updated metrics for row {row_idx}, group {g+1}")
                    except Exception as e:
                        logger.error(f"Failed to update metrics at row {row_idx}, group {g+1}: {e}")
                else:
                    logger.warning(f"Failed to get Lighthouse metrics for {url}")

                # throttle between runs
                time.sleep(5)

    logger.info("✅ URLs written and Lighthouse tests completed.")
except Exception as e:
    logger.error(f"Error in Lighthouse processing: {e}")
    raise
