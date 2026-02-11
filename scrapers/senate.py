import re
import time
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from scrapers.house import init_driver, quit_driver

HLS_BASE_URL = "https://dlttx48mxf9m3.cloudfront.net/outputs"

# Map Senate video title (without trailing date) to shorthand code.
# Convention: S prefix + keyword abbreviation (mirrors House H prefix).
# Where committees overlap with House, the keyword portion matches:
#   House HAPPR ↔ Senate SAPPR, HEDUC ↔ SEDUC, HHEAL ↔ SHEAL, HNATU ↔ SNATU
SENATE_TITLE_MAP = {
    # Sessions
    "Senate Session":                                "SSESS",
    "Senate Session (Sine Die)":                     "SSDIE",
    # Committees — standardized with House where possible
    "Appropriations":                                "SAPPR",
    "Appropriations Sub - PreK-12":                  "SAPK12",
    "Civil Rights, Judiciary, and Public Safety":     "SCIVL",
    "CREC":                                          "SCREC",
    "Economic and Community Development":             "SECON",
    "Education":                                     "SEDUC",
    "Conference Committee":                           "SCONF",
    "Finance, Insurance, and Consumer Protection":    "SFINC",
    "Health Policy":                                 "SHEAL",
    "Housing and Human Services":                    "SHOUS",
    "Judiciary and Public Safety":                    "SJUDI",
    "Local Government":                              "SLOCL",
    "Natural Resources and Agriculture":             "SNATU",
    "Regulatory Affairs":                            "SREGU",
    "Transportation and Infrastructure":             "STRAN",
    "Veterans and Emergency Services":               "SVETS",
}


def _title_to_shorthand(title, logger=None):
    """Map a Senate video title to its shorthand code (e.g. 'Senate Session 26-02-10' -> 'SSESS')."""
    # Strip trailing date (YY-MM-DD)
    name = re.sub(r'\s*\d{2}-\d{2}-\d{2}\s*$', '', title).strip()

    if name in SENATE_TITLE_MAP:
        return SENATE_TITLE_MAP[name]

    # Catchall for unmapped titles
    if logger:
        logger.warning(f"No shorthand mapping for '{name}' — using 'SMISC'")
    return 'SMISC'


def _build_filename(title, date_str, logger=None):
    """Build a human-readable filename: {SHORTHAND}-{MMDDYY}.mp4 (matches House convention)."""
    shorthand = _title_to_shorthand(title, logger)
    if date_str and date_str != 'Unknown':
        # date_str is YYYY-MM-DD → MMDDYY
        yyyy, mm, dd = date_str.split('-')
        mmddyy = f"{mm}{dd}{yyyy[2:]}"
        return f"{shorthand}-{mmddyy}.mp4"
    return f"{shorthand}.mp4"


def scrape(config, logger):
    """Scrape Michigan Senate video listing. Returns list of video dicts."""
    url = config['sources']['senate']['url']
    logger.info(f"Scraping Senate: {url}")

    logger.info("Initializing Chrome WebDriver...")
    driver = init_driver(config)
    logger.info("Chrome WebDriver initialized")

    try:
        driver.get(url)
        wait_timeout = config.get('selenium', {}).get('wait_timeout', 10)

        # SPA — wait for side menu to render, then click "All Videos"
        WebDriverWait(driver, wait_timeout).until(
            EC.presence_of_element_located((By.CLASS_NAME, "side-menu-item"))
        )
        time.sleep(2)

        for item in driver.find_elements(By.CLASS_NAME, 'side-menu-item'):
            if 'All Videos' in item.text:
                item.click()
                break
        time.sleep(3)

        # Wait for video cards to appear
        WebDriverWait(driver, wait_timeout).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.col-3.mb-3"))
        )

        # Scroll to load all videos (lazy loading)
        last_count = 0
        for _ in range(30):
            driver.execute_script('window.scrollTo(0, document.body.scrollHeight)')
            time.sleep(2)
            cards = driver.find_elements(By.CSS_SELECTOR, 'div.col-3.mb-3')
            if len(cards) == last_count:
                break
            last_count = len(cards)

        cards = driver.find_elements(By.CSS_SELECTOR, 'div.col-3.mb-3')
        logger.info(f"Found {len(cards)} video cards on listing page")

        videos = []
        for card in cards:
            try:
                title = card.text.strip().split('\n')[0] if card.text.strip() else ''

                # Extract video ID from thumbnail URL: /outputs/{VIDEO_ID}/
                imgs = card.find_elements(By.TAG_NAME, 'img')
                if not imgs:
                    continue
                img_src = imgs[0].get_attribute('src') or ''
                if '/outputs/' not in img_src:
                    continue
                video_id = img_src.split('/outputs/')[1].split('/')[0]

                # HLS URL follows a predictable pattern
                hls_url = f"{HLS_BASE_URL}/{video_id}/Default/HLS/out.m3u8"
                date = _parse_date(title)
                filename = _build_filename(title, date, logger)

                videos.append({
                    'filename': filename,
                    'title': title,
                    'date': date,
                    'url': f"https://cloud.castus.tv/vod/misenate/video/{video_id}",
                    'video_id': video_id,
                    'hls_url': hls_url,
                })

            except Exception as e:
                logger.warning(f"Failed to parse video card: {e}")

        logger.info(f"Successfully scraped {len(videos)} Senate videos")
        return videos

    except Exception as e:
        logger.error(f"Failed to scrape Senate videos: {e}")
        return []

    finally:
        quit_driver(driver)
        logger.info("Chrome WebDriver closed")


def _parse_date(title):
    """Parse date from Senate video title. Titles use 'YY-MM-DD' format, e.g. 'Senate Session 26-02-10'."""
    # CastUS format: "Title YY-MM-DD"
    match = re.search(r'(\d{2})-(\d{2})-(\d{2})$', title.strip())
    if match:
        yy, mm, dd = match.group(1), match.group(2), match.group(3)
        return f"20{yy}-{mm}-{dd}"

    # Fallback: MM/DD/YYYY
    match = re.search(r'(\d{1,2})/(\d{1,2})/(\d{4})', title)
    if match:
        mm, dd, yyyy = match.group(1), match.group(2), match.group(3)
        return f"{yyyy}-{int(mm):02d}-{int(dd):02d}"

    # Fallback: YYYY-MM-DD
    match = re.search(r'(\d{4})-(\d{2})-(\d{2})', title)
    if match:
        return match.group(0)

    return 'Unknown'
