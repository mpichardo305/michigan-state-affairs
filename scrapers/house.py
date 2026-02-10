import re
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def init_driver(config):
    """Initialize Selenium WebDriver with anti-detection settings"""
    chrome_options = Options()

    if config.get('selenium', {}).get('headless', True):
        chrome_options.add_argument('--headless')

    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument(
        'user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
        'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    )

    driver = webdriver.Chrome(options=chrome_options)
    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )

    return driver


def quit_driver(driver):
    """Quit the WebDriver"""
    if driver:
        driver.quit()


def scrape(config, logger):
    """Scrape Michigan House video archive. Returns list of video dicts."""
    url = config['sources']['house']['url']
    logger.info(f"Scraping: {url}")

    logger.info("Initializing Chrome WebDriver...")
    driver = init_driver(config)
    logger.info("Chrome WebDriver initialized")

    try:
        driver.get(url)

        WebDriverWait(driver, config.get('selenium', {}).get('wait_timeout', 10)).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        time.sleep(2)

        video_links = driver.find_elements(
            By.XPATH,
            "//a[contains(@href, '/VideoArchivePlayer?video=')]"
        )
        logger.info(f"Found {len(video_links)} video links")

        videos = []
        for link in video_links:
            try:
                href = link.get_attribute('href')
                if not href or '/VideoArchivePlayer?video=' not in href:
                    continue

                filename = href.split('video=')[1]

                parent = link.find_element(
                    By.XPATH,
                    "./ancestor::li[contains(@class, 'page-search-container')]"
                )

                # Parse category from page, strip " | X Videos" suffix
                try:
                    category_elem = parent.find_element(By.TAG_NAME, 'strong')
                    category = re.sub(r'\s*\|.*$', '', category_elem.text.strip())
                except Exception:
                    category = "Unknown"

                # Parse date from filename slug (MMDDYY)
                stem = filename.rsplit('.', 1)[0]  # e.g. "HAPPR-011626"
                date_match = re.search(r'(\d{6})$', stem)
                if date_match:
                    mmddyy = date_match.group(1)
                    mm, dd, yy = mmddyy[:2], mmddyy[2:4], mmddyy[4:]
                    date = f"20{yy}-{mm}-{dd}"
                else:
                    date = "Unknown"

                videos.append({
                    'filename': filename,
                    'category': category,
                    'date': date,
                    'url': href
                })

            except Exception as e:
                logger.warning(f"Failed to parse video link: {e}")
                continue

        logger.info(f"Successfully parsed {len(videos)} videos")
        return videos

    except Exception as e:
        logger.error(f"Failed to scrape videos: {e}")
        return []

    finally:
        quit_driver(driver)
        logger.info("Chrome WebDriver closed")
