"""
Phase 1: Scrape all Fishdope report pages and store raw HTML + text.

Iterates through listing pages (1 to 2087), extracts each report's
post ID, title, date, URL, and full text content.

Usage:
    python scraper.py [--start-page 1] [--end-page 2087]
"""
import os
import re
import json
import time
import logging
import argparse
from pathlib import Path
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from config import (
    FISHDOPE_SESSION_COOKIE_NAME, FISHDOPE_SESSION_COOKIE_VALUE,
    BASE_URL, LISTING_PAGE_URL, TOTAL_PAGES,
    SCRAPE_DELAY_SECONDS, MAX_RETRIES, RETRY_DELAY_SECONDS,
    REQUEST_TIMEOUT, RAW_HTML_DIR, RAW_TEXT_DIR,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("scraper.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


class FishdopeScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.cookies.set(
            FISHDOPE_SESSION_COOKIE_NAME,
            FISHDOPE_SESSION_COOKIE_VALUE,
            domain=".fishdope.com",
        )
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        })
        Path(RAW_HTML_DIR).mkdir(parents=True, exist_ok=True)
        Path(RAW_TEXT_DIR).mkdir(parents=True, exist_ok=True)

    def fetch_page(self, page_num: int):
        url = BASE_URL if page_num == 1 else LISTING_PAGE_URL.format(page=page_num)
        for attempt in range(MAX_RETRIES):
            try:
                resp = self.session.get(url, timeout=REQUEST_TIMEOUT)
                if resp.status_code == 200:
                    return resp.text
                elif resp.status_code == 403:
                    logger.error(f"Page {page_num}: 403 - Cookie expired?")
                    return None
                elif resp.status_code == 404:
                    logger.warning(f"Page {page_num}: 404 - End of archive?")
                    return None
                else:
                    logger.warning(f"Page {page_num}: HTTP {resp.status_code}")
            except requests.exceptions.RequestException as e:
                logger.warning(f"Page {page_num}: Error {e}, attempt {attempt+1}")
            time.sleep(RETRY_DELAY_SECONDS)
        logger.error(f"Page {page_num}: Failed after {MAX_RETRIES} attempts")
        return None

    def extract_reports_from_listing(self, html: str, page_num: int):
        soup = BeautifulSoup(html, "lxml")
        reports = []

        # Each report is in div.post with id="post-{ID}"
        wrapper = soup.select_one("div.the_content_wrapper")
        if not wrapper:
            logger.warning(f"Page {page_num}: No content wrapper found")
            return reports

        post_divs = wrapper.select("div.post[id^='post-']")
        for post_div in post_divs:
            post_id = post_div.get("id", "").replace("post-", "")

            # Title from h3.post-title or h3
            title_el = post_div.select_one("h3.post-title") or post_div.select_one("h3")
            title = title_el.get_text(strip=True) if title_el else ""

            # Timestamp
            updated_el = post_div.select_one("div.post-updated")
            timestamp = updated_el.get_text(strip=True) if updated_el else ""

            # Report URL from footer/contentinfo "Read more" link
            url = ""
            footer = post_div.find_next_sibling()
            if footer:
                link = footer.select_one("a[href]")
                if link:
                    url = link.get("href", "")

            # Full text content
            desc_div = post_div.select_one(".desc")
            if desc_div:
                paragraphs = desc_div.find_all("p")
                full_text = "\n\n".join(
                    p.get_text(separator="\n", strip=False) for p in paragraphs
                )
            else:
                full_text = post_div.get_text(separator="\n", strip=False)

            raw_html = str(desc_div) if desc_div else str(post_div)
            report_date = self._parse_date_from_title(title)

            reports.append({
                "post_id": post_id,
                "title": title,
                "timestamp": timestamp,
                "url": url,
                "date": report_date,
                "raw_text": full_text.strip(),
                "raw_html": raw_html,
                "page_num": page_num,
                "scraped_at": datetime.utcnow().isoformat(),
            })
        return reports

    def _parse_date_from_title(self, title: str) -> str:
        clean = re.sub(r"(\\d+)(st|nd|rd|th)", r"\\1", title)
        clean = re.sub(
            r"^(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)[,\\s]*",
            "", clean
        )
        for fmt in ["%B %d, %Y", "%B, %d %Y", "%B %d %Y", "%b %d, %Y"]:
            try:
                return datetime.strptime(clean.strip(), fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return ""

    def scrape_page(self, page_num: int):
        cache_file = Path(RAW_HTML_DIR) / f"page_{page_num:04d}.html"
        if cache_file.exists():
            html = cache_file.read_text(encoding="utf-8")
        else:
            html = self.fetch_page(page_num)
            if not html:
                return []
            cache_file.write_text(html, encoding="utf-8")
            time.sleep(SCRAPE_DELAY_SECONDS)

        reports = self.extract_reports_from_listing(html, page_num)
        for report in reports:
            if report["post_id"]:
                text_file = (
                    Path(RAW_TEXT_DIR)
                    / f"{report['date']}_{report['post_id']}.json"
                )
                text_file.write_text(
                    json.dumps(report, indent=2), encoding="utf-8"
                )
        logger.info(f"Page {page_num}: Extracted {len(reports)} reports")
        return reports

    def scrape_all(self, start_page=1, end_page=TOTAL_PAGES):
        all_reports = []
        for page_num in tqdm(range(start_page, end_page + 1), desc="Scraping"):
            reports = self.scrape_page(page_num)
            all_reports.extend(reports)
            if page_num % 100 == 0:
                logger.info(
                    f"Progress: {page_num}/{end_page}, "
                    f"{len(all_reports)} total reports"
                )

        # Save master index
        index_file = Path(RAW_TEXT_DIR) / "_index.json"
        index_data = [
            {
                "post_id": r["post_id"],
                "date": r["date"],
                "title": r["title"],
                "url": r["url"],
            }
            for r in all_reports
        ]
        index_file.write_text(json.dumps(index_data, indent=2), encoding="utf-8")
        logger.info(f"Done: {len(all_reports)} reports from {end_page-start_page+1} pages")
        return all_reports


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape Fishdope fish reports")
    parser.add_argument("--start-page", type=int, default=1)
    parser.add_argument("--end-page", type=int, default=TOTAL_PAGES)
    args = parser.parse_args()

    scraper = FishdopeScraper()
    reports = scraper.scrape_all(args.start_page, args.end_page)
    print(f"Scraped {len(reports)} reports.")
