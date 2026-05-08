import requests
from bs4 import BeautifulSoup
import logging
from typing import List, Dict
import time

logger = logging.getLogger(__name__)

BASE_URL = "https://quotes.toscrape.com"


class QuoteSpider:
    def __init__(self, delay: float = 1.0, max_pages: int = 5):
        self.delay = delay
        self.max_pages = max_pages
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; PythonScraper/1.0)"
        })

    def scrape(self) -> List[Dict]:
        all_quotes = []
        page = 1

        while page <= self.max_pages:
            url = f"{BASE_URL}/page/{page}/"
            logger.info(f"Scraping page {page}: {url}")

            try:
                response = self.session.get(url, timeout=10)
                response.raise_for_status()
            except requests.RequestException as e:
                logger.error(f"Request failed for page {page}: {e}")
                break

            quotes = self._parse_page(response.text, page)
            if not quotes:
                logger.info("No more quotes found — stopping")
                break

            all_quotes.extend(quotes)
            logger.info(f"Found {len(quotes)} quotes on page {page}")

            page += 1
            if page <= self.max_pages:
                time.sleep(self.delay)

        return all_quotes

    def _parse_page(self, html: str, page: int) -> List[Dict]:
        soup = BeautifulSoup(html, "lxml")
        quotes = []

        for quote_el in soup.select(".quote"):
            text_el = quote_el.select_one(".text")
            author_el = quote_el.select_one(".author")
            tags = [t.get_text() for t in quote_el.select(".tag")]

            if text_el and author_el:
                quotes.append({
                    "text": text_el.get_text(strip=True).strip('""\u201c\u201d'),
                    "author": author_el.get_text(strip=True),
                    "tags": ", ".join(tags),
                    "tag_count": len(tags),
                    "page": page,
                })

        return quotes
