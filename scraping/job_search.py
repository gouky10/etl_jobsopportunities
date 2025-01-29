from playwright.sync_api import sync_playwright
from config.search_params import COUNTRIES
from config.proxies import PROXY_SERVERS, ROTATION_INTERVAL, REQUEST_DELAY, MAX_RETRIES, TIMEOUT
import random
import time
from typing import List, Dict

class JobSearch:
    def __init__(self, headless=True):
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.firefox.launch(
            headless=headless,
            args=["--no-sandbox", "--disable-setuid-sandbox"]
        )
        self.context = self.browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            locale="en-US"
        )
        self.request_count = 0
        self.current_proxy = None
        
    def _rotate_proxy(self):
        self.request_count += 1
        if self.request_count % ROTATION_INTERVAL == 0:
            self.current_proxy = random.choice(PROXY_SERVERS)
            self.context = self.browser.new_context(
                proxy={"server": self.current_proxy},
                user_agent=self._random_user_agent()
            )
            
    def _random_user_agent(self):
        agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0"
        ]
        return random.choice(agents)

    def construct_search_url(self, country_code: str, job_title: str) -> str:
        base = COUNTRIES[country_code]["base_url"]
        params = f"?keywords={job_title.replace(' ', '%20')}" \
                 f"&location={COUNTRIES[country_code]['location']}" \
                 f"&geoId={COUNTRIES[country_code]['geo_id']}" \
                 f"{COUNTRIES[country_code]['filters']}"
        return base + params

    def scrape_jobs(self, job_title: str, countries: List[str]) -> List[Dict]:
        results = []
        page = self.context.new_page()
        
        try:
            for country in countries:
                search_url = self.construct_search_url(country, job_title)
                
                for attempt in range(MAX_RETRIES):
                    try:
                        self._rotate_proxy()
                        page.goto(search_url)
                        page.wait_for_selector('.jobs-search__results-list', timeout=TIMEOUT)
                        
                        # Handle pagination
                        for _ in range(3):  # Scrape first 3 pages
                            job_cards = page.query_selector_all('li:has(> div.base-card)')
                            for card in job_cards:
                                results.append(self._extract_job_data(card))
                            
                            if page.is_visible('button[aria-label="Next"]'):
                                page.click('button[aria-label="Next"]')
                                time.sleep(REQUEST_DELAY)
                            else:
                                break
                        break
                    except Exception as e:
                        if attempt == MAX_RETRIES - 1:
                            raise
                        time.sleep(REQUEST_DELAY * 2)
                        
        finally:
            self.context.close()
            self.browser.close()
            self.playwright.stop()
            
        return results

    def _extract_job_data(self, card) -> Dict:
        def safe_extract(selector, attr=None):
            element = card.query_selector(selector)
            if not element:
                return None
            if attr:
                return element.get_attribute(attr)
            return element.inner_text().strip()

        return {
            "title": safe_extract('h3.base-search-card__title'),
            "company": safe_extract('h4.base-search-card__subtitle a'),
            "location": safe_extract('span.job-search-card__location'),
            "posted": safe_extract('time.job-search-card__listdate', 'datetime'),
            "link": safe_extract('a.base-card__full-link', 'href'),
            "source": "LinkedIn"
        }