from modules.scrapers.base_scraper import BaseScraper
# from modules.utils.get_timestamp import get_timestamp
import asyncio
from playwright.async_api import async_playwright

class PageScraper(BaseScraper):
    """
        A class to scrape web pages with a specific URL structure.
        Inherits from BaseScraper to utilize customizable URL generation.

        attributes:
        - domain: The base domain for the URL
        - path_keys: List of keys to be used in the URL path
        - query_keys: List of keys to be used in the URL query parameters
        - scheme: The URL scheme (default is 'https')
    """

    def __init__(self, scheme: str = 'https', domain: str = None, path_keys: list = [], query_keys: list = [], **kwargs):
        super().__init__(scheme, domain, path_keys, query_keys, **kwargs)

    async def scrape_page(self, browser, url, semaphore):
        async with semaphore:
            page = await browser.new_page(user_agent = self.user_agent)
            try:
                response = await page.goto(url)
                print(f"Scraped {url} - Status: {response.status}")
                content = await page.content()
                return {
                    'url': url, 
                    'content': content,
                    # 'timestamp': get_timestamp()
                } 
            
            except:
                return {
                    'url': url, 
                    'content': None, 
                    # 'timestamp': get_timestamp()
                }
            
            finally:
                await page.close()

    async def scrape_all(self, urls: list = [], concurrency_limit: int = 5, headless: bool = True):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless = headless)
            semaphore = asyncio.Semaphore(concurrency_limit)
            tasks = [self.scrape_page(browser, url, semaphore) for url in urls]
            results = await asyncio.gather(*tasks)
            await browser.close()

            return results