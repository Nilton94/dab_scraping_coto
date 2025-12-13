from urllib.parse import urlunparse
from playwright.async_api import async_playwright
from random_user_agent.user_agent import UserAgent
import requests
from bs4 import BeautifulSoup
import re
import asyncio
import aiohttp
from math import ceil
from modules.utils.retry import retry
import os

try:
    from modules.utils.logger import get_logger
    logger = get_logger()
except:
    import logging
    logger = logging.getLogger(__name__)

class BaseScraper:
    """
        Base class for web scraping with customizable URL generation.
        This class allows you to define a domain, path keys, and query keys
        
        attributes:
        - domain: The base domain for the URL
        - path_keys: List of keys to be used in the URL path (e.g, 'transacao', 'estado', 'cidade')
        - query_keys: List of keys to be used in the URL query parameters (e.g, 'pagina')
        - scheme: The URL scheme (default is 'https')
        - user_agent: Random user agent for requests
        - required_string: If provided, only returns true if the status code is in 2XX and exists the string in response.text
        - kwargs: Additional keyword arguments for path and query parameters (e.g, 'transacao'='venda', 'estado'='rio-grande-do-sul', 'cidade'='sao-leopoldo', 'pagina'=1)

        methods:
        - get_url: Generates a URL based on the provided path and query keys.

        Example 1:
        ```
            scraper = BaseScraper(
                domain="example.com",
                path_keys=["section", "item"],
                query_keys=["page", "sort"],
                item="latest",
                page=1,
                sort="asc"
            )
            url = scraper.get_url()
            print(url)
            # Output: https://www.example.com/section/item/latest?page=1&sort=asc
        ```

        Example 2:
        ```
            scraper = BaseScraper(
                domain="lagallega.com.ar", 
                item="productosnl.asp",
                query_keys=["pg","TM"],
                path_keys=['secao','produtos'],
                required_string='Oferta'
            )
            url = scraper.get_url()
            print(url)
            # 'https://www.lagallega.com.ar/{secao}/{produtos}/productosnl.asp?pg={pg}&TM={TM}'

            scraper.get_url().format(
                secao='produtos',
                produtos='doces',
                pg=1,
                TM='CX'
            )
            # 'https://www.lagallega.com.ar/produtos/doces/productosnl.asp?pg=1&TM=CX'
        ```
    """
    
    def __init__(
            self, 
            scheme: str = 'https', 
            domain: str = None, 
            section: str = None, 
            item: str = None, 
            path_keys: list = [], 
            query_keys: list = [], 
            required_string: str = None, 
            **kwargs
        ):
        self.scheme = scheme
        self.domain = domain
        self.section = section
        self.item = item
        self.path_keys = path_keys
        self.query_keys = query_keys
        self.user_agent = UserAgent().get_random_user_agent()
        self.kwargs = kwargs
        self.required_string = required_string

        logger.info(f"Initialized BaseScraper for domain: {self.domain}")
        
    def __repr__(self) -> str:
        return f"<BaseScraper(domain='{self.domain}', path_keys={self.path_keys}, query_keys={self.query_keys}), kwargs={self.kwargs})>"
    
    @property
    def netloc(self) -> str:
        return f"www.{self.domain}"


    @property
    def base_url(self) -> str:
        """
            Generates the base URL template with placeholders for path and query parameters.
            Returns:
                str: A URL template with placeholders for path and query parameters.
        """

        logger.info(f"Generating base URL for domain: {self.domain}")

        if self.path_keys:
            path = '/' + '/'.join(f"{{{key}}}" for key in self.path_keys) + '/'
        else:
            path = ''
        
        try:
            if self.query_keys:
                query_string = '&'.join([f"{key}={{{key}}}" for key in self.query_keys])
            else:
                query_string = ''
        except KeyError as e:
            query_string = ''

        if self.item:
            path = '/' + path.lstrip('/') + self.item

        logger.info(f"Generated base URL: {urlunparse((self.scheme, self.netloc, path, '', query_string, ''))}")

        return urlunparse((self.scheme, self.netloc, path, '', query_string, ''))
    

    def get_url(self, *override_keys) -> str:
        """
            Generates a complete URL based on the provided path and query parameters.

            Args:
                *override_keys: Optional keys to override the default path and query parameters. (e.g., 'pagina' will become '{pagina}' in the output URL)

            Returns:
                str: The complete URL with the specified path and query parameters.
            
            Raises:
                ValueError: If a required path key is missing in the kwargs.

            Example:
            ```
                scraper = BaseScraper(
                    domain="lagallega.com.ar",
                    item="productosnl.asp",
                    query_keys=["pg", "nl"],
                    pg=1,  
                    nl="03"
                )

                scraper.get_url()
                # "https://www.lagallega.com.ar/productosnl.asp?pg=1&nl=03"

                scraper.get_url('pg', 'nl')
                # "https://www.lagallega.com.ar/productosnl.asp?pg={pg}&nl={nl}"
        """

        logger.info(f"Generating URL with override keys: {override_keys}")

        kwargs = self.kwargs.copy()
        
        try:
            logger.info(f"Formatting URL with kwargs: {kwargs}")

            for key in self.path_keys + self.query_keys:
                if key not in kwargs or key in override_keys:
                    kwargs[key] = f'{{{key}}}'

            return self.base_url.format(**kwargs)
        except KeyError as e:
            logger.error(f"Missing required key: {e.args}")
            raise ValueError(f"Missing required key: {e.args}")
        

    def get_all_urls(
            self, 
            page_param_name: str = 'pagina', 
            items_per_page: int = 30, 
            html_tag: str = None, 
            tag_params: dict = None, 
            custom_page_number: int = None,
            regex: str = None, 
            item_oriented: bool = False
        ) -> list[str]:
        """
            Generate all paginated URLs based on total pages extracted from the site.

            Args:
                page_param_name (str): Name of the pagination parameter in the query (e.g., 'pagina').
                items_per_page (int): Number of items per page.
                html_tag (str): HTML tag that contains the total item count.
                tag_params (dict): Tag attributes for identifying the total item count.
                custom_page_number (int): overwrite the actual total page number by a specific one.

            Returns:
                List[str]: A list of URLs with different page values.
        """

        logger.info(f"Generating all URLs for pagination with page_param_name: {page_param_name}")

        base_url = self.get_url(page_param_name)
        if custom_page_number:
            return [
                base_url.format(**{page_param_name: page_num})
                for page_num in range(1, custom_page_number + 1)
            ]
        
        else:
            total_pages = self._get_total_pages(items_per_page, html_tag, tag_params, regex, item_oriented).get('total_pages', 1)
            logger.info(f"Total pages calculated: {total_pages}")
            return [
                base_url.format(**{page_param_name: page_num})
                for page_num in range(1, total_pages + 1)
            ]
        
        
    def get_html(self, *url_args, **headers_kwargs) -> str:
        """
            Retrieves the HTML content of a page. First attempts with requests,
            if it fails, falls back to Playwright browser automation.

            Args:
                *url_args: Optional arguments to override the path and query parameters in the URL.
                **headers_kwargs: Additional headers for the requests call.

            Returns:
                str: The HTML content of the page.
        """

        logger.info(f"Extracting HTML from page {self.get_url()}")

        try:
            if self.url_accepts_request(*url_args)['bool']:
                logger.info(f"URL accepts requests: {self.get_url(*url_args)}")
                response = self._url_requests(*url_args, **headers_kwargs)
                return response.text
            else:
                logger.info(f"[Fallback] requests failed, trying browser...")
                return asyncio.run(self._browser_requests(*url_args))
        except Exception as e:
            logger.error(f"[Fallback] requests raised exception: {e}, trying browser...")
            return asyncio.run(self._browser_requests(*url_args))

    def url_accepts_request(self, *url_args) -> bool:
        """
            Checks if the URL generated by the scraper accepts requests.

            Args:
                * url_args: Optional arguments to override the path and query parameters in the URL.
            Returns:
                bool: True if the URL accepts requests (status code 200), False otherwise.
        """

        url = self.get_url(*url_args)
        logger.info(f"Checking if URL accepts requests: {url}")
        
        try:
            request = self._url_requests(*url_args)
            
            if self.required_string and self.required_string not in request.text:
                logger.info(f"Required string '{self.required_string}' not found in response.")

                return {
                    'url': url, 
                    'status_code': request.status_code, 
                    'length': len(request.text),
                    'bool': False,
                    'request': request
                }
            
            logger.info(f"Request successful with status code: {request.status_code} and response length: {len(request.text)}")
            return {
                'url': url, 
                'status_code': request.status_code, 
                'length': len(request.text),
                'bool': request.status_code in [200, 201, 202, 204],
                'request': request
            }
        except Exception as e:
            logger.error(f"Request failed: {e}")
            return {
                'url': url, 
                'status_code': None, 
                'length': None,
                'bool': False,
                'request': None
            }
        

    @retry(retries=5, delay=2)
    def _url_requests(self, *url_args, **headers_kwargs) -> requests.Response:
        """
            Makes a GET request to the generated URL with the specified headers.
            Args:
                **headers_kwargs: Keyword arguments representing headers to be included in the request.
                *url_args: Optional arguments to override the path and query parameters in the URL.
            Returns:
                requests.Response: The response object from the GET request.
        """
        url = self.get_url(*url_args)
        logger.info(f"Making URL request to: {url}")

        try:    
            logger.info(f"Headers for request: {{'User-Agent': {self.user_agent}, **{headers_kwargs}}}")
            response = requests.get(
                url=url,
                headers={'User-Agent': self.user_agent, **headers_kwargs}
            )
            response.raise_for_status() 
            logger.info(f"Request successful with status code: {response.status_code} and response length: {len(response.text)}")
            return response
        
        except Exception as e:
            logger.error(f"Request failed: {e}")
            return None
    
    async def _async_url_request(self, session: aiohttp.ClientSession, *url_args, **headers_kwargs) -> tuple[str, str]:
        """
            Makes an async GET request to the generated URL.
            Args:
                session: aiohttp ClientSession for making requests
                **headers_kwargs: Keyword arguments for headers
                *url_args: Optional arguments to override URL parameters
            Returns:
                tuple: (url, html_content) or (url, None) if failed
        """
        url = self.get_url(*url_args)
        headers = {'User-Agent': self.user_agent, **headers_kwargs}
        
        try:
            async with session.get(url, headers=headers) as response:
                response.raise_for_status()
                html = await response.text()
                logger.info(f"Async request successful for {url}")
                return url, html
        except Exception as e:
            logger.error(f"Async request failed for {url}: {e}")
            return url, None
        
    @retry(retries=5, delay=2)
    async def _browser_requests(self, *url_args):
        """ 
            Makes an asynchronous GET request to the generated URL using Playwright. Trying to use headless mode first, and if it fails, it retries in head mode.

            Args:
                *url_args: Optional arguments to override the path and query parameters in the URL.

            Returns:
                str: The HTML content of the page.
        """
        
        url = self.get_url(*url_args)
        logger.info(f"Making browser request to: {url}")

        async with async_playwright() as p:
            logger.info("Launching browser in headless mode")
            browser = await p.chromium.launch(
                headless=True,
                args=['--disable-blink-features=AutomationControlled']
            )
            context = await browser.new_context(
                user_agent=self.user_agent,
                viewport={'width': 1920, 'height': 1080},
                locale='es-AR'
            )

            await context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                })
            """)

            page = await context.new_page()
            response = await page.goto(url, timeout=60000)
            status = response.status if response else None

            if not status or not 200 <= status < 300:
                logger.info(f"[Headless] Failed with status {status}, retrying with head...")
                await browser.close()

                browser = await p.chromium.launch(headless=False)
                context = await browser.new_context(user_agent=self.user_agent)
                page = await context.new_page()
                response = await page.goto(url, timeout=60000)
                content = await page.content()
                await browser.close()
                logger.info(f"[Head] Request with head mode returned with response length: {len(content)}")
                return content

            content = await page.content()
            await browser.close()
            logger.info(f"[Headless] Request successful with response length: {len(content)}")
            return content
    
        
    @retry(retries=5, delay=2)
    def _get_total_pages(
        self, 
        items_per_page: int = 30, 
        html_tag: str = None, 
        tag_params: dict = None, 
        regex: str = None, 
        item_oriented: bool = False,
        *url_args
    ) -> int:
        """ 
            Retrieves the total number of pages based on the content of a specific HTML tag.

            Args:
                items_per_page (int): The number of items displayed per page (default is 30).
                html_tag (str): The HTML tag to search for the number of total items.
                tag_params (dict): Additional parameters to filter the HTML tag (default is None).
                url_args: Optional arguments to override the path and query parameters in the URL.
                regex (str): Optional regular expression to extract the number of items from the tag's text.
                item_oriented (bool): If True, the number returned represents total items instead of total pages.
                required_string: If provided, only returns true if the status code is in 2XX and exists the string in response.text
            Returns:
                int: The total number of pages, calculated based on the number of items and items per page.
            Example:
            
            Example: 
            ```
                scraper = BaseScraper(
                    domain="lagallega.com.ar",
                    item="productosnl.asp",
                    query_keys=["pg", "nl"],
                    pg=1,  
                    nl="03000000"
                )

                items = scraper._get_total_pages(
                    items_per_page=20,
                    html_tag='td',
                    tag_params={'class': 'pieBot'},
                    regex=r'de\s*(\d+)'
                )

                print(items)
                # {'items_per_page': 20, 'total_items': 1500, 'total_pages': 75, 'item_tag_text': '1 de 75', 'orientation': 'pages'}
            ```
        """

        logger.info(f"Calculating total pages from HTML tag: {html_tag} with params: {tag_params}")

        def return_calc(count, items_per_page, item_oriented):
            if item_oriented:
                total_pages = ceil(count / items_per_page)
                total_items = count
                return total_pages, total_items
            else:
                total_pages = count 
                total_items = ceil(count * items_per_page)
                return total_pages, total_items
        
        try:
            if self.url_accepts_request(*url_args)['bool']:
                logger.info("Using URL requests method")
                response = self._url_requests(*url_args)
            else:
                logger.info("Using browser requests method")
                response = asyncio.run(self._browser_requests(*url_args))

            response = response if isinstance(response, str) else response.text

            soup = BeautifulSoup(response, "html.parser")
            element = soup.find(html_tag, tag_params)

            logger.info(f"Found element: {element is not None}")
            
            text = element.get_text(strip=True)
            if regex:
                logger.info(f"Using regex: {regex} on text: {text}")
                match = re.search(regex, text)
                count = int(match.group(1)) if match else 1
                total_pages, total_items =return_calc(count, items_per_page, item_oriented)
            else:
                logger.info(f"Extracting numeric value from text: {text} using default method")
                count = int(re.sub(r'[^\d]', '', text)) if text else 1
                total_pages, total_items =return_calc(count, items_per_page, item_oriented)
            
            return {
                'items_per_page': items_per_page, 
                'total_items': total_items, 
                'total_pages': total_pages, 
                'item_tag_text': text,
                'orientation': 'items' if item_oriented else 'pages'
            }

        except Exception as e:
            logger.error(f"[Error] get_total_pages failed: {e}")
            return {
                'items_per_page': items_per_page, 
                'total_items': None, 
                'total_pages': 1, 
                'item_tag_text': None,
                'orientation': 'items' if item_oriented else 'pages'
            }