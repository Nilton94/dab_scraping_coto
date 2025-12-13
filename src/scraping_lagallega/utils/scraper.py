from bs4 import BeautifulSoup
import re
import os
import json
import datetime
import pytz
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from modules.scrapers.base_scraper import BaseScraper
from modules.utils.retry import retry

try:
    from modules.utils.logger import get_logger
    logger = get_logger()
except:
    import logging
    logger = logging.getLogger(__name__)

class ScrapingLaGallega(BaseScraper):

    BASE_URL = "https://www.lagallega.com.ar/productosnl.asp?pg={pg}&nl={category_nl}000000"
    BASE_URL_OFERTA = "https://www.lagallega.com.ar/productosnl.asp?pg={pg}&nl=&TM=CM"
    
    def __init__(self, **kwargs):
        kwargs.setdefault('domain', 'lagallega.com.ar')
        kwargs.setdefault('required_string', 'Agregar')
        super().__init__(**kwargs)

    @retry(retries=15, delay=2)
    def generate_categories_attributes(self, save_to_file: bool = True):
        """ 
            Generate categories and attributes from La Gallega website.
        """
        
        logger.info("Generating categories and attributes from La Gallega")

        current_dir = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.join(os.path.dirname(current_dir), 'data')
        os.makedirs(data_dir, exist_ok=True)
        file_path = os.path.join(data_dir, 'attributes.json')
        
        # dt_str = str(datetime.datetime.now(tz=pytz.timezone('America/Sao_Paulo')).date())
        # file_path = os.path.join(volume_path, dt_str + '_attributes.json') if volume_path else os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'attributes.json')

        if os.path.exists(file_path):
            logger.info(f"File {file_path} already exists. Keeping existing file.")
            return json.load(open(file_path, 'r', encoding='latin-1'))

        try:
            html = self.get_html()
            soup = BeautifulSoup(html, 'html.parser')
            data = []
            categories_section = soup.find('div', class_='TitPie', string='Categorias')

            logger.info(f"Found categories section: {categories_section is not None}")

            if categories_section is None:
                logger.error("No categories section found on the page.")
                raise ValueError("No data found")
            
            elif categories_section:
                logger.info("Categories section found, processing categories.")
                parent_div = categories_section.find_next('div', id='Categorias')
                if parent_div:
                    links = parent_div.find_all('a')
                    logger.info(f"Found {len(links)} category links.")

                    for link in links:
                        logger.info(f"Processing category link: {link.get('href')}")

                        pattern = r'nl=(\d+)'
                        match = re.search(pattern, link.get('href'))

                        if match:
                            category_nl = match.group(1)
                        else:
                            category_nl = None

                        data.append(
                            {
                                'category': link.get_text(strip=True),
                                'full_attribute': link.get('href'),
                                'category_nl': category_nl,
                                'category_nl_full': f"{category_nl}000000" if category_nl else None,
                                'category_url': (
                                    self.BASE_URL.format(pg=1, category_nl=category_nl) 
                                    if category_nl 
                                    else self.BASE_URL_OFERTA.format(pg=1) 
                                )
                            }
                        )

                        logger.info(f"Added category: {data[-1]}")

                
                logger.info(f"Total categories processed: {len(data)}")
                logger.info("Saving categories and attributes to attributes.json if no exists.")
                
                if save_to_file:
                    with open(file_path, 'w', encoding='utf-8') as f: 
                        logger.info(f"Writing data to {file_path}")
                        json.dump(data, f, indent=2, ensure_ascii=False)
                
                logger.info("Categories and attributes saved successfully.")
                return data
        except Exception as e:
            logger.error(f"An error occurred while generating categories and attributes: {e}")
            raise

    @retry(retries=15, delay=2)
    def generate_categories_urls(self, save_to_file=True, volume_path: str = None):
        """
            Generate all paginated URLs for each category and enrich the attributes data.
            
            Args:
                save_to_file (bool): Whether to save the enriched data to a JSON file.
            
            Returns:
                list: Enriched list of categories with URL information.
        """
        
        logger.info("Generating all category URLs from attributes.json")

        dt_str = str(datetime.datetime.now(tz=pytz.timezone('America/Sao_Paulo')).date())
        file_path = os.path.join(volume_path, dt_str + '_attributes_enriched.json') if volume_path else os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'attributes_enriched.json')

        # current_dir = os.path.dirname(os.path.abspath(__file__))
        # data_dir = os.path.join(os.path.dirname(current_dir), 'data')
        # os.makedirs(data_dir, exist_ok=True)
        # file_path = os.path.join(data_dir, 'attributes_enriched.json')

        if os.path.exists(file_path):
            logger.info(f"File {file_path} already exists. Keeping existing file.")
            return json.load(open(file_path, 'r', encoding='latin-1'))
        
        initial_data = self.generate_categories_attributes()
        
        # Store original values to restore later
        original_item = self.item
        original_query_keys = self.query_keys.copy()
        original_kwargs = self.kwargs.copy()

        # self.required_string = 'Agregar'
        
        for category_data in initial_data:
            try:
                category_name = category_data['category']
                category_nl = category_data['category_nl']
                
                logger.info(f"Generating URLs for category: {category_name} (nl={category_nl})")
                
                # Modify instance attributes for this category
                self.item = "productosnl.asp"
                
                if category_nl:
                    self.query_keys = ["pg", "nl"]
                    self.kwargs = {
                        'pg': 1,
                        'nl': f"{category_nl}000000"
                    }
                else:
                    # Ofertas category
                    self.query_keys = ["pg", "TM"]
                    self.kwargs = {
                        'pg': 1,
                        'TM': "CM"
                    }
                
                # Generate URLs using the modified instance
                urls = self.get_all_urls(
                    page_param_name='pg',
                    items_per_page=20,
                    html_tag='td',
                    tag_params={'class': 'pieBot'},
                    regex=r'de\s*(\d+)',
                    item_oriented=False
                )
                
                category_data['urls'] = urls
                category_data['total_pages'] = len(urls)
                
                logger.info(f"Generated {len(urls)} URLs for {category_name}")
            except Exception as e:
                logger.error(f"Error generating URLs for category {category_name}: {e}")
                category_data['urls'] = []
                category_data['total_pages'] = 0
        
        # Restore original values
        self.item = original_item
        self.query_keys = original_query_keys
        self.kwargs = original_kwargs
        
        # Save if requested
        if save_to_file:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(initial_data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Saved enriched category data to {file_path}")
        
        return initial_data
    
    @retry(retries=15, delay=2)
    def generate_categories_pages_html(self, save_to_file=True, volume_path: str = None, max_workers: int = 5):
        """
            Generate HTML content for all category URLs and enrich the data.
            Uses ThreadPoolExecutor for concurrent fetching (faster and safer).
            
            Args:
                save_to_file (bool): Whether to save the enriched data to a JSON file.
                volume_path (str): Optional volume path for saving files.
                max_workers (int): Maximum number of concurrent threads (default: 5).
            
            Returns:
                list: Enriched list of categories with HTML content for each URL.
        """
        
        logger.info(f"Generating HTML pages for all category URLs (concurrent mode with {max_workers} workers)")

        dt_str = str(datetime.datetime.now(tz=pytz.timezone('America/Sao_Paulo')).date())
        file_path = os.path.join(volume_path, dt_str + '_attributes_enriched_with_html.json') if volume_path else os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'attributes_enriched_with_html.json')

        if os.path.exists(file_path):
            logger.info(f"File {file_path} already exists. Keeping existing file.")
            return json.load(open(file_path, 'r', encoding='utf-8'))
        
        # Get enriched data with URLs
        initial_data = self.generate_categories_urls(save_to_file=False)
        
        # Store original values to restore later
        original_item = self.item
        original_query_keys = self.query_keys.copy()
        original_kwargs = self.kwargs.copy()
        
        for category_data in initial_data:
            try:
                category_name = category_data['category']
                category_nl = category_data['category_nl']
                urls = category_data.get('urls', [])
                
                logger.info(f"Fetching HTML for {len(urls)} pages in category: {category_name}")
                
                # Modify instance attributes for this category
                self.item = "productosnl.asp"
                
                if category_nl:
                    self.query_keys = ["pg", "nl"]
                    base_kwargs = {
                        'pg': 1,
                        'nl': f"{category_nl}000000"
                    }
                else:
                    # Ofertas category
                    self.query_keys = ["pg", "TM"]
                    base_kwargs = {
                        'pg': 1,
                        'TM': "CM"
                    }
                
                # Function to fetch a single page (thread-safe)
                def fetch_page(idx_and_url):
                    idx, url = idx_and_url
                    try:
                        # Make direct HTTP request without modifying shared state
                        headers = {'User-Agent': self.user_agent}
                        response = requests.get(url, headers=headers)
                        response.raise_for_status()
                        html = response.text
                        
                        logger.info(f"Successfully fetched {category_name} - Page {idx}/{len(urls)}")
                        
                        return {
                            'url': url,
                            'html': html,
                            'page_number': idx
                        }
                        
                    except Exception as e:
                        logger.error(f"Error fetching {url}: {e}")
                        return {
                            'url': url,
                            'html': None,
                            'error': str(e),
                            'page_number': idx
                        }
                
                # Fetch pages concurrently using ThreadPoolExecutor
                start_time = time.time()
                url_pages = []
                
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    # Submit all fetch tasks
                    futures = {
                        executor.submit(fetch_page, (idx, url)): idx 
                        for idx, url in enumerate(urls, start=1)
                    }
                    
                    # Collect results as they complete
                    for future in as_completed(futures):
                        try:
                            result = future.result()
                            url_pages.append(result)
                        except Exception as e:
                            idx = futures[future]
                            logger.error(f"Error getting result for page {idx}: {e}")
                            url_pages.append({
                                'url': urls[idx-1] if idx <= len(urls) else '',
                                'html': None,
                                'error': str(e),
                                'page_number': idx
                            })
                
                # Sort by page number to maintain order
                url_pages.sort(key=lambda x: x.get('page_number', 999999))
                
                # Remove page_number field before storing
                for page in url_pages:
                    page.pop('page_number', None)
                
                elapsed = time.time() - start_time
                logger.info(f"Fetched {len(url_pages)} pages for {category_name} in {elapsed:.2f}s")
                
                # Add url_pages to category_data
                category_data['url_pages'] = url_pages
                
            except Exception as e:
                logger.error(f"Error processing category {category_name}: {e}")
                category_data['url_pages'] = []
        
        # Restore original values
        self.item = original_item
        self.query_keys = original_query_keys
        self.kwargs = original_kwargs
        
        # Save enriched data to JSON file if requested
        if save_to_file:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(initial_data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Saved categories with HTML content to {file_path}")
        
        return initial_data

    def _extract_products_from_html(self, html: str) -> list:
        """
            Extract product information from La Gallega HTML.
            
            Args:
                html (str): HTML content containing product listings.
            
            Returns:
                list: List of dictionaries with product information.
        """

        logger.info("Extracting products from HTML content")
        
        soup = BeautifulSoup(html, 'html.parser')
        products = []
        
        # Find all product items
        product_items = soup.find_all('li', class_='cuadProd')
        
        logger.info(f"Found {len(product_items)} products in HTML")
        
        for item in product_items:
            logger.info(f"Processing product {item.find('a', href=re.compile(r'productosdet\.asp')).get('href', '')}")

            try:
                # Extract product ID from detail link
                detail_link = item.find('a', href=re.compile(r'productosdet\.asp'))
                product_id = None
                detail_url = None
                if detail_link:
                    detail_url = detail_link.get('href', '')
                    match = re.search(r'Pr=(\d+)', detail_url)
                    if match:
                        product_id = match.group(1)
                
                # Extract image URL and barcode from alt text
                img = item.find('img')
                image_url = img.get('src', '') if img else None
                barcode = None
                if img and img.get('alt'):
                    alt_text = img.get('alt', '')
                    # Barcode is the first part before the dash
                    parts = alt_text.split(' - ', 1)
                    if len(parts) == 2:
                        barcode = parts[0].strip()
                
                # Extract description
                desc_div = item.find('div', class_='desc')
                description = desc_div.get_text(strip=True) if desc_div else None
                
                # Extract price
                precio_div = item.find('div', class_='precio')
                price = None
                if precio_div:
                    price_text = precio_div.get_text(strip=True)
                    # Remove $ and convert to float
                    price_clean = re.sub(r'[^\d,]', '', price_text).replace(',', '.')
                    try:
                        price = float(price_clean)
                    except ValueError:
                        price = None
                
                # Check if product is on offer
                is_offer = item.find('div', class_='OferProd') is not None
                
                # Build product dictionary
                product = {
                    'product_id': product_id,
                    'barcode': barcode,
                    'description': description,
                    'price': price,
                    'price_text': precio_div.get_text(strip=True) if precio_div else None,
                    'image_url': image_url,
                    'detail_url': detail_url,
                    'is_offer': is_offer,
                    'timestamp': str(datetime.datetime.now(tz=pytz.timezone('America/Sao_Paulo'))),
                    'timezone': 'America/Sao_Paulo'
                }
                
                products.append(product)
                
            except Exception as e:
                logger.error(f"Error Processing product {item.find('a', href=re.compile(r'productosdet\.asp')).get('href', '')}: \n {e}")
                continue
        
        logger.info(f"Successfully extracted {len(products)} products")
        return products
    
    def process_products(self, save_to_file=True, max_workers=10, volume_path: str = None):
        """
            Process all HTML pages to extract products using ThreadPoolExecutor.
            
            Args:
                save_to_file (bool): Whether to save the enriched data to a JSON file.
                max_workers (int): Maximum number of threads for parallel processing.
            
            Returns:
                list: Complete enriched data with extracted products.
        """
        
        logger.info("Processing all HTML pages to extract products")

        dt_str = str(datetime.datetime.now(tz=pytz.timezone('America/Sao_Paulo')).date())
        file_path = os.path.join(volume_path, dt_str + '_complete_data.json') if volume_path else os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'complete_data.json')

        # current_dir = os.path.dirname(os.path.abspath(__file__))
        # data_dir = os.path.join(os.path.dirname(current_dir), 'data')
        # os.makedirs(data_dir, exist_ok=True)
        # file_path = os.path.join(data_dir, 'complete_data.json')

        if os.path.exists(file_path):
            logger.info(f"File {file_path} already exists. Keeping existing file.")
            return json.load(open(file_path, 'r', encoding='utf-8'))
        
        # Get data with HTML
        data_with_html = self.generate_categories_pages_html(save_to_file=False)
        
        # Track overall progress
        total_pages = sum(len(cat.get('url_pages', [])) for cat in data_with_html)
        processed_pages = 0
        
        logger.info(f"Starting product extraction from {total_pages} pages using {max_workers} workers")
        
        for category_data in data_with_html:
            category_name = category_data['category']
            url_pages = category_data.get('url_pages', [])
            
            if not url_pages:
                logger.warning(f"No pages found for category: {category_name}")
                continue
            
            logger.info(f"Processing {len(url_pages)} pages for category: {category_name}")
            
            # Function to process a single page
            def process_page(page_data):
                try:
                    url = page_data.get('url')
                    html = page_data.get('html')
                    
                    if not html:
                        logger.warning(f"No HTML found for URL: {url}")
                        return {**page_data, 'products': [], 'product_count': 0}
                    
                    # Extract products from HTML
                    products = self._extract_products_from_html(html)
                    
                    return {
                        **page_data,
                        'products': products,
                        'product_count': len(products)
                    }
                    
                except Exception as e:
                    logger.error(f"Error processing page {page_data.get('url')}: {e}")
                    return {
                        **page_data,
                        'products': [],
                        'product_count': 0,
                        'processing_error': str(e)
                    }
            
            # Process pages in parallel using ThreadPoolExecutor
            processed_url_pages = []
            start_time = time.time()
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all tasks
                future_to_page = {
                    executor.submit(process_page, page_data): idx 
                    for idx, page_data in enumerate(url_pages)
                }
                
                # Collect results as they complete
                for future in as_completed(future_to_page):
                    page_idx = future_to_page[future]
                    try:
                        result = future.result()
                        processed_url_pages.append(result)
                        processed_pages += 1
                        
                        # Log progress
                        if processed_pages % 10 == 0 or processed_pages == total_pages:
                            elapsed = time.time() - start_time
                            rate = processed_pages / elapsed if elapsed > 0 else 0
                            logger.info(
                                f"Progress: {processed_pages}/{total_pages} pages "
                                f"({processed_pages/total_pages*100:.1f}%) - "
                                f"Rate: {rate:.1f} pages/sec"
                            )
                            
                    except Exception as e:
                        logger.error(f"Error getting result for page {page_idx}: {e}")
                        processed_url_pages.append({
                            'url': url_pages[page_idx].get('url'),
                            'products': [],
                            'error': str(e)
                        })
            
            # Sort by original order (ThreadPoolExecutor may complete out of order)
            processed_url_pages.sort(
                key=lambda x: next(
                    (i for i, p in enumerate(url_pages) if p.get('url') == x.get('url')), 
                    999999
                )
            )
            
            # Update category data with processed pages
            category_data['url_pages'] = processed_url_pages
            
            # Calculate total products for this category
            total_products = sum(page.get('product_count', 0) for page in processed_url_pages)
            category_data['total_products'] = total_products
            
            elapsed = time.time() - start_time
            logger.info(
                f"Completed {category_name}: {len(processed_url_pages)} pages, "
                f"{total_products} products in {elapsed:.2f}s"
            )
        
        # Calculate overall statistics
        total_products = sum(cat.get('total_products', 0) for cat in data_with_html)
        total_elapsed = time.time() - start_time
        
        logger.info(
            f"Product extraction completed: {total_pages} pages, "
            f"{total_products} products in {total_elapsed:.2f}s"
        )
        
        # Save complete data to JSON file if requested
        if save_to_file:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data_with_html, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Saved complete data to {file_path}")
        
        return data_with_html