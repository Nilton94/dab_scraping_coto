import os, sys 
from bs4 import BeautifulSoup

if os.path.abspath(os.path.join(os.getcwd(), '..')) in sys.path: pass 
else: sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '..')))

from scraping_lagallega.utils.scraper import ScrapingLaGallega

# ScrapingLaGallega().generate_categories_attributes()

# html = ScrapingLaGallega(
#     item="productosnl.asp",
#     query_keys=["pg", "nl"],
#     pg=1,  
#     nl="03000000"
# ).get_html()

# total_page = BeautifulSoup(html, 'html.parser').find('td', class_='pieBot').text.strip()

# print(total_page)

# scraper = ScrapingLaGallega(
#     item="productosnl.asp",
#     query_keys=["TM"],
#     TM="CM"
# )

# items = scraper._get_total_pages(
#     items_per_page=20,
#     html_tag='td',
#     tag_params={'class': 'pieBot'},
#     regex=r'de\s*(\d+)'
# )

# urls = scraper.get_all_urls(
#     page_param_name='pg',
#     items_per_page=20,
#     html_tag='td',
#     tag_params={'class': 'pieBot'},
#     regex=r'de\s*(\d+)',
#     item_oriented=False
# )

# print(
#     scraper.get_url(),
#     BeautifulSoup(scraper.get_html(), 'html.parser').find('td', class_='pieBot').text.strip(),
#     sep="\n"
# )

scraper = ScrapingLaGallega(required_string='Agregar')

# scraper.generate_categories_urls(save_to_file=True)
# scraper.generate_categories_pages_html(save_to_file=True)
scraper.process_products()