# Imports
import sys
import os

current_dir = os.path.dirname(os.path.abspath(os.getcwd()))
src_dir = os.path.dirname(os.path.dirname(current_dir)) + '/src'
sys.path.insert(0, src_dir)

from modules.utils.dbutils import get_dbutils
from modules.utils.spark_session import get_spark
from modules.utils.logger import get_logger
from modules.utils.json import loader
from scraping_lagallega.utils.scraper import ScrapingLaGallega

dbutils = get_dbutils()
spark = get_spark()
logger = get_logger()

# Params
dbutils.widgets.text(name='target', defaultValue='dev')
bundle_target = dbutils.widgets.get("target")

target = 'lagallega' #if bundle_target == 'prod' else f'lagallega_{bundle_target}'
attributes_path = f'/Volumes/{target}/bronze/staging/attributes'
products_path = f'/Volumes/{target}/bronze/staging/products/files'


# Scraper
scraper = ScrapingLaGallega()

# Generate Attributes
try:
    logger.info("Generating attributes...")
    logger.info(f"Target path: {attributes_path}")
    
    attributes = scraper.generate_categories_attributes(save_to_file=False)
    
    logger.info(f"Attributes generated: {len(attributes)}")
    
except Exception as e:
    logger.error(f"Error generating attributes: {e}")
    raise

logger.info(f"Attributes generated: {len(attributes)}")
logger.info("Saving attributes to volume...")

loader(
    file=attributes,
    file_path=attributes_path,
    file_name='attributes'
)

attributes_enriched = scraper.generate_categories_pages_html(save_to_file=True, volume_path=attributes_path)

# Generate Products
logger.info("Generating products...")
products = scraper.process_products(save_to_file=True, volume_path=products_path)