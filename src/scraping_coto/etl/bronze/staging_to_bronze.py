from ...utils.json import reader
from ...utils.literals import URLTYPE
from .....scratch.extract_urls import CotoScraperTotalItems, CotoScraper
from pyspark.sql import SparkSession

async def load_to_bronze(spark: SparkSession, type: URLTYPE = 'department', catalog: str = 'main', schema: str = 'default', table: str = 'test'):
    
    if type not in ['department','category','subcategory']:
        return 'Error: type must be department, category or subcategory'
    
    elif type == 'department':
        scraper = CotoScraper()
        data = scraper.parse_base_json()
        df = spark.createDataFrame(data)
        try:
            (
                df
                .write
                .mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(f"{catalog}.{schema}.{table}")
            )
        except:
            (
                df
                .write
                .mode("overwrite")
                .option("mergeSchema", "true")
                .saveAsTable(f"{catalog}.{schema}.{table}")
            )

    else:
        scraper = CotoScraperTotalItems()
        data = await scraper.parse_item_json(prefix=type)
        df = spark.createDataFrame(data)
        try:
            (
                df
                .write
                .mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(f"{catalog}.{schema}.{table}")
            )
        except:
            (
                df
                .write
                .mode("overwrite")
                .option("mergeSchema", "true")
                .saveAsTable(f"{catalog}.{schema}.{table}")
            )