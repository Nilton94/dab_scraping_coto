from ...utils.json import reader
from ...utils.literals import URLTYPE, FOLDERTYPE
from ..staging.extract_urls import CotoScraperTotalItems, CotoScraper
from pyspark.sql import SparkSession
from ...utils.logger import get_logger
from pyspark.sql.functions import *
from ...utils.dbutils import get_dbutils

dbutils = get_dbutils()
logger = get_logger()

async def load_to_bronze(spark: SparkSession, type: URLTYPE = 'department', catalog: str = 'main', schema: str = 'default', table: str = 'test'):
    """ 
        Load data from CotoScraper into a Bronze table in the specified catalog, schema, and table.
        Args:
            spark (SparkSession): Active Spark session.
            type (URLTYPE): Type of URL to load ('department', 'category', or 'subcategory').
            catalog (str): Target catalog.
            schema (str): Target schema.
            table (str): Target table name.
        Returns:
            None
        Example:
            >>> await load_to_bronze(spark, type='department', catalog='main', schema='default', table='departments')
    """
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

def volumes_to_bronze(spark: SparkSession, type: FOLDERTYPE = "departments", catalog: str = "main", schema: str = "default", table: str = "test", volume: str = "staging", file_format: str = "json", mode: str = "append"):
    """
        Load raw files from a Unity Catalog volume and save them into a Bronze table.

        Args:
            spark (SparkSession): Active Spark session.
            type (str): Folder inside the volume (e.g. departments, categories).
            catalog (str): Target catalog.
            schema (str): Target schema.
            table (str): Target table name.
            volume (str): Source volume name.
            file_format (str): File format in the volume (json, parquet, csv...).
            mode (str): Write mode for the table ("append" or "overwrite").
        Returns:
            None
        Example:
            >>> volumes_to_bronze(spark, type="departments", catalog="main", schema="default", table="departments_bronze", volume="staging", file_format="json", mode="append")
            >>> volumes_to_bronze(spark, type="categories", catalog="main", schema="default", table="categories_bronze", volume="staging", file_format="parquet", mode="
    """

    logger.info(f"Starting saving data for table {catalog}.{schema}.{table}!")
    base_path = f"/Volumes/{catalog}/{schema}/{volume}/{type}"
    schema_path = f"{base_path}/schema"
    checkpoint_path = f"{base_path}/checkpoints"
    files_path = f"{base_path}/files"

    try:
        # Garantindo que os diretórios existem
        dbutils.fs.mkdirs(schema_path)
        dbutils.fs.mkdirs(checkpoint_path)
        dbutils.fs.mkdirs(files_path)

        # Usando Auto Loader para ler os dados de forma incremental, mantendo processamento em batch (trigger once)
        df = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", file_format)
            .option("multiLine", "true")
            # .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaLocation", schema_path)
            .option("cloudFiles.schemaEvolutionMode", "rescue")
            # .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .load(files_path)
        )

        # Escrevendo tabela Delta usando trigger once, que realiza o processo em batch
        (
            df.writeStream
            .format("delta")
            .option("checkpointLocation", checkpoint_path)
            .option("delta.enableChangeDataFeed", "true")
            .trigger(once=True)
            .table(f"{catalog}.{schema}.{table}")
        )

        logger.info(f"Saved data to {catalog}.{schema}.{table} as stream!")

    except Exception as e:
        logger.error(f"Failed to load or save data from {base_path}: {e}")
        raise