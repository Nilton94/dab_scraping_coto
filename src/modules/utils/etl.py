from modules.utils.literals import FOLDERTYPE
from pyspark.sql import SparkSession
from modules.utils.logger import get_logger
from pyspark.sql.functions import *
from modules.utils.dbutils import get_dbutils

dbutils = get_dbutils()
logger = get_logger()


def volumes_to_bronze(
        spark: SparkSession,
        type: str = None,
        catalog: str = "main",
        schema: str = "default",
        table: str = "test",
        volume: str = "staging",
        file_format: str = "json"
    ):
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
        Returns:
            None
        Example:
            >>> volumes_to_bronze(spark, type="departments", catalog="main", schema="default", table="departments_bronze", volume="staging", file_format="json")
            >>> volumes_to_bronze(spark, type="categories", catalog="main", schema="default", table="categories_bronze", volume="staging", file_format="parquet")
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

def load_to_silver(
        spark: SparkSession,
        target_catalog: str, 
        target_schema: str, 
        target_table: str, 
        source_catalog: str, 
        source_schema: str, 
        source_table: str, 
        transform_function=None,
        partition_by: list = None, 
        options: dict = {}
    ):
    """
        Load data from bronze to silver layer with transformations.
        Args:
            target_catalog (str): Target catalog for silver layer.
            target_schema (str): Target schema for silver layer.
            target_table (str): Target table for silver layer.
            source_catalog (str): Source catalog for bronze layer.
            source_schema (str): Source schema for bronze layer.
            source_table (str): Source table for bronze layer.
            partition_by (list, optional): List of columns to partition the target table by. Defaults to None.
            options (dict, optional): 
                Additional options for writing the data. Defaults to an empty dictionary. 
                These are passed to the DataFrameWriter options. Which can include options like:
                - "maxFilesPerTrigger": Limits the number of files processed in each trigger.
        Returns:
            DataFrame: Transformed DataFrame written to silver layer.
        Example:
            >>> load_to_silver(target_catalog="main", target_schema="silver", target_table="departments", source_catalog="main", source_schema="bronze", source_table="departments_bronze", partition_by=["country"], options={"maxFilesPerTrigger": 10})
            >>> load_to_silver(target_catalog="main", target_schema="silver", target_table="items", source_catalog="main", source_schema="bronze", source_table="items_bronze")
    """

    source_table_path = f"{source_catalog}.{source_schema}.{source_table}"
    target_table_path = f"{target_catalog}.{target_schema}.{target_table}"
    checkpoint_path = f"/Volumes/{target_catalog}/{target_schema}/checkpoints/{target_table}"

    logger.info(f"Starting saving data from table {source_table_path} to {target_table_path}, using streaming!")

    df = spark.readStream.format("delta").table(source_table_path)
    
    logger.info(f"Checkpoint path: {checkpoint_path}")

    # Garantindo que os diretórios existem
    dbutils.fs.mkdirs(checkpoint_path)

    df_transformed = transform_function(df) if transform_function else df
    
    logger.info(f"Writing transformed data to silver table {target_table_path}...")
    
    writer = (
        df_transformed
        .writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
    )
    
    if partition_by:
        logger.info(f"Partitioning by columns: {partition_by}")
        writer = writer.partitionBy(partition_by)
    
    for option_key, option_value in options.items():
        logger.info(f"Setting writer option: {option_key} = {option_value}")
        writer = writer.option(option_key, option_value)
    
    logger.info(f"Starting the streaming write operation for table {target_table_path}...")
    silver_table = (
        writer
        .trigger(availableNow=True)
        .table(target_table_path)
    )

    silver_table.awaitTermination()
    logger.info(f"Successfully completed streaming write to {target_table_path}")

    try:
        result_count = spark.table(target_table_path).count()
        logger.info(f"Silver table {target_table_path} now has {result_count} rows")
    except Exception as e:
        logger.warning(f"Could not verify row count: {e}")
    
    return silver_table