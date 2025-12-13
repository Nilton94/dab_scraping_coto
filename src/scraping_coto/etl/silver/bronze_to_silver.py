from .transformations.departments import transform_departments
from .transformations.categories import transform_categories
from .transformations.subcategories import transform_subcategories
from .transformations.items import transform_items
from modules.utils.logger import get_logger
from modules.utils.spark_session import get_spark 
from modules.utils.dbutils import get_dbutils

spark = get_spark()
dbutils = get_dbutils()
logger = get_logger()

def load_to_silver(target_catalog: str, target_schema: str, target_table: str, source_catalog: str, source_schema: str, source_table: str, partition_by: list = None, options: dict = {}):
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

    if target_table == "departments":
        logger.info("Transforming departments data...")
        df_transformed = transform_departments(df)
    
    elif target_table == "categories":
        logger.info("Transforming categories data...")
        df_transformed = transform_categories(df)
    
    elif target_table == "subcategories":
        logger.info("Transforming subcategories data...")
        df_transformed = transform_subcategories(df)
    
    elif target_table == "items":
        logger.info("Transforming items data...")
        df_transformed = transform_items(df)
    
    else:
        raise ValueError(f"Unknown table: {target_table}")
    
    logger.info(f"Writing transformed data to silver table {target_table_path}...")
    
    writer = df_transformed.writeStream.format("delta").option("checkpointLocation", checkpoint_path).outputMode("append")
    
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
    
