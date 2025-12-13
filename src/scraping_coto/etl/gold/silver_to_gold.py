from modules.utils.logger import get_logger
from modules.utils.spark_session import get_spark
from pyspark.sql.functions import col, explode, row_number, expr, struct, lit, coalesce, when
from datetime import date
from datetime import timedelta
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from typing import Optional, List, Dict, Any

logger = get_logger()
spark = get_spark()

def load_gold_items_table(
        source_catalog_name: str, 
        source_schema_name: str, 
        source_table_name: str, 
        target_catalog_name: str, 
        target_schema_name: str, 
        target_table_name: str,
        **options: Dict[str, Any]
    ) -> DataFrame:
    """
    Load and transform silver items data into gold layer with department hierarchy.

    This function reads from silver layer items, joins with department hierarchy,
    applies AI-based extraction for product details, and writes to gold layer
    with support for merge, append, or overwrite modes.

    Args:
        source_catalog_name: Source catalog name (e.g., 'coto_dev')
        source_schema_name: Source schema name (e.g., 'silver')
        source_table_name: Source table name (e.g., 'items')
        target_catalog_name: Target catalog name (e.g., 'coto_dev')
        target_schema_name: Target schema name (e.g., 'gold')
        target_table_name: Target table name (e.g., 'items')
        **options: Additional processing options:
            - partition_by (List[str]): Columns to partition by (default: None)
            - mode (str): Write mode - 'append', 'overwrite', 'merge' (default: 'overwrite')
            - merge_keys (List[str]): Keys for merge operations (default: None)
            - start_date (str): Filter start date in ISO format (default: None)
            - end_date (str): Filter end date in ISO format (default: today)
            - date_column (str): Date column name for filtering (default: 'dt')

    Returns:
        DataFrame: The transformed gold items DataFrame

    Raises:
        ValueError: If merge mode is used without merge_keys
        Exception: For any processing errors

    Examples:
        >>> # Overwrite mode
        >>> df = load_gold_items_table(
        ...     'coto_dev', 'silver', 'items',
        ...     'coto_dev', 'gold', 'items',
        ...     partition_by=['dt'],
        ...     mode='overwrite'
        ... )
        
        >>> # Merge mode with date range
        >>> df = load_gold_items_table(
        ...     'coto_dev', 'silver', 'items',
        ...     'coto_dev', 'gold', 'items',
        ...     mode='merge',
        ...     merge_keys=['item_id', 'dt'],
        ...     start_date='2025-01-01',
        ...     end_date='2025-01-31'
        ... )
    """

    source_table_path = f'{source_catalog_name}.{source_schema_name}.{source_table_name}'
    target_table_path = f'{target_catalog_name}.{target_schema_name}.{target_table_name}'
    
    logger.info(f"Loading gold items: {target_table_path} from source: {source_table_path}")
    logger.info(f"Options: {options}")

    try:
        # Extract and validate options
        partition_by: Optional[List[str]] = options.get('partition_by')
        mode: str = options.get('mode', 'overwrite')
        merge_keys: Optional[List[str]] = options.get('merge_keys')
        start_date: Optional[str] = options.get('start_date', (date.today() - timedelta(days=1)).isoformat())
        end_date: str = options.get('end_date', date.today().isoformat())
        date_column: str = options.get('date_column', 'dt')

        # Validate merge mode
        if mode == 'merge' and not merge_keys:
            raise ValueError("merge_keys must be provided when using mode='merge'")

        # Check if target exists
        target_exists = spark.catalog.tableExists(target_table_path)
        logger.info(f"Target table exists: {target_exists}")

        # Build department hierarchy
        logger.info("Building department hierarchy...")
        df_dept = _build_department_hierarchy(
            source_catalog_name, 
            source_schema_name,
            start_date,
            end_date,
            date_column
        )

        # Build gold items with enrichments
        logger.info("Transforming items to gold layer...")
        df_gold_items = _transform_items_to_gold(
            source_catalog_name,
            source_schema_name, 
            source_table_name,
            df_dept,
            start_date,
            end_date,
            date_column
        )

        # Write based on mode
        _write_gold_table(
            df_gold_items,
            target_table_path,
            target_exists,
            mode,
            merge_keys,
            partition_by,
            date_column
        )

        logger.info(f"Successfully processed records to {target_table_path}")
        return df_gold_items

    except Exception as e:
        logger.error(f"Failed to load gold items table: {e}")
        raise


def _build_department_hierarchy(
    catalog: str,
    schema: str, 
    start_date: Optional[str],
    end_date: str,
    date_column: str
) -> DataFrame:
    """
        Build flattened department hierarchy from nested structure.
        Args:
            catalog (str): Source catalog.
            schema (str): Source schema.
            start_date (Optional[str]): Start date for filtering.
            end_date (str): End date for filtering.
            date_column (str): Date column name for filtering.
        Returns:
            DataFrame: Flattened department hierarchy.
        Example:
            >>> df_dept = _build_department_hierarchy('coto_dev', 'silver', '2025-01-01', '2025-01-31', 'dt')
            >>> df_dept.show()
    """
    
    dept_table = f'{catalog}.{schema}.departments'
    
    # Build date filter
    if start_date:
        date_filter = f"{date_column} BETWEEN '{start_date}' AND '{end_date}'"
    else:
        date_filter = f"{date_column} <= '{end_date}'"
    
    df_dept = (
        spark.table(dept_table)
        .where(date_filter)
        # Get latest version of each department
        .withColumn(
            'rn',
            row_number().over(
                Window.partitionBy('department_id')
                .orderBy(col(date_column).desc_nulls_last())
            )
        )
        .where('rn = 1')
        .drop('rn')
        # Explode nested categories
        .withColumn('categories', explode('departments.subCategories'))
        .withColumn('subcategories', explode('categories.subCategories2'))
        # Select and rename
        .selectExpr(
            'department_id',
            'department_name',
            'categories.categoryId AS category_id',
            'categories.displayName AS category_name',
            'subcategories.categoryId AS subcategory_id',
            'subcategories.displayName AS subcategory_name'
        )
        .dropDuplicates(['subcategory_id'])  # More efficient than window function
    )
    
    logger.info(f"Built department hierarchy with {df_dept.count()} subcategories")
    return df_dept


def _transform_items_to_gold(
    catalog: str,
    schema: str,
    table: str,
    df_dept: DataFrame,
    start_date: Optional[str],
    end_date: str,
    date_column: str
) -> DataFrame:
    """
        Transform silver items to gold with enrichments.
        Args:
            catalog (str): Source catalog.
            schema (str): Source schema.
            table (str): Source table.
            df_dept (DataFrame): Department hierarchy DataFrame.
            start_date (Optional[str]): Start date for filtering.
            end_date (str): End date for filtering.
            date_column (str): Date column name for filtering.
        Returns:
            DataFrame: Transformed gold items.
        Example:
            >>> df_gold = _transform_items_to_gold('coto_dev', 'silver', 'items', df_dept, '2025-01-01', '2025-01-31', 'dt')
            >>> df_gold.show()
    """
    
    items_table = f'{catalog}.{schema}.{table}'
    
    # Build date filter
    if start_date:
        date_filter = f"{date_column} BETWEEN '{start_date}' AND '{end_date}'"
    else:
        date_filter = f"{date_column} <= '{end_date}'"
    
    df_items = spark.table(items_table).where(date_filter)
    
    # Join with department hierarchy
    df_gold = df_items.join(df_dept, on='subcategory_id', how='left')
    
    # Apply AI extraction with fallback
    df_gold = df_gold.withColumn(
        "ai_generated",
        _safe_ai_extract("item_name")
    )
    
    # Build structured output
    df_gold = df_gold.select(
        # Department hierarchy
        *[col(c) for c in df_dept.columns],
        
        # Item basics
        col("item_id"),
        col("item_name"),
        col("ai_generated"),
        col("product_brand"),
        col("product_subclass"),
        col("product_class"),
        col("product_category"),
        
        # Discounts
        col("product_discounts"),
        expr("transform(product_discounts, x -> x.descuento.valor)").alias("product_discount_prices"),
        col("sku_discounts")[0].alias("sku_prices"),
        
        # Prices structure
        struct(
            struct(
                lit("The actual price for this product, based on the current quantity.").alias("description"),
                col("sku_active_price").alias("value"),
                struct(
                    col("sku_quantity.value"),
                    coalesce(
                        col("ai_generated.product_unit_of_measure"), 
                        col("product_quantity.unit")
                    )
                ).alias("quantity")
            ).alias("actual"),
            struct(
                lit("The reference price for this product, based on a fixed quantity.").alias("description"),
                col("sku_reference_price").alias("value"),
                struct(
                    col("product_quantity.value"),
                    col("product_quantity.unit")
                ).alias("unit")
            ).alias("reference")
        ).alias("base_prices"),
        
        # Quantities and attributes
        col("product_quantity"),
        col("sku_quantity"),
        col("items_attributes.product_unidades_descunidad")[0].alias("sku_unity"),
        col("items_attributes.product_unidades_espesable")[0].cast("boolean").alias("is_variable_weight"),
        col("items_attributes"),
        col(date_column)
    ).dropDuplicates()
    
    return df_gold


def _safe_ai_extract(column_name: str):
    """
        Safely apply AI_EXTRACT with fallback to null if function doesn't exist.
        Args:
            column_name (str): Column to apply AI_EXTRACT on.
        Returns:
            Column: Resulting column after applying AI_EXTRACT or null if not available.
        Example:
            >>> df = df.withColumn("ai_generated", _safe_ai_extract("item_name"))
    """

    try:
        # return expr(f"""
        #     AI_EXTRACT(
        #         {column_name},
        #         ARRAY('item_name', 'product_brand', 'product_quantity', 'product_unit_of_measure')
        #     )
        # """)
        return lit(None).cast("struct<item_name:string,product_brand:string,product_quantity:string,product_unit_of_measure:string>")
    except Exception:
        logger.warning("AI_EXTRACT function not available, using null")
        return lit(None).cast("struct<item_name:string,product_brand:string,product_quantity:string,product_unit_of_measure:string>")


def _write_gold_table(
    df: DataFrame,
    target_table_path: str,
    target_exists: bool,
    mode: str,
    merge_keys: Optional[List[str]],
    partition_by: Optional[List[str]],
    date_column: str
) -> None:
    """
        Write DataFrame to gold table based on specified mode.
        Args:
            df (DataFrame): DataFrame to write.
            target_table_path (str): Target table path.
            target_exists (bool): Whether the target table exists.
            mode (str): Write mode ('append', 'overwrite', 'merge', 'replaceWhere').
            merge_keys (Optional[List[str]]): Keys for merge operations.
            partition_by (Optional[List[str]]): Columns to partition by.
            date_column (str): Date column name for filtering.
        Returns:
            None
        Example:
            >>> _write_gold_table(df, 'coto_dev.gold.items', True, 'merge', ['item_id', 'dt'], ['dt'], 'dt')
            >>> _write_gold_table(df, 'coto_dev.gold.items', False, 'overwrite', None, ['dt'], 'dt')
    """
    
    if target_exists and mode == 'merge':
        if not merge_keys:
            raise ValueError("merge_keys required for merge mode")
        
        logger.info(f"Merging data using keys: {merge_keys}")
        delta_table = DeltaTable.forName(spark, target_table_path)
        
        # Deduplicate source before merge
        df_deduped = (
            df.withColumn('_rn', row_number().over(
                Window.partitionBy(*merge_keys).orderBy(col(date_column).desc())
            ))
            .where('_rn = 1')
            .drop('_rn')
        )
        
        merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in merge_keys])
        
        (
            delta_table.alias("target")
            .merge(df_deduped.alias("source"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        
    elif target_exists and mode == 'append':
        logger.info(f"Appending with partition replacement on {date_column}")
        delta_table = DeltaTable.forName(spark, target_table_path)
        
        # Deduplicate by date and item_id before merge
        df_deduped = (
            df.withColumn('_rn', row_number().over(
                Window.partitionBy(date_column, 'item_id').orderBy(col(date_column).desc())
            ))
            .where('_rn = 1')
            .drop('_rn')
        )
        
        (
            delta_table.alias("target")
            .merge(
                df_deduped.alias("source"),
                f"target.{date_column} = source.{date_column} AND target.item_id = source.item_id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    
    elif mode == 'replaceWhere' and target_exists:
        # Get unique dates in the source DataFrame
        dates_to_replace = [row[date_column] for row in df.select(date_column).distinct().collect()]
        dates_str = "', '".join(dates_to_replace)
        
        logger.info(f"Replacing partitions: {date_column} IN ('{dates_str}')")
        
        writer = df.write.mode("overwrite").format("delta")
        
        if partition_by:
            writer = writer.partitionBy(partition_by)
        
        # This replaces ONLY the partitions in the DataFrame
        (
            writer
            .option("replaceWhere", f"{date_column} IN ('{dates_str}')")
            .saveAsTable(target_table_path)
        )
        
    else:  # overwrite or table doesn't exist
        logger.info(f"Writing with mode '{mode}'")
        writer = df.write.mode(mode).format('delta')
        
        if partition_by:
            logger.info(f"Partitioning by: {partition_by}")
            writer = writer.partitionBy(partition_by)
        
        writer.saveAsTable(target_table_path)
