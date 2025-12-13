from pyspark.sql.functions import *
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType, DoubleType, BooleanType

def category_transformation(df):
    """
        Apply transformations to the categories DataFrame for the silver layer.

        Args:
            df (DataFrame): Input DataFrame from bronze layer.

        Returns:
            DataFrame: Transformed DataFrame ready for silver layer.
    """

    product_schema = StructType([
        StructField("barcode", StringType()),
        StructField("description", StringType()),
        StructField("detail_url", StringType()),
        StructField("image_url", StringType()),
        StructField("is_offer", BooleanType()),
        StructField("price", DoubleType()),
        StructField("price_text", StringType()),
        StructField("product_id", StringType()),
        StructField("timestamp", StringType()),
        StructField("timezone", StringType())
    ])

    url_page_schema = ArrayType(
        StructType([
            StructField("product_count", IntegerType()),
            StructField("products", ArrayType(product_schema))
        ])
    )

    transformed_df = (
        df
        .withColumn(
            'url_pages',
            from_json(col('url_pages'), url_page_schema)
        )
        .selectExpr(
            '* EXCEPT (urls, url_pages)',
            '''
            TRANSFORM(
                url_pages,
                x -> STRUCT(x.product_count, x.products)
            ) AS products
            ''',
            'CURRENT_DATE AS dt'
        )
    )

    return transformed_df