from pyspark.sql.functions import *
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType, DoubleType, BooleanType

def product_transformation(df):
    """
        Apply transformations to the products DataFrame for the silver layer.

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
        .withColumn(
            'products',
            explode('products')
        )
        .withColumn(
            'product',
            explode('products.products')
        )
        .selectExpr(
            'product.product_id',
            """
                STRUCT(
                    category,
                    category_nl,
                    category_url,
                    full_attribute,
                    total_pages,
                    total_products
                ) AS category_attributes
            """,
            'product.barcode',
            'product.description',
            'product.detail_url',
            'product.image_url',
            'product.is_offer',
            'product.price',
            'product.price_text',
            'product.timestamp::TIMESTAMP AS timestamp',
            'product.timezone',
            'product.timestamp::DATE dt'
        )
    )

    return transformed_df
