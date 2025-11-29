from ..schemas.categories import category_schema
from pyspark.sql.functions import col, from_json, expr, struct, get

def transform_subcategories(df):
    df_transformed = (
        df
        .withColumn(
            'parsed_contents',
            get(from_json('contents', category_schema()), 0)
        )
        .withColumn(
            'total_items',
            expr(
                '''
                    FILTER(
                        FILTER(parsed_contents.Main, x -> x.`@type` = 'Main_Slot').contents[0],
                        x -> x.`@type` = 'Category_ResultsList'
                    ).totalNumRecs[0]
                '''
            )
        )
        .select(
            'subcategory_id',
            'subcategory_name',
            'total_items',
            struct(
                col('atg:currentSiteProductionURL').alias('prod_url'),
                col('subcategory_url').alias('path'),
                col('subcategory_extended_url').alias('extended_url'),
                col('status_code'),
                col('timestamp'),
                col('timezone')
            ).alias('requests_struct'),
            col('timestamp').cast('date').alias('dt')
        )
    )

    return df_transformed