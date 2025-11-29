from ..schemas.categories import category_schema
from pyspark.sql.functions import col, from_json, expr, struct, get

def transform_categories(df):
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
            'category_id',
            'category_name',
            'total_items',
            struct(
                col('atg:currentSiteProductionURL').alias('prod_url'),
                col('category_url').alias('path'),
                col('category_extended_url').alias('extended_url'),
                col('status_code'),
                col('timestamp'),
                col('timezone')
            ).alias('requests_struct'),
            col('timestamp').cast('date').alias('dt')
        )
    )

    return df_transformed