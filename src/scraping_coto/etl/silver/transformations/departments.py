from ..schemas.departments import department_schema
from pyspark.sql.functions import col, from_json, expr, explode, struct

def transform_departments(df):

    df_transformed = (
        df
        .withColumn(
            "parsed_contents",
            from_json(col("contents"), department_schema())
        )
        .withColumn(
            "main_slot",
            expr(
                """
                    FILTER(
                        FILTER(
                            parsed_contents[0].Main, x -> x.`@type` = "Main_Slot"
                        )[0].contents,
                        x -> x.`@type` = "Category_ResultsList"
                    )[0]
                """
            )
        )
        .withColumn(
            "left_slot",
            expr(
                "FILTER(parsed_contents[0].Left, x -> x.`@type` = 'Left_Slot')[0]"
            )
        )
        .withColumn("total_items", col("main_slot.totalNumRecs"))
        .withColumn(
            "departments",
            explode(
                expr(
                    "FILTER(left_slot.contents, x -> x.`@type` = 'Category_GuidedNavigation')[0].categories"
                )
            ),
        )
        
        # Extract department info
        .withColumn("department_id", col("departments.topLevelCategory.categoryId"))
        .withColumn("department_name", col("departments.topLevelCategory.displayName"))
        .withColumn("department_url", col("departments.topLevelCategory.navigationState"))
        
        # # Final projection
        .select(
            "department_id",
            "department_name",
            "department_url",
            "total_items",
            struct(
                col('atg:currentSiteProductionURL').alias('prod_url'),
                col('department_url').alias('path'),
                col('url').alias('main_url'),
                col('timestamp'),
                col('timezone')
            ).alias('requests_struct'),
            "departments",
            col('timestamp').cast('date').alias('dt')    
        )
    )

    return df_transformed

    