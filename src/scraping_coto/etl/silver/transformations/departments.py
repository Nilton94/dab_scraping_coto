from ..schemas.departments import department_schema

def transform_departments(df):

    df_transformed = (
        df
        .withColumn(
            "contents",
            from_json(col("contents"))
        )
    )

    