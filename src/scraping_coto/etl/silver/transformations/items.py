from ..schemas.items import items_main_schema
from pyspark.sql.functions import col, from_json, expr, struct, get, element_at, explode
from .utils.converters import from_unixtime_to_timestamp
import re
import unidecode

def transform_raw_items(df):
    
    return (
        df
        .withColumn(
            'parsed_contens',
            from_json('contents', items_main_schema())
        )
        .withColumn(
            'main_slot',
            expr(
                '''
                    FILTER(
                        FILTER(parsed_contens[0].Main, x -> x.`@type` = "Main_Slot").contents[0],
                        x -> x.`@type` = "Category_ResultsList"
                    )
                '''
            )
        )
        .withColumn(
            'items_struct',
            explode(get(col('main_slot.records'),0))
        )
        .withColumn(
            'items_attributes',
            expr('items_struct.records[0].attributes')
        )
    )

def transform_items(df):
    
    df_transformed = (
        transform_raw_items(df)
        .withColumn(
            'items_attributes',
            struct(
                *[
                    col('items_attributes').getField(f'{column.name}').alias(
                        re.sub('[^a-zA-Z0-9]+', '_', unidecode.unidecode(column.name)).lower()
                    )
                    for column in transform_raw_items(df).schema['items_attributes'].dataType
                ]
            )
        )
        .selectExpr(
            "id AS subcategory_id",
            "items_attributes.product_repositoryid[0] AS item_id",
            "items_attributes.product_displayname[0] AS item_name",
            "COALESCE(items_attributes.product_brand, items_attributes.product_marca)[0] AS product_brand",
            "items_attributes.product_origen_de_la_marca[0] AS product_origin",
            """
                TRANSFORM(
                    items_attributes.product_dtocaracteristicas,
                    x -> FROM_JSON(
                        x,
                        'ARRAY<STRUCT<nombre: STRING, descripcion: STRING>>'
                    )
                )[0] AS product_info
            """,
            "items_attributes.product_envase[0] AS product_packing_type",
            "items_attributes.product_elaboracion[0] AS product_country_elaboration",
            "items_attributes.product_lclase[0] AS product_subclass",
            "items_attributes.product_ldepar[0] AS product_class",
            "items_attributes.product_TOTALDEVENTAS[0]::FLOAT AS product_total_sales",
            """
                STRUCT(
                    items_attributes.product_cFormato[0] AS unit,
                    items_attributes.product_cantForm[0]::INT AS value
                ) AS product_quantity
            """,
            "items_attributes.product_category[0] AS product_category",
            "items_attributes.product_creationDate[0] AS product_creation_date",
            "items_attributes.product_endDate[0] AS product_end_date",
            "items_attributes.product_dateAvailable[0] AS product_date_available",
            "items_attributes.product_daysAvailable[0]::INT AS product_days_available",
            """
                TRANSFORM(
                    items_attributes.product_dtoDescuentos,
                    x -> 
                        FROM_JSON(
                            x,
                            '''
                                ARRAY<
                                    STRUCT<
                                        id:STRING,
                                        textoVigencia:STRING,
                                        textoPrecioRegular:STRING,
                                        `precio Contado`:STRING,
                                        precioRegular:STRING,
                                        textoDescuento:STRING,
                                        precioDescuento:STRING,
                                        imagenDescuento:STRING,
                                        comentarios:STRING
                                    >
                                >
                            '''
                        )
                )[0] AS product_discounts
            """,
            "items_attributes.product_eanPrincipal[0] AS product_primary_ean",
            "items_attributes.product_tipoOferta AS product_offer_type",
            "items_attributes.sku_activePrice[0]::FLOAT AS sku_active_price",
            """
                TRANSFORM(
                    items_attributes.sku_dtoPrice,
                    x -> FROM_JSON(
                            x,
                            '''
                                STRUCT<
                                    id:STRING,
                                    skuId:STRING,
                                    precioLista:FLOAT,
                                    precio:FLOAT,
                                    precioSinImp:FLOAT
                                >
                            '''
                        ) 
                )AS sku_discounts
            """,
            "items_attributes.sku_referencePrice[0]::FLOAT AS sku_reference_price",
            "items_attributes.sku_creationDate[0] AS sku_creation_date",
            "items_attributes.sku_endDate[0] AS sku_end_date",
            """
                STRUCT(
                    items_attributes.sku_unit_of_measure[0] AS unit,
                    items_attributes.sku_quantity[0]::FLOAT AS value
                ) AS sku_quantity
            """,
            "items_attributes.sku_repositoryId[0] AS sku_repository_id",
            "items_attributes",
            "ELEMENT_AT(main_slot.totalNumRecs, 1) AS total_items",
            "timestamp::DATE AS dt"
        )
        .withColumns(
            {
                column: from_unixtime_to_timestamp(column)
                for column in ['product_creation_date', 'product_end_date', 'product_date_available', 'sku_creation_date', 'sku_end_date']
            }
        )
        .withColumn(
            'product_discounts',
            expr("""
                TRANSFORM(
                    product_discounts,
                    x -> STRUCT(
                            x.id,
                            x.textoVigencia,
                            x.`precio Contado` AS precioContado,
                            STRUCT(
                                x.textoPrecioRegular AS texto,
                                x.textoPrecioRegular AS original,
                                NULLIF(REGEXP_EXTRACT(x.textoPrecioRegular, '([0-9\\.]+)'), '')::FLOAT AS valor
                            ) AS precioRegular,
                            STRUCT(
                                x.textoDescuento AS texto,
                                x.precioDescuento AS original,
                                REGEXP_EXTRACT(x.precioDescuento, '([^0-9\\.]+)') AS moeda,
                                NULLIF(REGEXP_EXTRACT(x.precioDescuento, '([0-9\\.]+)'), '')::FLOAT AS valor
                            ) AS descuento,
                            x.imagenDescuento,
                            x.comentarios
                        )
                )
            """)
        )
    )

    return df_transformed