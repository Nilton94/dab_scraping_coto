from pyspark.sql.types import *

def department_schema():

    audit_info = StructType([
        StructField("ecr:innerPath", StringType(), True),
        StructField("ecr:resourcePath", StringType(), True),
    ])

    link_struct = StructType([
        StructField("@class", StringType(), True),
        StructField("linkType", StringType(), True),
        StructField("path", StringType(), True),
        StructField("queryString", StringType(), True),
    ])

    site_definition = StructType([
        StructField("@class", StringType(), True),
        StructField("displayName", StringType(), True),
        StructField("filterStateConfiguration", StringType(), True),
        StructField("id", StringType(), True),
        StructField("patterns", ArrayType(StringType()), True),
        StructField("urlPattern", StringType(), True),
    ])

    site_state = StructType([
        StructField("@class", StringType(), True),
        StructField("contentPath", StringType(), True),
        StructField("matchedUrlPattern", StringType(), True),
        StructField("siteDefinition", site_definition, True),
        StructField("siteId", StringType(), True),
        StructField("validSite", BooleanType(), True),
    ])

    nav_action = StructType([
        StructField("@class", StringType(), True),
        StructField("contentPath", StringType(), True),
        StructField("label", StringType(), True),
        StructField("navigationState", StringType(), True),
        StructField("siteRootPath", StringType(), True),
        StructField("siteState", site_state, True),
    ])

    # ----------------------------
    # Footer sub-structures
    # ----------------------------
    footer_bottom_item = StructType([
        StructField("@type", StringType(), True),
        StructField("content", StringType(), True),
        StructField("endeca:auditInfo", audit_info, True),
        StructField("image_src", StringType(), True),
        StructField("linkClass", StringType(), True),
        StructField("name", StringType(), True),
    ])

    footer_center_subitem = StructType([
        StructField("@type", StringType(), True),
        StructField("endeca:auditInfo", audit_info, True),
        StructField("isButton", BooleanType(), True),
        StructField("isIMG", BooleanType(), True),
        StructField("label", StringType(), True),
        StructField("link", link_struct, True),
        StructField("linkClass", StringType(), True),
        StructField("name", StringType(), True),
        StructField("src", StringType(), True),
        StructField("subitems", ArrayType(StringType()), True),
        StructField("typeRedirect", StringType(), True),
    ])

    footer_center_item = StructType([
        StructField("@type", StringType(), True),
        StructField("content", StringType(), True),
        StructField("endeca:auditInfo", audit_info, True),
        StructField("image_src", StringType(), True),
        StructField("label", StringType(), True),
        StructField("link", link_struct, True),
        StructField("linkClass", StringType(), True),
        StructField("name", StringType(), True),
        StructField("subitems", ArrayType(footer_center_subitem), True),
    ])

    footer_top_item = StructType([
        StructField("@type", StringType(), True),
        StructField("displayNamePropertyAlias", StringType(), True),
        StructField("endeca:auditInfo", audit_info, True),
        StructField("isFooter", BooleanType(), True),
        StructField("name", StringType(), True),
    ])

    footer_contents_item = StructType([
        StructField("@type", StringType(), True),
        StructField("FooterBottom", ArrayType(footer_bottom_item), True),
        StructField("FooterCenter", ArrayType(footer_center_item), True),
        StructField("FooterTop", ArrayType(footer_top_item), True),
        StructField("endeca:auditInfo", audit_info, True),
        StructField("name", StringType(), True),
    ])

    footer_element = StructType([
        StructField("@type", StringType(), True),
        StructField("contentPaths", ArrayType(StringType()), True),
        StructField("contents", ArrayType(footer_contents_item), True),
        StructField("endeca:auditInfo", audit_info, True),
        StructField("name", StringType(), True),
        StructField("ruleLimit", StringType(), True),
        StructField("templateIds", ArrayType(StringType()), True),
        StructField("templateTypes", ArrayType(StringType()), True),
    ])

    # ----------------------------
    # Header sub-structures
    # ----------------------------
    header_top_subitem = StructType([
        StructField("@type", StringType(), True),
        StructField("Suscription", ArrayType(StringType()), True),
        StructField("endeca:auditInfo", audit_info, True),
        StructField("label", StringType(), True),
        StructField("link", link_struct, True),
        StructField("linkClass", StringType(), True),
        StructField("name", StringType(), True),
        StructField("subitems", ArrayType(StringType()), True),
    ])

    header_component = StructType([
        StructField("@type", StringType(), True),
        StructField("displayNamePropertyAlias", StringType(), True),
        StructField("endeca:auditInfo", audit_info, True),
        StructField("name", StringType(), True),
        StructField("image_src", StringType(), True),
        StructField("link", link_struct, True),
        StructField("linkClass", StringType(), True),
        StructField("urlImage", StringType(), True),
        StructField("subitems", ArrayType(header_top_subitem), True),
    ])

    header_contents_block = StructType([
        StructField("Bottom", ArrayType(header_component), True),
        StructField("Center", ArrayType(header_component), True),
        StructField("Mobile", ArrayType(header_component), True),
        StructField("Top", ArrayType(header_component), True),
    ])

    header_element = StructType([
        StructField("@type", StringType(), True),
        StructField("contentPaths", ArrayType(StringType()), True),
        StructField("contents", ArrayType(header_contents_block), True),
        StructField("endeca:auditInfo", audit_info, True),
        StructField("name", StringType(), True),
        StructField("ruleLimit", StringType(), True),
        StructField("templateIds", ArrayType(StringType()), True),
        StructField("templateTypes", ArrayType(StringType()), True),
    ])

    # ----------------------------
    # Left (navigation) sub-structures
    # ----------------------------
    subcat_level2 = StructType([
        StructField("categoryId", StringType(), True),
        StructField("displayName", StringType(), True),
        StructField("navigationState", StringType(), True),
    ])

    subcat = StructType([
        StructField("categoryId", StringType(), True),
        StructField("displayName", StringType(), True),
        StructField("navigationState", StringType(), True),
        StructField("subCategories2", ArrayType(subcat_level2), True),
    ])

    top_level_category = StructType([
        StructField("categoryId", StringType(), True),
        StructField("displayName", StringType(), True),
        StructField("heroImageDescription", StringType(), True),
        StructField("heroImageUrl", StringType(), True),
        StructField("navigationState", StringType(), True),
        StructField("smallImageUrl", StringType(), True),
    ])

    category_struct = StructType([
        StructField("subCategories", ArrayType(subcat), True),
        StructField("topLevelCategory", top_level_category, True),
    ])

    # ancestor properties struct
    ancestor_props = StructType([
        StructField("DGraph.Spec", StringType(), True),
        StructField("category.ancestorCatalogIds", StringType(), True),
        StructField("category.catalogs.repositoryId", StringType(), True),
        StructField("category.repositoryId", StringType(), True),
        StructField("category.rootCatalogId", StringType(), True),
        StructField("category.siteId", StringType(), True),
        StructField("displayName_es", StringType(), True),
        StructField("record.id", StringType(), True),
    ])

    ancestor_site_definition = StructType([
        StructField("@class", StringType(), True),
        StructField("displayName", StringType(), True),
        StructField("filterStateConfiguration", StringType(), True),
        StructField("id", StringType(), True),
        StructField("patterns", ArrayType(StringType()), True),
        StructField("urlPattern", StringType(), True),
    ])

    ancestor_site_state = StructType([
        StructField("@class", StringType(), True),
        StructField("contentPath", StringType(), True),
        StructField("matchedUrlPattern", StringType(), True),
        StructField("siteDefinition", ancestor_site_definition, True),
        StructField("siteId", StringType(), True),
        StructField("validSite", BooleanType(), True),
    ])

    ancestor_item = StructType([
        StructField("@class", StringType(), True),
        StructField("contentPath", StringType(), True),
        StructField("label", StringType(), True),
        StructField("navigationState", StringType(), True),
        StructField("properties", ancestor_props, True),
        StructField("siteRootPath", StringType(), True),
        StructField("siteState", ancestor_site_state, True),
    ])

    navigation_item = StructType([
        StructField("@type", StringType(), True),
        StructField("ancestors", ArrayType(ancestor_item), True),
        StructField("siteRootPath", StringType(), True),
        StructField("siteState", ancestor_site_state, True),
    ])

    refinement_properties = StructType([
        StructField("DGraph.Spec", StringType(), True),
        StructField("DGraph.Strata", StringType(), True),
        StructField("category.ancestorCatalogIds", StringType(), True),
        StructField("category.catalogs.repositoryId", StringType(), True),
        StructField("category.repositoryId", StringType(), True),
        StructField("category.rootCatalogId", StringType(), True),
        StructField("category.siteId", StringType(), True),
        StructField("displayName_es", StringType(), True),
        StructField("record.id", StringType(), True),
    ])

    refinement_item = StructType([
        StructField("@class", StringType(), True),
        StructField("contentPath", StringType(), True),
        StructField("count", LongType(), True),
        StructField("label", StringType(), True),
        StructField("multiSelect", BooleanType(), True),
        StructField("navigationState", StringType(), True),
        StructField("properties", refinement_properties, True),
        StructField("siteRootPath", StringType(), True),
        StructField("siteState", ancestor_site_state, True),
    ])

    range_remove_action = StructType([
        StructField("@class", StringType(), True),
        StructField("contentPath", StringType(), True),
        StructField("label", StringType(), True),
        StructField("navigationState", StringType(), True),
        StructField("siteRootPath", StringType(), True),
        StructField("siteState", ancestor_site_state, True),
    ])

    range_filter_crumb = StructType([
        StructField("@class", StringType(), True),
        StructField("label", StringType(), True),
        StructField("lowerBound", StringType(), True),
        StructField("name", StringType(), True),
        StructField("operation", StringType(), True),
        StructField("propertyName", StringType(), True),
        StructField("removeAction", range_remove_action, True),
        StructField("upperBound", StringType(), True),
    ])

    left_content_inner = StructType([
        StructField("@type", StringType(), True),
        StructField("categories", ArrayType(category_struct), True),
        StructField("endeca:auditInfo", audit_info, True),
        StructField("name", StringType(), True),
        StructField("navigation", ArrayType(navigation_item), True),
        StructField("dimensionName", StringType(), True),
        StructField("displayName", StringType(), True),
        StructField("displayNameProperty", StringType(), True),
        StructField("displayNamePropertyAlias", StringType(), True),
        StructField("moreLink", nav_action, True),
        StructField("multiSelect", BooleanType(), True),
        StructField("refinements", ArrayType(refinement_item), True),
        StructField("whyPrecedenceRuleFired", StringType(), True),
    ])

    left_item = StructType([
        StructField("@type", StringType(), True),
        StructField("contentPaths", ArrayType(StringType()), True),
        StructField("contents", ArrayType(left_content_inner), True),
        StructField("endeca:auditInfo", audit_info, True),
        StructField("displayNameProperty", StringType(), True),
        StructField("displayNamePropertyAlias", StringType(), True),
        StructField("geoFilterCrumb", StringType(), True),
        StructField("imageURL1", StringType(), True),
        StructField("imageURL2", StringType(), True),
        StructField("name", StringType(), True),
        StructField("rangeFilterCrumbs", ArrayType(range_filter_crumb), True),
        StructField("refinementCrumbs", ArrayType(StringType()), True),
        StructField("removeAllAction", nav_action, True),
        StructField("ruleLimit", StringType(), True),
        StructField("searchCrumbs", ArrayType(StringType()), True),
        StructField("templateIds", ArrayType(StringType()), True),
        StructField("templateTypes", ArrayType(StringType()), True),
        StructField("urlRedirect1", StringType(), True),
        StructField("urlRedirect2", StringType(), True),
    ])

    # ----------------------------
    # Main sub-structures (contents -> records)
    # ----------------------------
    # inner-most attributes struct in your print (long list) - we'll include the keys shown
    inner_attributes = StructType([
        StructField("allAncestors.displayName", ArrayType(StringType()), True),
        StructField("allAncestors.repositoryId", ArrayType(StringType()), True),
        StructField("parentCategory.displayName", ArrayType(StringType()), True),
        StructField("product.CLASE", ArrayType(StringType()), True),
        StructField("product.DEPTO", ArrayType(StringType()), True),
        StructField("product.FAMILIA", ArrayType(StringType()), True),
        StructField("product.LCLASE", ArrayType(StringType()), True),
        StructField("product.LDEPAR", ArrayType(StringType()), True),
        StructField("product.MARCA", ArrayType(StringType()), True),
        StructField("product.RELEVANCIA", ArrayType(StringType()), True),
        StructField("product.SIN TACC", ArrayType(StringType()), True),
        StructField("product.TIPO", ArrayType(StringType()), True),
        StructField("product.TOTALDEVENTAS", ArrayType(StringType()), True),
        StructField("product.auxiliaryMedia", ArrayType(StringType()), True),
        StructField("product.averageCustomerRating", ArrayType(StringType()), True),
        StructField("product.baseUrl", ArrayType(StringType()), True),
        StructField("product.brand", ArrayType(StringType()), True),
        StructField("product.briefDescription", ArrayType(StringType()), True),
        StructField("product.cFormato", ArrayType(StringType()), True),
        StructField("product.cantForm", ArrayType(StringType()), True),
        StructField("product.catalogId", ArrayType(StringType()), True),
        StructField("product.category", ArrayType(StringType()), True),
        StructField("product.categoryRank", ArrayType(StringType()), True),
        StructField("product.creationDate", ArrayType(StringType()), True),
        StructField("product.dateAvailable", ArrayType(StringType()), True),
        StructField("product.daysAvailable", ArrayType(StringType()), True),
        StructField("product.description", ArrayType(StringType()), True),
        StructField("product.disallowAsRecommendation", ArrayType(StringType()), True),
        StructField("product.displayName", ArrayType(StringType()), True),
        StructField("product.dtoCaracteristicas", ArrayType(StringType()), True),
        StructField("product.dtoDescuentos", ArrayType(StringType()), True),
        StructField("product.dtoDescuentosMediosPago", ArrayType(StringType()), True),
        StructField("product.eanPrincipal", ArrayType(StringType()), True),
        StructField("product.endDate", ArrayType(StringType()), True),
        StructField("product.esPeque", ArrayType(StringType()), True),
        StructField("product.idTipoGrupo", ArrayType(StringType()), True),
        StructField("product.keywords", ArrayType(StringType()), True),
        StructField("product.language", ArrayType(StringType()), True),
        StructField("product.largeImage.url", ArrayType(StringType()), True),
        StructField("product.ldescr", ArrayType(StringType()), True),
        StructField("product.longDescription", ArrayType(StringType()), True),
        StructField("product.mediumImage.url", ArrayType(StringType()), True),
        StructField("product.nonreturnable", ArrayType(StringType()), True),
        StructField("product.priceListPair", ArrayType(StringType()), True),
        StructField("product.proveedorChico", ArrayType(StringType()), True),
        StructField("product.repositoryId", ArrayType(StringType()), True),
        # many sDisp_* fields as strings
        StructField("product.sDisp_056", ArrayType(StringType()), True),
        StructField("product.sDisp_060", ArrayType(StringType()), True),
        StructField("product.sDisp_061", ArrayType(StringType()), True),
        StructField("product.sDisp_063", ArrayType(StringType()), True),
        StructField("product.sDisp_064", ArrayType(StringType()), True),
        StructField("product.sDisp_065", ArrayType(StringType()), True),
        StructField("product.sDisp_075", ArrayType(StringType()), True),
        StructField("product.sDisp_078", ArrayType(StringType()), True),
        StructField("product.sDisp_085", ArrayType(StringType()), True),
        StructField("product.sDisp_090", ArrayType(StringType()), True),
        StructField("product.sDisp_091", ArrayType(StringType()), True),
        StructField("product.sDisp_092", ArrayType(StringType()), True),
        StructField("product.sDisp_107", ArrayType(StringType()), True),
        StructField("product.sDisp_109", ArrayType(StringType()), True),
        StructField("product.sDisp_129", ArrayType(StringType()), True),
        StructField("product.sDisp_131", ArrayType(StringType()), True),
        StructField("product.sDisp_133", ArrayType(StringType()), True),
        StructField("product.sDisp_160", ArrayType(StringType()), True),
        StructField("product.sDisp_165", ArrayType(StringType()), True),
        StructField("product.sDisp_178", ArrayType(StringType()), True),
        StructField("product.sDisp_181", ArrayType(StringType()), True),
        StructField("product.sDisp_182", ArrayType(StringType()), True),
        StructField("product.sDisp_184", ArrayType(StringType()), True),
        StructField("product.sDisp_185", ArrayType(StringType()), True),
        StructField("product.sDisp_188", ArrayType(StringType()), True),
        StructField("product.sDisp_189", ArrayType(StringType()), True),
        StructField("product.sDisp_197", ArrayType(StringType()), True),
        StructField("product.sDisp_200", ArrayType(StringType()), True),
        StructField("product.sDisp_204", ArrayType(StringType()), True),
        StructField("product.sDisp_209", ArrayType(StringType()), True),
        StructField("product.sDisp_215", ArrayType(StringType()), True),
        StructField("product.sDisp_219", ArrayType(StringType()), True),
        StructField("product.sDisp_220", ArrayType(StringType()), True),
        StructField("product.sDisp_238", ArrayType(StringType()), True),
        StructField("product.sDisp_400", ArrayType(StringType()), True),
        StructField("product.sDisp_710", ArrayType(StringType()), True),
        StructField("product.siteId", ArrayType(StringType()), True),
        StructField("product.startDate", ArrayType(StringType()), True),
        StructField("product.tipoOferta", ArrayType(StringType()), True),
        StructField("product.unidades.descUnidad", ArrayType(StringType()), True),
        StructField("product.unidades.esPesable", ArrayType(StringType()), True),
        StructField("product.url", ArrayType(StringType()), True),

        StructField("record.id", ArrayType(StringType()), True),
        StructField("record.source", ArrayType(StringType()), True),
        StructField("record.type", ArrayType(StringType()), True),

        # SKU fields
        StructField("sku.activePrice", ArrayType(StringType()), True),
        StructField("sku.baseUrl", ArrayType(StringType()), True),
        StructField("sku.creationDate", ArrayType(StringType()), True),
        StructField("sku.description", ArrayType(StringType()), True),
        StructField("sku.displayName", ArrayType(StringType()), True),
        StructField("sku.dtoPrice", ArrayType(StringType()), True),
        StructField("sku.endDate", ArrayType(StringType()), True),
        StructField("sku.margin", ArrayType(StringType()), True),
        StructField("sku.onSaleText", ArrayType(StringType()), True),
        StructField("sku.quantity", ArrayType(StringType()), True),
        StructField("sku.referencePrice", ArrayType(StringType()), True),
        StructField("sku.repositoryId", ArrayType(StringType()), True),
        StructField("sku.siteId", ArrayType(StringType()), True),
        StructField("sku.startDate", ArrayType(StringType()), True),
        StructField("sku.unit_of_measure", ArrayType(StringType()), True),
        StructField("sku.url", ArrayType(StringType()), True),
    ])

    inner_record_level = StructType([
        StructField("@class", StringType(), True),
        StructField("attributes", inner_attributes, True),
        StructField("detailsAction", StructType([
            StructField("@class", StringType(), True),
            StructField("contentPath", StringType(), True),
            StructField("label", StringType(), True),
            StructField("recordState", StringType(), True),
            StructField("siteRootPath", StringType(), True),
            StructField("siteState", site_state, True),
        ]), True),
        StructField("numRecords", LongType(), True),
        StructField("records", StringType(), True),  # schema printed shows records as string at this inner-most level
    ])

    outer_record_attributes = StructType([
        StructField("product.displayName", ArrayType(StringType()), True),
        StructField("product.repositoryId", ArrayType(StringType()), True),
    ])

    outer_record = StructType([
        StructField("@class", StringType(), True),
        StructField("attributes", outer_record_attributes, True),
        StructField("detailsAction", StructType([
            StructField("@class", StringType(), True),
            StructField("contentPath", StringType(), True),
            StructField("label", StringType(), True),
            StructField("recordState", StringType(), True),
            StructField("siteRootPath", StringType(), True),
            StructField("siteState", site_state, True),
        ]), True),
        StructField("numRecords", LongType(), True),
        StructField("records", ArrayType(inner_record_level), True),
    ])

    content_record = StructType([
        StructField("@class", StringType(), True),
        StructField("attributes", outer_record_attributes, True),
        StructField("detailsAction", StructType([
            StructField("@class", StringType(), True),
            StructField("contentPath", StringType(), True),
            StructField("label", StringType(), True),
            StructField("recordState", StringType(), True),
            StructField("siteRootPath", StringType(), True),
            StructField("siteState", site_state, True),
        ]), True),
        StructField("numRecords", LongType(), True),
        StructField("records", ArrayType(inner_record_level), True),
    ])

    content_struct = StructType([
        StructField("@type", StringType(), True),
        StructField("endeca:auditInfo", audit_info, True),
        StructField("firstRecNum", LongType(), True),
        StructField("json-ld", StringType(), True),
        StructField("lastRecNum", LongType(), True),
        StructField("name", StringType(), True),
        StructField("pagingActionTemplate", StructType([
            StructField("@class", StringType(), True),
            StructField("contentPath", StringType(), True),
            StructField("label", StringType(), True),
            StructField("navigationState", StringType(), True),
            StructField("siteRootPath", StringType(), True),
            StructField("siteState", site_state, True),
        ]), True),
        StructField("precomputedSorts", ArrayType(StringType()), True),
        StructField("records", ArrayType(outer_record), True),
        StructField("recsPerPage", LongType(), True),
        StructField("sortOptions", ArrayType(StructType([
            StructField("@class", StringType(), True),
            StructField("contentPath", StringType(), True),
            StructField("label", StringType(), True),
            StructField("navigationState", StringType(), True),
            StructField("selected", BooleanType(), True),
            StructField("siteRootPath", StringType(), True),
            StructField("siteState", site_state, True),
        ])), True),
        StructField("totalNumRecs", LongType(), True),
    ])

    main_item = StructType([
        StructField("@type", StringType(), True),
        StructField("bannerName", StringType(), True),
        StructField("contentPaths", ArrayType(StringType()), True),
        StructField("contents", ArrayType(content_struct), True),
        StructField("endeca:auditInfo", audit_info, True),
        StructField("externo", BooleanType(), True),
        StructField("imageURL", StringType(), True),
        StructField("modal", BooleanType(), True),
        StructField("name", StringType(), True),
        StructField("ruleLimit", StringType(), True),
        StructField("templateIds", ArrayType(StringType()), True),
        StructField("templateTypes", ArrayType(StringType()), True),
        StructField("urlRedirect", StringType(), True),
    ])

    # ----------------------------
    # Root element struct (array<struct< ... >>)
    # ----------------------------
    root_struct = StructType([
        StructField("@type", StringType(), True),
        StructField("Footer", ArrayType(footer_element), True),
        StructField("Header", ArrayType(header_element), True),
        StructField("Left", ArrayType(left_item), True),
        StructField("Main", ArrayType(main_item), True),
        StructField("descriptionSEO", StringType(), True),
        StructField("endeca:auditInfo", audit_info, True),
        StructField("keywordsSEO", StringType(), True),
        StructField("name", StringType(), True),
        StructField("titleSEO", StringType(), True),
    ])

    schema = ArrayType(root_struct, True)

    return schema