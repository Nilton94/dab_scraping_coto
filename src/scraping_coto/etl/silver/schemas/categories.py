from pyspark.sql.types import *

def category_schema():

    cat_schema = ArrayType(
        StructType([
            StructField("@type", StringType(), True),
            StructField("Footer", ArrayType(
                StructType([
                    StructField("@type", StringType(), True),
                    StructField("contentPaths", ArrayType(StringType(), True), True),
                    StructField("contents", ArrayType(
                        StructType([
                            StructField("@type", StringType(), True),
                            StructField("FooterBottom", ArrayType(
                                StructType([
                                    StructField("@type", StringType(), True),
                                    StructField("content", StringType(), True),
                                    StructField("endeca:auditInfo", StructType([
                                        StructField("ecr:innerPath", StringType(), True),
                                        StructField("ecr:resourcePath", StringType(), True)
                                    ]), True),
                                    StructField("image_src", StringType(), True),
                                    StructField("linkClass", StringType(), True),
                                    StructField("name", StringType(), True)
                                ]), True
                            ), True),
                            StructField("FooterCenter", ArrayType(
                                StructType([
                                    StructField("@type", StringType(), True),
                                    StructField("content", StringType(), True),
                                    StructField("endeca:auditInfo", StructType([
                                        StructField("ecr:innerPath", StringType(), True),
                                        StructField("ecr:resourcePath", StringType(), True)
                                    ]), True),
                                    StructField("image_src", StringType(), True),
                                    StructField("label", StringType(), True),
                                    StructField("link", StructType([
                                        StructField("@class", StringType(), True),
                                        StructField("linkType", StringType(), True),
                                        StructField("path", StringType(), True),
                                        StructField("queryString", StringType(), True)
                                    ]), True),
                                    StructField("linkClass", StringType(), True),
                                    StructField("name", StringType(), True),
                                    StructField("subitems", ArrayType(
                                        StructType([
                                            StructField("@type", StringType(), True),
                                            StructField("endeca:auditInfo", StructType([
                                                StructField("ecr:innerPath", StringType(), True),
                                                StructField("ecr:resourcePath", StringType(), True)
                                            ]), True),
                                            StructField("isButton", BooleanType(), True),
                                            StructField("isIMG", BooleanType(), True),
                                            StructField("label", StringType(), True),
                                            StructField("link", StructType([
                                                StructField("@class", StringType(), True),
                                                StructField("linkType", StringType(), True),
                                                StructField("path", StringType(), True),
                                                StructField("queryString", StringType(), True)
                                            ]), True),
                                            StructField("linkClass", StringType(), True),
                                            StructField("name", StringType(), True),
                                            StructField("src", StringType(), True),
                                            StructField("subitems", ArrayType(StringType(), True), True),
                                            StructField("typeRedirect", StringType(), True)
                                        ]), True
                                    ), True)
                                ]), True
                            ), True),
                            StructField("FooterTop", ArrayType(
                                StructType([
                                    StructField("@type", StringType(), True),
                                    StructField("displayNamePropertyAlias", StringType(), True),
                                    StructField("endeca:auditInfo", StructType([
                                        StructField("ecr:innerPath", StringType(), True),
                                        StructField("ecr:resourcePath", StringType(), True)
                                    ]), True),
                                    StructField("isFooter", BooleanType(), True),
                                    StructField("name", StringType(), True)
                                ]), True
                            ), True),
                            StructField("endeca:auditInfo", StructType([
                                StructField("ecr:innerPath", StringType(), True),
                                StructField("ecr:resourcePath", StringType(), True)
                            ]), True),
                            StructField("name", StringType(), True)
                        ]), True
                    ), True),
                    StructField("endeca:auditInfo", StructType([
                        StructField("ecr:innerPath", StringType(), True),
                        StructField("ecr:resourcePath", StringType(), True)
                    ]), True),
                    StructField("name", StringType(), True),
                    StructField("ruleLimit", StringType(), True),
                    StructField("templateIds", ArrayType(StringType(), True), True),
                    StructField("templateTypes", ArrayType(StringType(), True), True)
                ]), True
            ), True),
            StructField("Header", ArrayType(
                StructType([
                    StructField("@type", StringType(), True),
                    StructField("contentPaths", ArrayType(StringType(), True), True),
                    StructField("contents", ArrayType(
                        StructType([
                            StructField("@type", StringType(), True),
                            StructField("Bottom", ArrayType(
                                StructType([
                                    StructField("@type", StringType(), True),
                                    StructField("displayNamePropertyAlias", StringType(), True),
                                    StructField("endeca:auditInfo", StructType([
                                        StructField("ecr:innerPath", StringType(), True),
                                        StructField("ecr:resourcePath", StringType(), True)
                                    ]), True),
                                    StructField("name", StringType(), True)
                                ]), True
                            ), True),
                            StructField("Center", ArrayType(
                                StructType([
                                    StructField("@type", StringType(), True),
                                    StructField("displayNamePropertyAlias", StringType(), True),
                                    StructField("endeca:auditInfo", StructType([
                                        StructField("ecr:innerPath", StringType(), True),
                                        StructField("ecr:resourcePath", StringType(), True)
                                    ]), True),
                                    StructField("image_src", StringType(), True),
                                    StructField("link", StructType([
                                        StructField("@class", StringType(), True),
                                        StructField("linkType", StringType(), True),
                                        StructField("path", StringType(), True),
                                        StructField("queryString", StringType(), True)
                                    ]), True),
                                    StructField("linkClass", StringType(), True),
                                    StructField("name", StringType(), True),
                                    StructField("urlImage", StringType(), True)
                                ]), True
                            ), True),
                            StructField("Mobile", ArrayType(
                                StructType([
                                    StructField("@type", StringType(), True),
                                    StructField("displayNamePropertyAlias", StringType(), True),
                                    StructField("endeca:auditInfo", StructType([
                                        StructField("ecr:innerPath", StringType(), True),
                                        StructField("ecr:resourcePath", StringType(), True)
                                    ]), True),
                                    StructField("name", StringType(), True)
                                ]), True
                            ), True),
                            StructField("Top", ArrayType(
                                StructType([
                                    StructField("@type", StringType(), True),
                                    StructField("displayNamePropertyAlias", StringType(), True),
                                    StructField("endeca:auditInfo", StructType([
                                        StructField("ecr:innerPath", StringType(), True),
                                        StructField("ecr:resourcePath", StringType(), True)
                                    ]), True),
                                    StructField("name", StringType(), True),
                                    StructField("subitems", ArrayType(
                                        StructType([
                                            StructField("@type", StringType(), True),
                                            StructField("Suscription", ArrayType(StringType(), True), True),
                                            StructField("endeca:auditInfo", StructType([
                                                StructField("ecr:innerPath", StringType(), True),
                                                StructField("ecr:resourcePath", StringType(), True)
                                            ]), True),
                                            StructField("label", StringType(), True),
                                            StructField("link", StructType([
                                                StructField("@class", StringType(), True),
                                                StructField("linkType", StringType(), True),
                                                StructField("path", StringType(), True),
                                                StructField("queryString", StringType(), True)
                                            ]), True),
                                            StructField("linkClass", StringType(), True),
                                            StructField("name", StringType(), True),
                                            StructField("subitems", ArrayType(StringType(), True), True)
                                        ]), True
                                    ), True)
                                ]), True
                            ), True),
                            StructField("endeca:auditInfo", StructType([
                                StructField("ecr:innerPath", StringType(), True),
                                StructField("ecr:resourcePath", StringType(), True)
                            ]), True),
                            StructField("name", StringType(), True)
                        ]), True
                    ), True),
                    StructField("endeca:auditInfo", StructType([
                        StructField("ecr:innerPath", StringType(), True),
                        StructField("ecr:resourcePath", StringType(), True)
                    ]), True),
                    StructField("name", StringType(), True),
                    StructField("ruleLimit", StringType(), True),
                    StructField("templateIds", ArrayType(StringType(), True), True),
                    StructField("templateTypes", ArrayType(StringType(), True), True)
                ]), True
            ), True),
            StructField("Left", ArrayType(
                StructType([
                    StructField("@type", StringType(), True),
                    StructField("contentPaths", ArrayType(StringType(), True), True),
                    StructField("contents", ArrayType(
                        StructType([
                            StructField("@type", StringType(), True),
                            StructField("categories", ArrayType(
                                StructType([
                                    StructField("subCategories", ArrayType(
                                        StructType([
                                            StructField("categoryId", StringType(), True),
                                            StructField("displayName", StringType(), True),
                                            StructField("navigationState", StringType(), True),
                                            StructField("subCategories2", ArrayType(
                                                StructType([
                                                    StructField("categoryId", StringType(), True),
                                                    StructField("displayName", StringType(), True),
                                                    StructField("navigationState", StringType(), True)
                                                ]), True
                                            ), True)
                                        ]), True
                                    ), True),
                                    StructField("topLevelCategory", StructType([
                                        StructField("categoryId", StringType(), True),
                                        StructField("displayName", StringType(), True),
                                        StructField("heroImageDescription", StringType(), True),
                                        StructField("heroImageUrl", StringType(), True),
                                        StructField("navigationState", StringType(), True),
                                        StructField("smallImageUrl", StringType(), True)
                                    ]), True)
                                ]), True
                            ), True),
                            StructField("endeca:auditInfo", StructType([
                                StructField("ecr:innerPath", StringType(), True),
                                StructField("ecr:resourcePath", StringType(), True)
                            ]), True),
                            StructField("name", StringType(), True),
                            StructField("navigation", ArrayType(
                                StructType([
                                    StructField("@type", StringType(), True),
                                    StructField("ancestors", ArrayType(
                                        StructType([
                                            StructField("@class", StringType(), True),
                                            StructField("contentPath", StringType(), True),
                                            StructField("label", StringType(), True),
                                            StructField("navigationState", StringType(), True),
                                            StructField("properties", StructType([
                                                StructField("DGraph.More", StringType(), True),
                                                StructField("DGraph.Spec", StringType(), True),
                                                StructField("category.ancestorCatalogIds", StringType(), True),
                                                StructField("category.catalogs.repositoryId", StringType(), True),
                                                StructField("category.repositoryId", StringType(), True),
                                                StructField("category.rootCatalogId", StringType(), True),
                                                StructField("category.siteId", StringType(), True),
                                                StructField("displayName_es", StringType(), True),
                                                StructField("record.id", StringType(), True)
                                            ]), True),
                                            StructField("siteRootPath", StringType(), True),
                                            StructField("siteState", StructType([
                                                StructField("@class", StringType(), True),
                                                StructField("contentPath", StringType(), True),
                                                StructField("matchedUrlPattern", StringType(), True),
                                                StructField("siteDefinition", StructType([
                                                    StructField("@class", StringType(), True),
                                                    StructField("displayName", StringType(), True),
                                                    StructField("filterStateConfiguration", StringType(), True),
                                                    StructField("id", StringType(), True),
                                                    StructField("patterns", ArrayType(StringType(), True), True),
                                                    StructField("urlPattern", StringType(), True)
                                                ]), True),
                                                StructField("siteId", StringType(), True),
                                                StructField("validSite", BooleanType(), True)
                                            ]), True)
                                        ]), True
                                    ), True),
                                    StructField("dimensionName", StringType(), True),
                                    StructField("displayName", StringType(), True),
                                    StructField("displayNameProperty", StringType(), True),
                                    StructField("displayNamePropertyAlias", StringType(), True),
                                    StructField("endeca:auditInfo", StructType([
                                        StructField("ecr:innerPath", StringType(), True),
                                        StructField("ecr:resourcePath", StringType(), True)
                                    ]), True),
                                    StructField("moreLink", StructType([
                                        StructField("@class", StringType(), True),
                                        StructField("contentPath", StringType(), True),
                                        StructField("label", StringType(), True),
                                        StructField("navigationState", StringType(), True),
                                        StructField("siteRootPath", StringType(), True),
                                        StructField("siteState", StructType([
                                            StructField("@class", StringType(), True),
                                            StructField("contentPath", StringType(), True),
                                            StructField("matchedUrlPattern", StringType(), True),
                                            StructField("siteDefinition", StructType([
                                                StructField("@class", StringType(), True),
                                                StructField("displayName", StringType(), True),
                                                StructField("filterStateConfiguration", StringType(), True),
                                                StructField("id", StringType(), True),
                                                StructField("patterns", ArrayType(StringType(), True), True),
                                                StructField("urlPattern", StringType(), True)
                                            ]), True),
                                            StructField("siteId", StringType(), True),
                                            StructField("validSite", BooleanType(), True)
                                        ]), True)
                                    ]), True),
                                    StructField("multiSelect", BooleanType(), True),
                                    StructField("name", StringType(), True),
                                    StructField("refinements", ArrayType(
                                        StructType([
                                            StructField("@class", StringType(), True),
                                            StructField("contentPath", StringType(), True),
                                            StructField("count", LongType(), True),
                                            StructField("label", StringType(), True),
                                            StructField("multiSelect", BooleanType(), True),
                                            StructField("navigationState", StringType(), True),
                                            StructField("properties", StructType([
                                                StructField("DGraph.Spec", StringType(), True),
                                                StructField("DGraph.Strata", StringType(), True),
                                                StructField("category.ancestorCatalogIds", StringType(), True),
                                                StructField("category.catalogs.repositoryId", StringType(), True),
                                                StructField("category.repositoryId", StringType(), True),
                                                StructField("category.rootCatalogId", StringType(), True),
                                                StructField("category.siteId", StringType(), True),
                                                StructField("displayName_es", StringType(), True),
                                                StructField("record.id", StringType(), True)
                                            ]), True),
                                            StructField("siteRootPath", StringType(), True),
                                            StructField("siteState", StructType([
                                                StructField("@class", StringType(), True),
                                                StructField("contentPath", StringType(), True),
                                                StructField("matchedUrlPattern", StringType(), True),
                                                StructField("siteDefinition", StructType([
                                                    StructField("@class", StringType(), True),
                                                    StructField("displayName", StringType(), True),
                                                    StructField("filterStateConfiguration", StringType(), True),
                                                    StructField("id", StringType(), True),
                                                    StructField("patterns", ArrayType(StringType(), True), True),
                                                    StructField("urlPattern", StringType(), True)
                                                ]), True),
                                                StructField("siteId", StringType(), True),
                                                StructField("validSite", BooleanType(), True)
                                            ]), True)
                                        ]), True
                                    ), True),
                                    StructField("whyPrecedenceRuleFired", StringType(), True)
                                ]), True
                            ), True)
                        ]), True
                    ), True),
                    StructField("displayNameProperty", StringType(), True),
                    StructField("displayNamePropertyAlias", StringType(), True),
                    StructField("endeca:auditInfo", StructType([
                        StructField("ecr:innerPath", StringType(), True),
                        StructField("ecr:resourcePath", StringType(), True)
                    ]), True),
                    StructField("geoFilterCrumb", StringType(), True),
                    StructField("imageURL1", StringType(), True),
                    StructField("imageURL2", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("rangeFilterCrumbs", ArrayType(
                        StructType([
                            StructField("@class", StringType(), True),
                            StructField("label", StringType(), True),
                            StructField("lowerBound", StringType(), True),
                            StructField("name", StringType(), True),
                            StructField("operation", StringType(), True),
                            StructField("propertyName", StringType(), True),
                            StructField("removeAction", StructType([
                                StructField("@class", StringType(), True),
                                StructField("contentPath", StringType(), True),
                                StructField("label", StringType(), True),
                                StructField("navigationState", StringType(), True),
                                StructField("siteRootPath", StringType(), True),
                                StructField("siteState", StructType([
                                    StructField("@class", StringType(), True),
                                    StructField("contentPath", StringType(), True),
                                    StructField("matchedUrlPattern", StringType(), True),
                                    StructField("siteDefinition", StructType([
                                        StructField("@class", StringType(), True),
                                        StructField("displayName", StringType(), True),
                                        StructField("filterStateConfiguration", StringType(), True),
                                        StructField("id", StringType(), True),
                                        StructField("patterns", ArrayType(StringType(), True), True),
                                        StructField("urlPattern", StringType(), True)
                                    ]), True),
                                    StructField("siteId", StringType(), True),
                                    StructField("validSite", BooleanType(), True)
                                ]), True)
                            ]), True),
                            StructField("upperBound", StringType(), True)
                        ]), True
                    ), True),
                    StructField("refinementCrumbs", ArrayType(
                        StructType([
                            StructField("@class", StringType(), True),
                            StructField("ancestors", ArrayType(
                                StructType([
                                    StructField("@class", StringType(), True),
                                    StructField("contentPath", StringType(), True),
                                    StructField("label", StringType(), True),
                                    StructField("navigationState", StringType(), True),
                                    StructField("properties", StructType([
                                        StructField("DGraph.Spec", StringType(), True),
                                        StructField("category.ancestorCatalogIds", StringType(), True),
                                        StructField("category.catalogs.repositoryId", StringType(), True),
                                        StructField("category.repositoryId", StringType(), True),
                                        StructField("category.rootCatalogId", StringType(), True),
                                        StructField("category.siteId", StringType(), True),
                                        StructField("displayName_es", StringType(), True),
                                        StructField("record.id", StringType(), True)
                                    ]), True),
                                    StructField("siteRootPath", StringType(), True),
                                    StructField("siteState", StructType([
                                        StructField("@class", StringType(), True),
                                        StructField("contentPath", StringType(), True),
                                        StructField("matchedUrlPattern", StringType(), True),
                                        StructField("siteDefinition", StructType([
                                            StructField("@class", StringType(), True),
                                            StructField("displayName", StringType(), True),
                                            StructField("filterStateConfiguration", StringType(), True),
                                            StructField("id", StringType(), True),
                                            StructField("patterns", ArrayType(StringType(), True), True),
                                            StructField("urlPattern", StringType(), True)
                                        ]), True),
                                        StructField("siteId", StringType(), True),
                                        StructField("validSite", BooleanType(), True)
                                    ]), True)
                                ]), True
                            ), True),
                            StructField("count", LongType(), True),
                            StructField("dimensionName", StringType(), True),
                            StructField("displayName", StringType(), True),
                            StructField("label", StringType(), True),
                            StructField("multiSelect", BooleanType(), True),
                            StructField("properties", StructType([
                                StructField("DGraph.More", StringType(), True),
                                StructField("DGraph.Spec", StringType(), True),
                                StructField("category.ancestorCatalogIds", StringType(), True),
                                StructField("category.catalogs.repositoryId", StringType(), True),
                                StructField("category.repositoryId", StringType(), True),
                                StructField("category.rootCatalogId", StringType(), True),
                                StructField("category.siteId", StringType(), True),
                                StructField("displayName_es", StringType(), True),
                                StructField("record.id", StringType(), True)
                            ]), True),
                            StructField("removeAction", StructType([
                                StructField("@class", StringType(), True),
                                StructField("contentPath", StringType(), True),
                                StructField("label", StringType(), True),
                                StructField("navigationState", StringType(), True),
                                StructField("siteRootPath", StringType(), True),
                                StructField("siteState", StructType([
                                    StructField("@class", StringType(), True),
                                    StructField("contentPath", StringType(), True),
                                    StructField("matchedUrlPattern", StringType(), True),
                                    StructField("siteDefinition", StructType([
                                        StructField("@class", StringType(), True),
                                        StructField("displayName", StringType(), True),
                                        StructField("filterStateConfiguration", StringType(), True),
                                        StructField("id", StringType(), True),
                                        StructField("patterns", ArrayType(StringType(), True), True),
                                        StructField("urlPattern", StringType(), True)
                                    ]), True),
                                    StructField("siteId", StringType(), True),
                                    StructField("validSite", BooleanType(), True)
                                ]), True)
                            ]), True)
                        ]), True
                    ), True),
                    StructField("removeAllAction", StructType([
                        StructField("@class", StringType(), True),
                        StructField("contentPath", StringType(), True),
                        StructField("label", StringType(), True),
                        StructField("navigationState", StringType(), True),
                        StructField("siteRootPath", StringType(), True),
                        StructField("siteState", StructType([
                            StructField("@class", StringType(), True),
                            StructField("contentPath", StringType(), True),
                            StructField("matchedUrlPattern", StringType(), True),
                            StructField("siteDefinition", StructType([
                                StructField("@class", StringType(), True),
                                StructField("displayName", StringType(), True),
                                StructField("filterStateConfiguration", StringType(), True),
                                StructField("id", StringType(), True),
                                StructField("patterns", ArrayType(StringType(), True), True),
                                StructField("urlPattern", StringType(), True)
                            ]), True),
                            StructField("siteId", StringType(), True),
                            StructField("validSite", BooleanType(), True)
                        ]), True)
                    ]), True),
                    StructField("ruleLimit", StringType(), True),
                    StructField("searchCrumbs", ArrayType(StringType(), True), True),
                    StructField("templateIds", ArrayType(StringType(), True), True),
                    StructField("templateTypes", ArrayType(StringType(), True), True),
                    StructField("urlRedirect1", StringType(), True),
                    StructField("urlRedirect2", StringType(), True)
                ]), True
            ), True),
            StructField("Main", ArrayType(
                StructType([
                    StructField("@type", StringType(), True),
                    StructField("bannerName", StringType(), True),
                    StructField("contentPaths", ArrayType(StringType(), True), True),
                    StructField("contents", ArrayType(
                        StructType([
                            StructField("@type", StringType(), True),
                            StructField("endeca:auditInfo", StructType([
                                StructField("ecr:innerPath", StringType(), True),
                                StructField("ecr:resourcePath", StringType(), True)
                            ]), True),
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
                                StructField("siteState", StructType([
                                    StructField("@class", StringType(), True),
                                    StructField("contentPath", StringType(), True),
                                    StructField("matchedUrlPattern", StringType(), True),
                                    StructField("siteDefinition", StructType([
                                        StructField("@class", StringType(), True),
                                        StructField("displayName", StringType(), True),
                                        StructField("filterStateConfiguration", StringType(), True),
                                        StructField("id", StringType(), True),
                                        StructField("patterns", ArrayType(StringType(), True), True),
                                        StructField("urlPattern", StringType(), True)
                                    ]), True),
                                    StructField("siteId", StringType(), True),
                                    StructField("validSite", BooleanType(), True)
                                ]), True)
                            ]), True),
                            StructField("precomputedSorts", ArrayType(StringType(), True), True),
                            StructField("records", ArrayType(
                                StructType([
                                    StructField("@class", StringType(), True),
                                    StructField("attributes", StructType([
                                        StructField("product.displayName", ArrayType(StringType(), True), True),
                                        StructField("product.repositoryId", ArrayType(StringType(), True), True)
                                    ]), True),
                                    StructField("detailsAction", StructType([
                                        StructField("@class", StringType(), True),
                                        StructField("contentPath", StringType(), True),
                                        StructField("label", StringType(), True),
                                        StructField("recordState", StringType(), True),
                                        StructField("siteRootPath", StringType(), True),
                                        StructField("siteState", StructType([
                                            StructField("@class", StringType(), True),
                                            StructField("contentPath", StringType(), True),
                                            StructField("matchedUrlPattern", StringType(), True),
                                            StructField("siteDefinition", StructType([
                                                StructField("@class", StringType(), True),
                                                StructField("displayName", StringType(), True),
                                                StructField("filterStateConfiguration", StringType(), True),
                                                StructField("id", StringType(), True),
                                                StructField("patterns", ArrayType(StringType(), True), True),
                                                StructField("urlPattern", StringType(), True)
                                            ]), True),
                                            StructField("siteId", StringType(), True),
                                            StructField("validSite", BooleanType(), True)
                                        ]), True)
                                    ]), True),
                                    StructField("numRecords", LongType(), True),
                                    StructField("records", ArrayType(
                                        StructType([
                                            StructField("@class", StringType(), True),
                                            StructField("attributes", StructType([
                                                StructField("allAncestors.displayName", ArrayType(StringType(), True), True),
                                                StructField("allAncestors.repositoryId", ArrayType(StringType(), True), True),
                                                StructField("parentCategory.displayName", ArrayType(StringType(), True), True),
                                                StructField("product.ACCIONAMIENTO AUTOMATICO", ArrayType(StringType(), True), True),
                                                StructField("product.BOTELLA RETORNABLE", ArrayType(StringType(), True), True),
                                                StructField("product.CABLE RETRACTIL", ArrayType(StringType(), True), True),
                                                StructField("product.CANASTO", ArrayType(StringType(), True), True),
                                                StructField("product.CANTIDAD", ArrayType(StringType(), True), True),
                                                StructField("product.CAPACIDAD", ArrayType(StringType(), True), True),
                                                StructField("product.CAPACIDAD DE LAVADO", ArrayType(StringType(), True), True),
                                                StructField("product.CAPACIDAD DE MOLIENDA", ArrayType(StringType(), True), True),
                                                StructField("product.CAPACIDAD MAXIMA", ArrayType(StringType(), True), True),
                                                StructField("product.CAPAS DE CONFORT", ArrayType(StringType(), True), True),
                                                StructField("product.CATEGORIA", ArrayType(StringType(), True), True),
                                                StructField("product.CLASE", ArrayType(StringType(), True), True),
                                                StructField("product.CLIMATIZACION", ArrayType(StringType(), True), True),
                                                StructField("product.COLOR", ArrayType(StringType(), True), True),
                                                StructField("product.COMPOSICIÓN DE TELA", ArrayType(StringType(), True), True),
                                                StructField("product.COMPOSICIÓN TELA", ArrayType(StringType(), True), True),
                                                StructField("product.CONECTIVIDAD", ArrayType(StringType(), True), True),
                                                StructField("product.CONTENIDO", ArrayType(StringType(), True), True),
                                                StructField("product.CONTROL DE TEMPERATURA", ArrayType(StringType(), True), True),
                                                StructField("product.CORTE AUTOMATICO", ArrayType(StringType(), True), True),
                                                StructField("product.CUADRO", ArrayType(StringType(), True), True),
                                                StructField("product.DEPTO", ArrayType(StringType(), True), True),
                                                StructField("product.DISPLAY DIGITAL", ArrayType(StringType(), True), True),
                                                StructField("product.DURACIÓN ESTIMADA", ArrayType(StringType(), True), True),
                                                StructField("product.EDAD", ArrayType(StringType(), True), True),
                                                StructField("product.ELABORACIÓN", ArrayType(StringType(), True), True),
                                                StructField("product.ENVASADO AL VACÍO", ArrayType(StringType(), True), True),
                                                StructField("product.ENVASE", ArrayType(StringType(), True), True),
                                                StructField("product.EQUIVALENTE A", ArrayType(StringType(), True), True),
                                                StructField("product.ESTILO", ArrayType(StringType(), True), True),
                                                StructField("product.ESTRUCTURA", ArrayType(StringType(), True), True),
                                                StructField("product.ESTUCHE", ArrayType(StringType(), True), True),
                                                StructField("product.ETAPA DE VIDA", ArrayType(StringType(), True), True),
                                                StructField("product.FAMILIA", ArrayType(StringType(), True), True),
                                                StructField("product.FORMA DE CALENTAMIENTO", ArrayType(StringType(), True), True),
                                                StructField("product.FRAGANCIAS", ArrayType(StringType(), True), True),
                                                StructField("product.FRIGORIAS", ArrayType(StringType(), True), True),
                                                StructField("product.GARANTIA", ArrayType(StringType(), True), True),
                                                StructField("product.GRAMAJE", ArrayType(StringType(), True), True),
                                                StructField("product.GÉNERO", ArrayType(StringType(), True), True),
                                                StructField("product.HORQUILLA", ArrayType(StringType(), True), True),
                                                StructField("product.IBU", ArrayType(StringType(), True), True),
                                                StructField("product.INTERNA-EXTERNA", ArrayType(StringType(), True), True),
                                                StructField("product.KOSHER", ArrayType(StringType(), True), True),
                                                StructField("product.LCLASE", ArrayType(StringType(), True), True),
                                                StructField("product.LDEPAR", ArrayType(StringType(), True), True),
                                                StructField("product.MARCA", ArrayType(StringType(), True), True),
                                                StructField("product.MATERIAL DE CUCHILLAS", ArrayType(StringType(), True), True),
                                                StructField("product.MATERIAL DE PLACAS", ArrayType(StringType(), True), True),
                                                StructField("product.MEDIDA", ArrayType(StringType(), True), True),
                                                StructField("product.MODELO DE LÁMPARA", ArrayType(StringType(), True), True),
                                                StructField("product.MODELO DE ROSCA", ArrayType(StringType(), True), True),
                                                StructField("product.MODO DEPILACION", ArrayType(StringType(), True), True),
                                                StructField("product.MOTOR", ArrayType(StringType(), True), True),
                                                StructField("product.NIVEL DE FIRMEZA", ArrayType(StringType(), True), True),
                                                StructField("product.NOMBRE", ArrayType(StringType(), True), True),
                                                StructField("product.ORIGEN", ArrayType(StringType(), True), True),
                                                StructField("product.ORIGEN DE LA MARCA", ArrayType(StringType(), True), True),
                                                StructField("product.PAIS", ArrayType(StringType(), True), True),
                                                StructField("product.PANEL DIGITAL", ArrayType(StringType(), True), True),
                                                StructField("product.PESO", ArrayType(StringType(), True), True),
                                                StructField("product.PILLOW", ArrayType(StringType(), True), True),
                                                StructField("product.POSICIONADOR", ArrayType(StringType(), True), True),
                                                StructField("product.POTENCIA", ArrayType(StringType(), True), True),
                                                StructField("product.POTENCIA (WATS)", ArrayType(StringType(), True), True),
                                                StructField("product.PRECISION", ArrayType(StringType(), True), True),
                                                StructField("product.PRENSATELAS", ArrayType(StringType(), True), True),
                                                StructField("product.PULGADAS", ArrayType(StringType(), True), True),
                                                StructField("product.REGION", ArrayType(StringType(), True), True),
                                                StructField("product.RELEVANCIA", ArrayType(StringType(), True), True),
                                                StructField("product.RESOLUCION", ArrayType(StringType(), True), True),
                                                StructField("product.RODADO", ArrayType(StringType(), True), True),
                                                StructField("product.SABOR", ArrayType(StringType(), True), True),
                                                StructField("product.SALIDA AL EXTERIOR", ArrayType(StringType(), True), True),
                                                StructField("product.SELECTOR DE TEMPERATURA", ArrayType(StringType(), True), True),
                                                StructField("product.SIN TACC", ArrayType(StringType(), True), True),
                                                StructField("product.SISTEMA DE FUNCIONAMIENTO", ArrayType(StringType(), True), True),
                                                StructField("product.SMART", ArrayType(StringType(), True), True),
                                                StructField("product.SUBMARCA-LÍNEA", ArrayType(StringType(), True), True),
                                                StructField("product.SUSPENSION", ArrayType(StringType(), True), True),
                                                StructField("product.TALLE", ArrayType(StringType(), True), True),
                                                StructField("product.TAMAÑO", ArrayType(StringType(), True), True),
                                                StructField("product.TAMAÑO DE LA RAZA", ArrayType(StringType(), True), True),
                                                StructField("product.TARA", ArrayType(StringType(), True), True),
                                                StructField("product.TECNOLOGIA", ArrayType(StringType(), True), True),
                                                StructField("product.TEMPORIZADOR", ArrayType(StringType(), True), True),
                                                StructField("product.TIMER PROGRAMABLE", ArrayType(StringType(), True), True),
                                                StructField("product.TIPO", ArrayType(StringType(), True), True),
                                                StructField("product.TIPO DE AGUJA", ArrayType(StringType(), True), True),
                                                StructField("product.TIPO DE ALIMENTACION", ArrayType(StringType(), True), True),
                                                StructField("product.TIPO DE ALIMENTO", ArrayType(StringType(), True), True),
                                                StructField("product.TIPO DE ASPIRADORA", ArrayType(StringType(), True), True),
                                                StructField("product.TIPO DE BALANZA", ArrayType(StringType(), True), True),
                                                StructField("product.TIPO DE BICICLETA", ArrayType(StringType(), True), True),
                                                StructField("product.TIPO DE CARGA", ArrayType(StringType(), True), True),
                                                StructField("product.TIPO DE CARNE", ArrayType(StringType(), True), True),
                                                StructField("product.TIPO DE CERVEZA", ArrayType(StringType(), True), True),
                                                StructField("product.TIPO DE CORTE", ArrayType(StringType(), True), True),
                                                StructField("product.TIPO DE EMPAQUE", ArrayType(StringType(), True), True),
                                                StructField("product.TIPO DE EQUIPO", ArrayType(StringType(), True), True),
                                                StructField("product.TIPO DE FRENOS", ArrayType(StringType(), True), True),
                                                StructField("product.TIPO DE LUZ", ArrayType(StringType(), True), True),
                                                StructField("product.TIPO DE LÁMPARA", ArrayType(StringType(), True), True),
                                                StructField("product.TIPO DE MATERIAL", ArrayType(StringType(), True), True),
                                                StructField("product.TIPO DE PRODUCTO", ArrayType(StringType(), True), True),
                                                StructField("product.TIPO DE VACUNO", ArrayType(StringType(), True), True),
                                                StructField("product.TIRAJE", ArrayType(StringType(), True), True),
                                                StructField("product.TOTALDEVENTAS", ArrayType(StringType(), True), True),
                                                StructField("product.TRABAJA EN FRIO", ArrayType(StringType(), True), True),
                                                StructField("product.ULTRA SILENCIOSO", ArrayType(StringType(), True), True),
                                                StructField("product.VARIETAL", ArrayType(StringType(), True), True),
                                                StructField("product.VELOCIDAD DE CENTRIFUGADO", ArrayType(StringType(), True), True),
                                                StructField("product.VELOCIDADES", ArrayType(StringType(), True), True),
                                                StructField("product.VERSION CPU", ArrayType(StringType(), True), True),
                                                StructField("product.VOLUMEN", ArrayType(StringType(), True), True),
                                                StructField("product.adescr", ArrayType(StringType(), True), True),
                                                StructField("product.auxiliaryMedia", ArrayType(StringType(), True), True),
                                                StructField("product.averageCustomerRating", ArrayType(StringType(), True), True),
                                                StructField("product.baseUrl", ArrayType(StringType(), True), True),
                                                StructField("product.brand", ArrayType(StringType(), True), True),
                                                StructField("product.briefDescription", ArrayType(StringType(), True), True),
                                                StructField("product.cFormato", ArrayType(StringType(), True), True),
                                                StructField("product.cantForm", ArrayType(StringType(), True), True),
                                                StructField("product.cantidadMinima", ArrayType(StringType(), True), True),
                                                StructField("product.catalogId", ArrayType(StringType(), True), True),
                                                StructField("product.category", ArrayType(StringType(), True), True),
                                                StructField("product.categoryRank", ArrayType(StringType(), True), True),
                                                StructField("product.creationDate", ArrayType(StringType(), True), True),
                                                StructField("product.dateAvailable", ArrayType(StringType(), True), True),
                                                StructField("product.daysAvailable", ArrayType(StringType(), True), True),
                                                StructField("product.description", ArrayType(StringType(), True), True),
                                                StructField("product.disallowAsRecommendation", ArrayType(StringType(), True), True),
                                                StructField("product.displayName", ArrayType(StringType(), True), True),
                                                StructField("product.dtoCaracteristicas", ArrayType(StringType(), True), True),
                                                StructField("product.dtoDescuentos", ArrayType(StringType(), True), True),
                                                StructField("product.dtoDescuentosMediosPago", ArrayType(StringType(), True), True),
                                                StructField("product.eanPrincipal", ArrayType(StringType(), True), True),
                                                StructField("product.endDate", ArrayType(StringType(), True), True),
                                                StructField("product.esPeque", ArrayType(StringType(), True), True),
                                                StructField("product.idTipoGrupo", ArrayType(StringType(), True), True),
                                                StructField("product.keywords", ArrayType(StringType(), True), True),
                                                StructField("product.language", ArrayType(StringType(), True), True),
                                                StructField("product.largeImage.url", ArrayType(StringType(), True), True),
                                                StructField("product.ldescr", ArrayType(StringType(), True), True),
                                                StructField("product.longDescription", ArrayType(StringType(), True), True),
                                                StructField("product.mediumImage.url", ArrayType(StringType(), True), True),
                                                StructField("product.nonreturnable", ArrayType(StringType(), True), True),
                                                StructField("product.priceListPair", ArrayType(StringType(), True), True),
                                                StructField("product.proveedorChico", ArrayType(StringType(), True), True),
                                                StructField("product.repositoryId", ArrayType(StringType(), True), True),
                                                StructField("product.sDisp_056", ArrayType(StringType(), True), True),
                                                StructField("product.sDisp_060", ArrayType(StringType(), True), True),
                                                StructField("product.sDisp_061", ArrayType(StringType(), True), True),
                                                StructField("product.sDisp_063", ArrayType(StringType(), True), True),
                                                StructField("product.sDisp_064", ArrayType(StringType(), True), True),
                                                StructField("product.sDisp_065", ArrayType(StringType(), True), True),
                                                StructField("product.sDisp_075", ArrayType(StringType(), True), True),
                                                StructField("product.sDisp_078", ArrayType(StringType(), True), True),
                                                StructField("product.sDisp_085", ArrayType(StringType(), True), True),
                                                StructField("product.sDisp_090", ArrayType(StringType(), True), True),
                                                StructField("product.sDisp_091", ArrayType(StringType(), True), True),
                                                StructField("product.sDisp_092", ArrayType(StringType(), True), True),
                                                StructField("product.sDisp_107", ArrayType(StringType(), True), True),
                                                StructField("product.sDisp_109", ArrayType(StringType(), True), True),
                                                StructField("product.sDisp_129", ArrayType(StringType(), True), True),
                                                StructField("product.sDisp_131", ArrayType(StringType(), True), True),
                                                StructField("product.sDisp_133", ArrayType(StringType(), True), True),
                                                StructField("product.sDisp_160", ArrayType(StringType(), True), True),
                                                StructField("product.sDisp_165", ArrayType(StringType(), True), True),
                                                StructField("product.sDisp_178", ArrayType(StringType(), True), True),
                                                StructField("product.sDisp_181", ArrayType(StringType(), True), True),
                                                StructField("product.sDisp_182", ArrayType(StringType(), True), True),
                                                StructField("product.sDisp_184", ArrayType(StringType(), True), True),
                                                StructField("product.sDisp_185", ArrayType(StringType(), True), True),
                                                StructField("product.sDisp_188", ArrayType(StringType(), True), True),
                                                StructField("product.sDisp_189", ArrayType(StringType(), True), True),
                                                StructField("product.sDisp_197", ArrayType(StringType(), True), True),
                                                StructField("product.sDisp_200", ArrayType(StringType(), True), True),
                                                StructField("product.sDisp_204", ArrayType(StringType(), True), True),
                                                StructField("product.sDisp_209", ArrayType(StringType(), True), True),
                                                StructField("product.sDisp_215", ArrayType(StringType(), True), True),
                                                StructField("product.sDisp_219", ArrayType(StringType(), True), True),
                                                StructField("product.sDisp_220", ArrayType(StringType(), True), True),
                                                StructField("product.sDisp_238", ArrayType(StringType(), True), True),
                                                StructField("product.sDisp_400", ArrayType(StringType(), True), True),
                                                StructField("product.sDisp_710", ArrayType(StringType(), True), True),
                                                StructField("product.saltoCantidad", ArrayType(StringType(), True), True),
                                                StructField("product.siteId", ArrayType(StringType(), True), True),
                                                StructField("product.startDate", ArrayType(StringType(), True), True),
                                                StructField("product.tipoOferta", ArrayType(StringType(), True), True),
                                                StructField("product.unidades.descUnidad", ArrayType(StringType(), True), True),
                                                StructField("product.unidades.esPesable", ArrayType(StringType(), True), True),
                                                StructField("product.url", ArrayType(StringType(), True), True),
                                                StructField("record.id", ArrayType(StringType(), True), True),
                                                StructField("record.source", ArrayType(StringType(), True), True),
                                                StructField("record.type", ArrayType(StringType(), True), True),
                                                StructField("sku.activePrice", ArrayType(StringType(), True), True),
                                                StructField("sku.baseUrl", ArrayType(StringType(), True), True),
                                                StructField("sku.creationDate", ArrayType(StringType(), True), True),
                                                StructField("sku.description", ArrayType(StringType(), True), True),
                                                StructField("sku.displayName", ArrayType(StringType(), True), True),
                                                StructField("sku.dtoPrice", ArrayType(StringType(), True), True),
                                                StructField("sku.endDate", ArrayType(StringType(), True), True),
                                                StructField("sku.margin", ArrayType(StringType(), True), True),
                                                StructField("sku.onSaleText", ArrayType(StringType(), True), True),
                                                StructField("sku.quantity", ArrayType(StringType(), True), True),
                                                StructField("sku.referencePrice", ArrayType(StringType(), True), True),
                                                StructField("sku.repositoryId", ArrayType(StringType(), True), True),
                                                StructField("sku.siteId", ArrayType(StringType(), True), True),
                                                StructField("sku.startDate", ArrayType(StringType(), True), True),
                                                StructField("sku.unit_of_measure", ArrayType(StringType(), True), True),
                                                StructField("sku.url", ArrayType(StringType(), True), True)
                                            ]), True),
                                            StructField("detailsAction", StructType([
                                                StructField("@class", StringType(), True),
                                                StructField("contentPath", StringType(), True),
                                                StructField("label", StringType(), True),
                                                StructField("recordState", StringType(), True),
                                                StructField("siteRootPath", StringType(), True),
                                                StructField("siteState", StructType([
                                                    StructField("@class", StringType(), True),
                                                    StructField("contentPath", StringType(), True),
                                                    StructField("matchedUrlPattern", StringType(), True),
                                                    StructField("siteDefinition", StructType([
                                                        StructField("@class", StringType(), True),
                                                        StructField("displayName", StringType(), True),
                                                        StructField("filterStateConfiguration", StringType(), True),
                                                        StructField("id", StringType(), True),
                                                        StructField("patterns", ArrayType(StringType(), True), True),
                                                        StructField("urlPattern", StringType(), True)
                                                    ]), True),
                                                    StructField("siteId", StringType(), True),
                                                    StructField("validSite", BooleanType(), True)
                                                ]), True)
                                            ]), True),
                                            StructField("numRecords", LongType(), True),
                                            StructField("records", StringType(), True)
                                        ]), True
                                    ), True)
                                ]), True
                            ), True),
                            StructField("recsPerPage", LongType(), True),
                            StructField("sortOptions", ArrayType(
                                StructType([
                                    StructField("@class", StringType(), True),
                                    StructField("contentPath", StringType(), True),
                                    StructField("label", StringType(), True),
                                    StructField("navigationState", StringType(), True),
                                    StructField("selected", BooleanType(), True),
                                    StructField("siteRootPath", StringType(), True),
                                    StructField("siteState", StructType([
                                        StructField("@class", StringType(), True),
                                        StructField("contentPath", StringType(), True),
                                        StructField("matchedUrlPattern", StringType(), True),
                                        StructField("siteDefinition", StructType([
                                            StructField("@class", StringType(), True),
                                            StructField("displayName", StringType(), True),
                                            StructField("filterStateConfiguration", StringType(), True),
                                            StructField("id", StringType(), True),
                                            StructField("patterns", ArrayType(StringType(), True), True),
                                            StructField("urlPattern", StringType(), True)
                                        ]), True),
                                        StructField("siteId", StringType(), True),
                                        StructField("validSite", BooleanType(), True)
                                    ]), True)
                                ]), True
                            ), True),
                            StructField("totalNumRecs", LongType(), True)
                        ]), True
                    ), True),
                    StructField("endeca:auditInfo", StructType([
                        StructField("ecr:innerPath", StringType(), True),
                        StructField("ecr:resourcePath", StringType(), True)
                    ]), True),
                    StructField("externo", BooleanType(), True),
                    StructField("imageURL", StringType(), True),
                    StructField("modal", BooleanType(), True),
                    StructField("name", StringType(), True),
                    StructField("ruleLimit", StringType(), True),
                    StructField("templateIds", ArrayType(StringType(), True), True),
                    StructField("templateTypes", ArrayType(StringType(), True), True),
                    StructField("urlRedirect", StringType(), True)
                ]), True
            ), True),
            StructField("descriptionSEO", StringType(), True),
            StructField("endeca:auditInfo", StructType([
                StructField("ecr:innerPath", StringType(), True),
                StructField("ecr:resourcePath", StringType(), True)
            ]), True),
            StructField("keywordsSEO", StringType(), True),
            StructField("name", StringType(), True),
            StructField("titleSEO", StringType(), True)
        ]), True
    )
    
    return cat_schema