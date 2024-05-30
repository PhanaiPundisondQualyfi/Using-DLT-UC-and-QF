# federation

# ADDRESS
@dlt.table(
    comment="staging data for street address info",
    table_properties={"quality": "silver"}
)
def address_stg():
    return spark.table("resellerAzSqlDb_pp.saleslt.address")

# CUSTOMER
@dlt.table(
    comment="staging data for customer info",
    table_properties={"quality": "silver"}
)
def customer_stg():
    return spark.table("resellerAzSqlDb_pp.saleslt.customer")

# CUSTOMER ADDRESS
@dlt.table(
    comment="staging data for cross-reference table mapping customers to their address(es)",
    table_properties={"quality": "silver"}
)
def customer_address_stg():
    return spark.table("resellerAzSqlDb_pp.saleslt.customeraddress")

# PRODUCT
@dlt.table(
    comment="staging data for products sold or used in the manfacturing of sold products",
    table_properties={"quality": "silver"}
)
def product_stg():
    return spark.table("resellerAzSqlDb_pp.saleslt.product")

# PRODUCT CATEGORY
@dlt.table(
    comment="staging data for high-level product categorization",
    table_properties={"quality": "silver"}
)
def product_category_stg():
    return spark.table("resellerAzSqlDb_pp.saleslt.productcategory")

# PRODUCT DESCRIPTION
@dlt.table(
    comment="staging data for product description",
    table_properties={"quality": "silver"}
)
def product_description_stg():
    return spark.table("resellerAzSqlDb_pp.saleslt.productdescription")

# PRODUCT MODEL
@dlt.table(
    comment="staging data for product model",
    table_properties={"quality": "silver"}
)
def product_model_stg():
    return spark.table("resellerAzSqlDb_pp.saleslt.productmodel")

# PRODUCT MODEL PRODUCT DESCRIPTION
@dlt.table(
    comment="staging data for cross-reference table mapping product descriptions",
    table_properties={"quality": "silver"}
)
def product_model_product_description_stg():
    return spark.table("resellerAzSqlDb_pp.saleslt.productmodelproductdescription")

# SALES ORDER HEADER
@dlt.table(
    comment="staging data for general sales order",
    table_properties={"quality": "silver"}
)
def sales_order_header_stg():
    return spark.table("resellerAzSqlDb_pp.saleslt.salesorderheader")

# SALES ORDER DETAIL
@dlt.table(
    comment="staging data for individual products associated with a specific sales order",
    table_properties={"quality": "silver"}
)
def sales_order_detail_stg():
    return spark.table("resellerAzSqlDb_pp.saleslt.salesorderdetail")
