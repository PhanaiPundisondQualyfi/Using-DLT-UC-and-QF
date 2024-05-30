# modelling

# ADDRESS
@dlt.table(
    comment="aggregated data for address",
    table_properties={"quality": "gold"}
)
@dlt.expect_or_drop("valid AddressID", "AddressID IS NOT NULL")
def address():
    dfa = dlt.read("address_stg")
    dfa = dfa.drop("AddressLine1", 
                   "AddressLine2", 
                   "PostalCode", 
                   "rowguid", 
                   "ModifiedDate")
    return dfa

# CUSTOMER
@dlt.table(
    comment="aggregated data for customer",
    table_properties={"quality": "gold"}
)
@dlt.expect_or_drop("valid CustomerID", "CustomerID IS NOT NULL")
def customer():
    dfc = dlt.read("customer_stg")
    dfc = dfc.drop("NameStyle", 
                  "FirstName", 
                  "MiddleName",
                  "LastName",
                  "Suffix",
                  "SalesPerson",
                  "EmailAddress",
                  "Phone",
                  "PasswordHash",
                  "PasswordSalt",
                  "rowguid",
                  "ModifiedDate")
    return dfc

# CUSTOMER ADDRESS
@dlt.table(
    comment="aggregated data for customer address",
    table_properties={"quality": "gold"}
)
@dlt.expect_or_drop("valid AddressID", "AddressID IS NOT NULL")
@dlt.expect_or_drop("valid CustomerID", "CustomerID IS NOT NULL")
def customer_address():
    dfca = dlt.read("customer_address_stg")
    dfca = dfca.drop("rowguid",
                     "ModifiedDate")
    return dfca

# PRODUCT
@dlt.table(
    comment="aggregated data for product",
    table_properties={"quality": "gold"}
)
@dlt.expect_or_drop("valid ProductID", "ProductID IS NOT NULL")
def product():
    dfp = dlt.read("product_stg")
    dfp = dfp.drop("ProductNumber",
                  "Color",
                  "Size",
                  "Weight",
                  "SellStartDate",
                  "SellEndDate",
                  "DiscontinuedDate",
                  "ThumbNailPhoto",
                  "ThumbnailPhotoFileName",
                  "rowguid",
                  "ModifiedDate")
    return dfp

# PRODUCT CATEGORY
@dlt.table(
    comment="aggregated data for product category",
    table_properties={"quality": "gold"}
)
@dlt.expect_or_drop("valid ProductCategoryID", "ProductCategoryID IS NOT NULL")
def product_category():
    dfpc = dlt.read("product_category_stg")
    dfpc = dfpc.drop("ParentProductCategoryID",
                     "rowguid",
                     "ModifiedDate")
    return dfpc

# PRODUCT MODEL
@dlt.table(
    comment="aggregated data for product model",
    table_properties={"quality": "gold"}
)
@dlt.expect_or_drop("valid ProductModelID", "ProductModelID IS NOT NULL")
def product_model():
    dfpm = dlt.read("product_model_stg")
    dfpm = dfpm.drop("CatalogDescription",
                     "rowguid",
                     "ModifiedDate")
    return dfpm

# ORDER
@dlt.table(
    comment="aggregated data for sales order header",
    table_properties={"quality": "gold"}
)
@dlt.expect_or_drop("valid SalesOrderID", "SalesOrderID IS NOT NULL")
@dlt.expect_or_drop("valid CustomerID", "CustomerID IS NOT NULL")
@dlt.expect_or_drop("valid ShipToAddressID", "ShipToAddressID IS NOT NULL")
@dlt.expect_or_drop("valid BillToAddressID", "BillToAddressID IS NOT NULL")
def order():
    dfo = dlt.read("sales_order_header_stg")
    dfo = dfo.drop("RevisionNumber",
                   "OrderDate",
                   "DueDate",
                   "ShipDate",
                   "Status",
                   "OnlineOrderFlag",
                   "SalesOrderNumber",
                   "PurchaseOrderNumber",
                   "AccountNumber",
                   "ShipMethod",
                   "CreditCardApprovalCode",
                   "Comment",
                   "rowguid",
                   "ModifiedDate")
    return dfo

# SALES
@dlt.table(
    comment="aggregated data for fact table",
    table_properties={"quality": "gold"}
)
@dlt.expect_or_drop("valid SalesOrderID", "SalesOrderID IS NOT NULL")
@dlt.expect_or_drop("valid SalesOrderDetailID", "SalesOrderDetailID IS NOT NULL")
@dlt.expect_or_drop("valid ProductID", "ProductID IS NOT NULL")
def sales():
    dfs = dlt.read("sales_order_detail_stg")
    dfs = dfs.drop("rowguid",
                   "ModifiedDate")
    return dfs
