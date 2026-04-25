from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, monotonically_increasing_id

spark = SparkSession.builder \
    .appName("ETL_TO_STAR") \
    .getOrCreate()

url = "jdbc:postgresql://postgres:5432/etl_db"
props = {
    "user": "user",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

df = spark.read.jdbc(url=url, table="mock_data", properties=props)


dim_customer = df.select(
    col("sale_customer_id").alias("customer_id"),
    "customer_first_name",
    "customer_last_name",
    "customer_age",
    "customer_email",
    "customer_country",
    "customer_postal_code"
).dropDuplicates()

dim_product = df.select(
    col("sale_product_id").alias("product_id"),
    "product_name",
    "product_category",
    "product_price",
    "product_weight",
    "product_color",
    "product_size",
    "product_brand",
    "product_material",
    "product_description",
    "product_release_date",
    "product_expiry_date"
).dropDuplicates()

dim_seller = df.select(
    col("sale_seller_id").alias("seller_id"),
    "seller_first_name",
    "seller_last_name",
    "seller_email",
    "seller_country",
    "seller_postal_code"
).dropDuplicates()

dim_store = df.select(
    "store_name",
    "store_location",
    "store_city",
    "store_state",
    "store_country",
    "store_phone",
    "store_email"
).dropDuplicates() \
.withColumn("store_id", monotonically_increasing_id())

dim_supplier = df.select(
    "supplier_name",
    "supplier_contact",
    "supplier_email",
    "supplier_phone",
    "supplier_address",
    "supplier_city",
    "supplier_country"
).dropDuplicates() \
.withColumn("supplier_id", monotonically_increasing_id())

dim_time = df.select("sale_date").dropDuplicates() \
.withColumnRenamed("sale_date", "date") \
.withColumn("year", year("date")) \
.withColumn("month", month("date")) \
.withColumn("day", dayofmonth("date"))


fact = df \
    .join(dim_store, ["store_name", "store_city", "store_country"]) \
    .join(dim_supplier, ["supplier_name", "supplier_country"]) \
    .join(dim_time, df.sale_date == dim_time.date)

fact_sales = fact.select(
    col("id").alias("sale_id"),
    col("sale_customer_id").alias("customer_id"),
    col("sale_seller_id").alias("seller_id"),
    col("sale_product_id").alias("product_id"),
    "store_id",
    "supplier_id",
    col("date"),
    col("sale_quantity").alias("quantity"),
    col("sale_total_price").alias("total_price")
)


dim_customer.write.jdbc(url, "dim_customer", "overwrite", props)
dim_product.write.jdbc(url, "dim_product", "overwrite", props)
dim_seller.write.jdbc(url, "dim_seller", "overwrite", props)
dim_store.write.jdbc(url, "dim_store", "overwrite", props)
dim_supplier.write.jdbc(url, "dim_supplier", "overwrite", props)
dim_time.write.jdbc(url, "dim_time", "overwrite", props)
fact_sales.write.jdbc(url, "fact_sales", "overwrite", props)

spark.stop()