from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg

spark = SparkSession.builder \
    .appName("MARTS_TO_CLICKHOUSE") \
    .getOrCreate()

pg_url = "jdbc:postgresql://postgres:5432/etl_db"
pg_props = {
    "user": "user",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

ch_url = "jdbc:clickhouse://clickhouse:8123/default"

fact = spark.read.jdbc(pg_url, "fact_sales", properties=pg_props)
dim_product = spark.read.jdbc(pg_url, "dim_product", properties=pg_props)
dim_customer = spark.read.jdbc(pg_url, "dim_customer", properties=pg_props)
dim_supplier = spark.read.jdbc(pg_url, "dim_supplier", properties=pg_props)


top_products = fact.groupBy("product_id") \
    .agg(sum("quantity").alias("total_qty")) \
    .orderBy("total_qty", ascending=False) \
    .limit(10)

revenue_by_category = fact.join(dim_product, "product_id") \
    .groupBy("product_category") \
    .agg(sum("total_price").alias("revenue"))

ratings = fact.join(dim_product, "product_id") \
    .groupBy("product_id") \
    .agg(avg("product_price").alias("avg_price"))


top_customers = fact.groupBy("customer_id") \
    .agg(sum("total_price").alias("total_spent")) \
    .orderBy("total_spent", ascending=False) \
    .limit(10)


time_sales = fact.groupBy("date") \
    .agg(sum("total_price").alias("daily_revenue"))


store_sales = fact.groupBy("store_id") \
    .agg(sum("total_price").alias("revenue")) \
    .orderBy("revenue", ascending=False) \
    .limit(5)


supplier_sales = fact.groupBy("supplier_id") \
    .agg(sum("total_price").alias("revenue")) \
    .orderBy("revenue", ascending=False) \
    .limit(5)


def write(df, table):
    df.write.format("jdbc") \
        .option("url", ch_url) \
        .option("dbtable", table) \
        .mode("overwrite") \
        .save()

write(top_products, "top_products")
write(revenue_by_category, "revenue_by_category")
write(ratings, "product_ratings")
write(top_customers, "top_customers")
write(time_sales, "time_sales")
write(store_sales, "store_sales")
write(supplier_sales, "supplier_sales")

spark.stop()