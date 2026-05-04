from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, count, col, month, year

spark = SparkSession.builder.appName("MARTS_FULL").getOrCreate()

pg_url = "jdbc:postgresql://postgres:5432/etl_db"
pg_props = {"user": "user", "password": "password"}

ch_url = "jdbc:clickhouse://clickhouse:8123/default"

fact = spark.read.jdbc(pg_url, "fact_sales", properties=pg_props)
dim_product = spark.read.jdbc(pg_url, "dim_product", properties=pg_props)
dim_customer = spark.read.jdbc(pg_url, "dim_customer", properties=pg_props)
dim_supplier = spark.read.jdbc(pg_url, "dim_supplier", properties=pg_props)
dim_store = spark.read.jdbc(pg_url, "dim_store", properties=pg_props)

product_join = fact.join(dim_product, "product_id")

top_products = product_join.groupBy("product_name") \
    .agg(sum("quantity").alias("qty")) \
    .orderBy("qty", ascending=False).limit(10)

revenue_by_category = product_join.groupBy("product_category") \
    .agg(sum("total_price").alias("revenue"))

product_rating = product_join.groupBy("product_name") \
    .agg(avg("product_price").alias("avg_price"),
         count("*").alias("sales_count"))

customer_join = fact.join(dim_customer, "customer_id")

top_customers = customer_join.groupBy("customer_id") \
    .agg(sum("total_price").alias("spent")) \
    .orderBy("spent", ascending=False).limit(10)

customers_by_country = dim_customer.groupBy("customer_country") \
    .count()

avg_check_customer = customer_join.groupBy("customer_id") \
    .agg(avg("total_price").alias("avg_check"))


time_df = fact.withColumn("year", year("date")) \
              .withColumn("month", month("date"))

monthly_sales = time_df.groupBy("year", "month") \
    .agg(sum("total_price").alias("revenue"))

yearly_sales = time_df.groupBy("year") \
    .agg(sum("total_price").alias("revenue"))

avg_month_check = time_df.groupBy("year", "month") \
    .agg(avg("total_price").alias("avg_check"))


store_join = fact.join(dim_store, "store_id")

top_stores = store_join.groupBy("store_id") \
    .agg(sum("total_price").alias("revenue")) \
    .orderBy("revenue", ascending=False).limit(5)

sales_by_city = store_join.groupBy("store_city", "store_country") \
    .agg(sum("total_price").alias("revenue"))

avg_store_check = store_join.groupBy("store_id") \
    .agg(avg("total_price").alias("avg_check"))


supplier_join = fact.join(dim_supplier, "supplier_id")

top_suppliers = supplier_join.groupBy("supplier_id") \
    .agg(sum("total_price").alias("revenue")) \
    .orderBy("revenue", ascending=False).limit(5)

avg_supplier_price = supplier_join.groupBy("supplier_id") \
    .agg(avg("total_price").alias("avg_price"))

supplier_geo = dim_supplier.groupBy("supplier_country").count()


quality = product_join.groupBy("product_name") \
    .agg(avg("total_price").alias("avg_sales"),
         count("*").alias("sales_count"))

top_rated = quality.orderBy("avg_sales", ascending=False).limit(5)
worst_rated = quality.orderBy("avg_sales", ascending=True).limit(5)


def write(df, table):
    df.write.format("jdbc") \
        .option("url", ch_url) \
        .option("dbtable", table) \
        .mode("overwrite") \
        .save()

tables = {
    "top_products": top_products,
    "revenue_by_category": revenue_by_category,
    "product_rating": product_rating,
    "top_customers": top_customers,
    "customers_by_country": customers_by_country,
    "avg_check_customer": avg_check_customer,
    "monthly_sales": monthly_sales,
    "yearly_sales": yearly_sales,
    "avg_month_check": avg_month_check,
    "top_stores": top_stores,
    "sales_by_city": sales_by_city,
    "avg_store_check": avg_store_check,
    "top_suppliers": top_suppliers,
    "avg_supplier_price": avg_supplier_price,
    "supplier_geo": supplier_geo,
    "top_rated": top_rated,
    "worst_rated": worst_rated
}

for name, df in tables.items():
    write(df, name)

spark.stop()
