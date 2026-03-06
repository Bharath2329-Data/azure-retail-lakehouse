# COMMAND ----------

storage_account = "retaillakehousebharath"

bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/"
silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/clean/"

print(bronze_path)
print(silver_path)

# COMMAND ----------

display(dbutils.fs.ls(bronze_path))

# COMMAND ----------

customers_path = bronze_path + "customers.csv"

df_customers = (spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv(customers_path)
)

display(df_customers)
df_customers.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, trim, lower, to_date

df_customers = (spark.read.option("header", True).option("inferSchema", True)
  .csv(bronze_path + "customers.csv"))

df_customers_silver = (df_customers
  .withColumn("customer_id", trim(col("customer_id")))
  .withColumn("first_name", trim(col("first_name")))
  .withColumn("last_name", trim(col("last_name")))
  .withColumn("email", lower(trim(col("email"))))
  .withColumn("phone", trim(col("phone")))
  .withColumn("signup_date", to_date(col("signup_date")))
  .dropDuplicates(["customer_id"])
)

(df_customers_silver.write.mode("overwrite").format("delta")
  .save(silver_path + "customers"))

# COMMAND ----------

display(spark.read.format("delta").load(silver_path + "customers"))

# COMMAND ----------

storage_account = "retaillakehousebharath"

bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/"
silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/clean/"

# COMMAND ----------

(df_customers_silver.write
 .format("delta")
 .mode("overwrite")
 .save(silver_path + "customers"))

# COMMAND ----------

display(dbutils.fs.ls(silver_path))

# COMMAND ----------

df_silver = spark.read.format("delta").load(silver_path + "customers")
display(df_silver)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import col, trim, lower, to_date

# 1) Define schemas (so types are correct in Silver)
schemas = {
  "customers": StructType([
      StructField("customer_id", StringType(), True),
      StructField("first_name", StringType(), True),
      StructField("last_name", StringType(), True),
      StructField("email", StringType(), True),
      StructField("phone", StringType(), True),
      StructField("signup_date", StringType(), True),
      StructField("loyalty_tier", StringType(), True),
  ]),
  "products": StructType([
      StructField("product_id", StringType(), True),
      StructField("product_name", StringType(), True),
      StructField("category", StringType(), True),
      StructField("brand", StringType(), True),
      StructField("price", DoubleType(), True),
  ]),
  "stores": StructType([
      StructField("store_id", StringType(), True),
      StructField("store_name", StringType(), True),
      StructField("city", StringType(), True),
      StructField("state", StringType(), True),
  ]),
  "order_id": StructType([
      StructField("order_id", StringType(), True),
      StructField("customer_id", StringType(), True),
      StructField("store_id", StringType(), True),
      StructField("order_date", StringType(), True),
      StructField("order_status", StringType(), True),
  ]),
  "order_item": StructType([
      StructField("order_id", StringType(), True),
      StructField("product_id", StringType(), True),
      StructField("quantity", IntegerType(), True),
      StructField("unit_price", DoubleType(), True),
  ]),
  "payment_id": StructType([
      StructField("payment_id", StringType(), True),
      StructField("order_id", StringType(), True),
      StructField("payment_method", StringType(), True),
      StructField("amount", DoubleType(), True),
      StructField("payment_date", StringType(), True),
  ])
}

# 2) Transform functions (cleaning rules)
def transform_customers(df):
    return (df
      .withColumn("customer_id", trim(col("customer_id")))
      .withColumn("first_name", trim(col("first_name")))
      .withColumn("last_name", trim(col("last_name")))
      .withColumn("email", lower(trim(col("email"))))
      .withColumn("phone", trim(col("phone")))
      .withColumn("signup_date", to_date(col("signup_date")))
      .dropDuplicates(["customer_id"])
    )

def transform_products(df):
    return (df
      .withColumn("product_id", trim(col("product_id")))
      .withColumn("product_name", trim(col("product_name")))
      .withColumn("category", trim(col("category")))
      .withColumn("brand", trim(col("brand")))
      .dropDuplicates(["product_id"])
    )

def transform_stores(df):
    return (df
      .withColumn("store_id", trim(col("store_id")))
      .withColumn("store_name", trim(col("store_name")))
      .withColumn("city", trim(col("city")))
      .withColumn("state", trim(col("state")))
      .dropDuplicates(["store_id"])
    )

def transform_orders(df):
    return (df
      .withColumn("order_id", trim(col("order_id")))
      .withColumn("customer_id", trim(col("customer_id")))
      .withColumn("store_id", trim(col("store_id")))
      .withColumn("order_date", to_date(col("order_date")))
      .withColumn("order_status", trim(col("order_status")))
      .dropDuplicates(["order_id"])
    )

def transform_order_items(df):
    return (df
      .withColumn("order_id", trim(col("order_id")))
      .withColumn("product_id", trim(col("product_id")))
      .dropDuplicates(["order_id", "product_id"])
    )

def transform_payments(df):
    return (df
      .withColumn("payment_id", trim(col("payment_id")))
      .withColumn("order_id", trim(col("order_id")))
      .withColumn("payment_method", trim(col("payment_method")))
      .withColumn("payment_date", to_date(col("payment_date")))
      .dropDuplicates(["payment_id"])
    )

transforms = {
  "customers": transform_customers,
  "products": transform_products,
  "stores": transform_stores,
  "order_id": transform_orders,
  "order_item": transform_order_items,
  "payment_id": transform_payments
}

# COMMAND ----------

tables = ["customers", "products", "stores", "order_id", "order_item", "payment_id"]

for t in tables:
    bronze_file = bronze_path + f"{t}.csv"
    silver_table_path = silver_path + f"{t}/"

    print(f"\n=== Processing {t} ===")
    print("Bronze:", bronze_file)
    print("Silver:", silver_table_path)

    df = (spark.read
          .option("header", "true")
          .schema(schemas[t])
          .csv(bronze_file))

    df_silver = transforms[t](df)

    (df_silver.write
        .format("delta")
        .mode("overwrite")
        .save(silver_table_path))

    print(f"✅ Done: {t} rows =", df_silver.count())

# COMMAND ----------

storage_account = "retaillakehousebharath"

bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/"
silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/clean/"

# COMMAND ----------

df_orders = (spark.read
  .option("header", "true")
  .csv(bronze_path + "order_id.csv")
)

display(df_orders)
df_orders.printSchema()

# COMMAND ----------

from pyspark.sql.functions import to_date

df_orders_silver = df.withColumn(
    "order_date",
    to_date("order_date", "yyyy-MM-dd")
)

# COMMAND ----------

from pyspark.sql.functions import expr

df_orders_silver = df.withColumn(
    "order_date",
    expr("try_to_date(order_date, 'yyyy-MM-dd')")
)

# COMMAND ----------

(df_orders_silver.write
 .format("delta")
 .mode("overwrite")
 .save(silver_path + "order_id/")
)

# COMMAND ----------

display(dbutils.fs.ls(silver_path + "order_id/"))

# COMMAND ----------

display(
    spark.read.format("delta").load(silver_path + "order_id/")
)

# COMMAND ----------

customers_silver = spark.read.option("header","true").csv(bronze_path + "customers.csv")

display(customers_silver)

# COMMAND ----------

(customers_silver.write
 .format("delta")
 .mode("overwrite")
 .save(silver_path + "customers/")
)

print("Saved customers to:", silver_path + "customers/")

# COMMAND ----------

dbutils.fs.rm(silver_path + "customers/", True)

# COMMAND ----------

dbutils.fs.ls(silver_path)


# COMMAND ----------

customers_silver = spark.read.option("header","true").csv(bronze_path + "customers.csv")

display(customers_silver)

# COMMAND ----------

(customers_silver.write
 .format("delta")
 .mode("overwrite")
 .save(silver_path + "customers/")
)

# COMMAND ----------

dbutils.fs.ls(silver_path)

# COMMAND ----------

display(dbutils.fs.ls(silver_path + "customers/"))

# COMMAND ----------

df_check = spark.read.format("delta").load(silver_path + "customers/")
display(df_check)
df_check.count()

# COMMAND ----------

(products_silver.write
 .format("delta")
 .mode("overwrite")
 .save(silver_path + "products/")
)

# COMMAND ----------

products_silver = spark.read.option("header","true").csv(bronze_path + "products.csv")

display(products_silver)

# COMMAND ----------

(products_silver.write
 .format("delta")
 .mode("overwrite").option("mergeSchema", "true")
 .save(silver_path + "products/")
)

# COMMAND ----------

dbutils.fs.ls(silver_path)

# COMMAND ----------

df_products = spark.read.format("delta").load(silver_path + "products/")
display(df_products)
df_products.count()
