from pyspark import pipelines as dp

from pyspark.sql.functions import (
    lit,
    current_timestamp,
    col
)

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType
)


# ---------------------------------
# 1. Configuration
# ---------------------------------

BASE_PATH = (
    "/Volumes/main/retail_bronze/raw_orders"
)

TARGET_TABLE = (
    "main.retail_bronze.orders_bronze"
)


# ---------------------------------
# 2. Source Schema
# ---------------------------------

orders_schema = StructType([

    StructField("order_id", StringType(), True),

    StructField("customer_id", StringType(), True),

    StructField("product_id", StringType(), True),

    StructField("quantity", IntegerType(), True),

    StructField("amount", DecimalType(18, 2), True),

    StructField("order_date", StringType(), True)

])


# ---------------------------------
# 3. Create Target Streaming Table
# ---------------------------------

dp.create_streaming_table(
    name=TARGET_TABLE,
    comment="Bronze orders from multiple regions"
)


# ---------------------------------
# 4. Common ingestion function
# ---------------------------------

def read_orders(region):

    source_path = f"{BASE_PATH}/{region.lower()}/"

    return (

        spark.readStream

        .format("cloudFiles")

        .option("cloudFiles.format", "csv")

        .option("header", "true")

        .schema(orders_schema)

        .load(source_path)

        .withColumn(
            "source_region",
            lit(region)
        )

        .withColumn(
            "ingested_at",
            current_timestamp()
        )

    )


# ---------------------------------
# 5. US Flow
# ---------------------------------

@dp.append_flow(
    target=TARGET_TABLE,
    name="orders_us_flow"
)

def orders_us_flow():

    return read_orders("US")


# ---------------------------------
# 6. EU Flow
# ---------------------------------

@dp.append_flow(
    target=TARGET_TABLE,
    name="orders_eu_flow"
)

def orders_eu_flow():

    return read_orders("EU")


# ---------------------------------
# 7. APAC Flow
# ---------------------------------

@dp.append_flow(
    target=TARGET_TABLE,
    name="orders_apac_flow"
)

def orders_apac_flow():

    return read_orders("APAC")
