from pyspark import pipelines as dp

from pyspark.sql.functions import (
    col,
    to_date,
    to_timestamp
)


@dp.table(
    name="main.retail_silver.orders_silver",
    comment="Cleaned and validated retail orders"
)

def orders_silver():

    df = (

        spark.readStream

        .table("main.retail_bronze.orders_bronze")

    )

    cleaned_df = (

        df

        # Data Quality
        .filter(col("order_id").isNotNull())

        .filter(col("customer_id").isNotNull())

        .filter(col("quantity") > 0)

        .filter(col("amount") > 0)

        # Date conversion
        .withColumn(
            "order_date",
            to_date(col("order_date"), "yyyy-MM-dd")
        )

        .withColumn(
            "order_timestamp",
            to_timestamp(col("order_date"))
        )

    )

    # Streaming deduplication
    result = (

        cleaned_df

        .withWatermark(
            "order_timestamp",
            "30 days"
        )

        .dropDuplicates([
            "source_region",
            "order_id"
        ])

    )

    return result
