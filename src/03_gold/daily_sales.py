from pyspark import pipelines as dp

from pyspark.sql.functions import (
    sum,
    count,
    round
)


@dp.materialized_view(
    name="main.retail_gold.daily_sales",
    comment="Daily sales aggregated by region"
)

def daily_sales():

    return (

        spark.read.table(
            "main.retail_silver.orders_silver"
        )

        .groupBy(
            "order_date",
            "source_region"
        )

        .agg(

            sum("amount").alias(
                "total_sales"
            ),

            count("order_id").alias(
                "total_orders"
            ),

            sum("quantity").alias(
                "total_quantity"
            )

        )

    )
