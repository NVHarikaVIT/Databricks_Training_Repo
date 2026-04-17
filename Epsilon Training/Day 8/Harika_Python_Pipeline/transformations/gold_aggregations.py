from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="dev.harika_gold.destination_summary",
    comment="Aggregated booking and review metrics per destination"
)
def gold_destination_summary():
    bookings = spark.read.table("dev.harika_silver.bookings_cleaned_ps")
    properties = spark.read.table("dev.harika_silver.properties_cleaned_ps")
    reviews = spark.read.table("dev.harika_silver.bookings_cleaned_ps")

    booking_metrics = (
        bookings.join(properties, "property_id", "inner")
        .groupBy("destination_id", "destination_name", "destination_country")
        .agg(
            F.count("booking_id").alias("total_bookings"),
            F.sum("total_amount").alias("total_revenue"),
            F.round(F.avg("total_amount"), 2).alias("avg_booking_amount"),
            F.round(F.avg("stay_duration_days"), 1).alias("avg_stay_days"),
            F.countDistinct("user_id").alias("unique_guests")
        )
    )

    review_metrics = (
        reviews.join(properties, "property_id", "inner")
        .groupBy("destination_id")
        .agg(
            F.count("review_id").alias("total_reviews"),
            F.round(F.avg("rating"), 2).alias("avg_rating")
        )
    )

    return booking_metrics.join(review_metrics, "destination_id", "left")


@dp.materialized_view(
    name="dev.harika_gold.host_performance",
    comment="Host-level performance metrics including revenue, bookings, and ratings"
)
def gold_host_performance():
    bookings = spark.read.table("dev.harika_silver.bookings_cleaned_ps")
    properties = spark.read.table("dev.harika_silver.properties_cleaned_ps")
    reviews = spark.read.table("dev.harika_silver.bookings_cleaned_ps")

    return (
        bookings.join(properties, "property_id", "inner")
        .join(reviews, ["property_id"], "left")
        .groupBy("host_id")
        .agg(
            F.countDistinct("property_id").alias("total_properties"),
            F.countDistinct("booking_id").alias("total_bookings"),
            F.sum("total_amount").alias("total_revenue"),
            F.round(F.avg("total_amount"), 2).alias("avg_booking_amount"),
            F.round(F.avg("rating"), 2).alias("avg_rating"),
            F.count("review_id").alias("total_reviews")
        )
    )
