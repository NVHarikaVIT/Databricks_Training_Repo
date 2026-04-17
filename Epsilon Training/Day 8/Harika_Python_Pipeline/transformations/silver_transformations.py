from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="dev.harika_silver.users_cleaned_ps",
    comment="Cleaned users with validated email and trimmed names"
)
@dp.expect_or_drop("valid_user_id", "user_id IS NOT NULL")
@dp.expect_or_drop("valid_email", "email IS NOT NULL")
@dp.expect("valid_name", "name IS NOT NULL")
def silver_users():
    return (
        spark.read.table("bronze_users")
        .withColumn("name", F.trim(F.col("name")))
        .withColumn("email", F.lower(F.trim(F.col("email"))))
        .withColumn("country", F.trim(F.col("country")))
    )


@dp.table(
    name="dev.harika_silver.properties_cleaned_ps",
    comment="Properties enriched with destination details"
)
@dp.expect_or_drop("valid_property_id", "property_id IS NOT NULL")
@dp.expect("valid_base_price", "base_price > 0")
def silver_properties():
    properties = spark.read.table("bronze_properties")
    destinations = spark.read.table("bronze_destinations").drop("description")
    return (
        properties.join(destinations, "destination_id", "left")
        .select(
            "property_id",
            "host_id",
            "title",
            properties["description"],
            "base_price",
            "property_type",
            "max_guests",
            "bedrooms",
            "bathrooms",
            "destination_id",
            destinations["destination"].alias("destination_name"),
            destinations["country"].alias("destination_country"),
            destinations["state_or_province"],
            "property_latitude",
            "property_longitude",
            properties["created_at"]
        )
    )


@dp.table(
    name="dev.harika_silver.bookings_cleaned_ps",
    comment="Bookings with computed stay duration and validated fields"
)
@dp.expect_or_drop("valid_booking_id", "booking_id IS NOT NULL")
@dp.expect_or_drop("valid_dates", "check_in IS NOT NULL AND check_out IS NOT NULL")
@dp.expect("valid_amount", "total_amount > 0")
def silver_bookings():
    return (
        spark.read.table("bronze_bookings")
        .withColumn("stay_duration_days", F.datediff(F.col("check_out"), F.col("check_in")))
        .withColumn("price_per_night", F.col("total_amount") / F.datediff(F.col("check_out"), F.col("check_in")))
    )


@dp.table(
    name="dev.harika_silver.reviews_cleaned_ps",
    comment="Active reviews filtered out deleted ones with validated ratings"
)
@dp.expect_or_drop("valid_review_id", "review_id IS NOT NULL")
@dp.expect("valid_rating", "rating >= 1.0 AND rating <= 5.0")
def silver_reviews():
    return (
        spark.read.table("bronze_reviews")
        .filter(F.col("is_deleted") == False)
    )
