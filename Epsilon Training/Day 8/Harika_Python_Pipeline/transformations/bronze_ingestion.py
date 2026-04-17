from pyspark import pipelines as dp
from pyspark.sql.functions import col

@dp.table
def bronze_users():
    return spark.read.table("samples.wanderbricks.users")


@dp.table
def bronze_properties():
    return spark.read.table("samples.wanderbricks.properties")


@dp.table
def bronze_destinations():
    return spark.read.table("samples.wanderbricks.destinations")


@dp.table
def bronze_bookings():
    return spark.read.table("samples.wanderbricks.bookings")


@dp.table
def bronze_reviews():
    return spark.read.table("samples.wanderbricks.reviews")
