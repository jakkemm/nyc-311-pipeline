from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from nyc311 import config
from nyc311.utils.spark import raw_path

NYC311_SCHEMA = StructType([
    # Identifiers and timestamps
    StructField("unique_key", StringType(), True),
    StructField("created_date", TimestampType(), True),
    StructField("closed_date", TimestampType(), True),
    # Complaint information
    StructField("agency", StringType(), True),
    StructField("agency_name", StringType(), True),
    StructField("complaint_type", StringType(), True),
    StructField("descriptor", StringType(), True),
    StructField("descriptor_2", StringType(), True),
    StructField("location_type", StringType(), True),
    # Address information
    StructField("incident_zip", StringType(), True),
    StructField("incident_address", StringType(), True),
    StructField("street_name", StringType(), True),
    StructField("cross_street_1", StringType(), True),
    StructField("cross_street_2", StringType(), True),
    StructField("intersection_street_1", StringType(), True),
    StructField("intersection_street_2", StringType(), True),
    StructField("address_type", StringType(), True),
    StructField("city", StringType(), True),
    StructField("landmark", StringType(), True),
    StructField("facility_type", StringType(), True),
    # Request status and resolution
    StructField("status", StringType(), True),
    StructField("due_date", TimestampType(), True),
    StructField("resolution_description", StringType(), True),
    StructField("resolution_action_updated_date", TimestampType(), True),
    # Administrative geography
    StructField("community_board", StringType(), True),
    StructField("council_district", StringType(), True),
    StructField("police_precinct", StringType(), True),
    StructField("bbl", StringType(), True),
    StructField("borough", StringType(), True),
    # State plane coordinates
    StructField("x_coordinate_state_plane", DoubleType(), True),
    StructField("y_coordinate_state_plane", DoubleType(), True),
    # Request channel and location details
    StructField("open_data_channel_type", StringType(), True),
    StructField("park_facility_name", StringType(), True),
    StructField("park_borough", StringType(), True),
    StructField("vehicle_type", StringType(), True),
    StructField("taxi_company_borough", StringType(), True),
    StructField("taxi_pick_up_location", StringType(), True),
    StructField("bridge_highway_name", StringType(), True),
    StructField("bridge_highway_direction", StringType(), True),
    StructField("road_ramp", StringType(), True),
    StructField("bridge_highway_segment", StringType(), True),
    # Geographic coordinates
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    # `location` (nested GeoJSON Point) is dropped: latitude/longitude already carry it.
])

TARGET_FIELDS = [f for f in NYC311_SCHEMA.fields]
STRING_COLS = [f.name for f in TARGET_FIELDS if isinstance(f.dataType, StringType)]

# Values that mean "no value" even though they are not NULL
PLACEHOLDERS = ["", "N/A", "NULL", "UNSPECIFIED", "UNKNOWN"]
VALID_BOROUGHS = ["BRONX", "BROOKLYN", "MANHATTAN", "QUEENS", "STATEN ISLAND"]
BOROUGH_COLS = ["borough", "park_borough", "taxi_company_borough"]


def read_raw(spark: SparkSession, day: date) -> DataFrame:
    """Read the original .jsonl.gz as raw strings."""
    all_strings = StructType(
        [StructField(f.name, StringType(), True) for f in TARGET_FIELDS]
    )
    path = raw_path(day)
    return spark.read.schema(all_strings).json(str(path))

def write_clean(df: DataFrame) -> None:
    """Write Parquet partitioned by created_dt."""
    (
        df.write.mode("overwrite")
        .partitionBy("created_dt")
        .parquet(str(config.CLEAN_DIR))
    )


def cast_types(df: DataFrame) -> DataFrame:
    """Cast string columns to the types in NYC311_SCHEMA."""
    return df.select(*[F.col(f.name).cast(f.dataType).alias(f.name) for f in TARGET_FIELDS])

def placeholders_to_null(df: DataFrame) -> DataFrame:
    """Trim strings and turn empty/placeholder values into real NULLs."""
    for c in STRING_COLS:
        trimmed = F.trim(F.col(c))
        df = df.withColumn(
            c, F.when(F.upper(trimmed).isin(PLACEHOLDERS), None).otherwise(trimmed)
        )
    return df

def drop_unusable_rows(df: DataFrame) -> DataFrame:
    """Only drop rows the pipeline itself can't work with (no key, or no creation time)."""
    return df.filter(F.col("unique_key").isNotNull() & F.col("created_date").isNotNull())

def normalize(df: DataFrame) -> DataFrame:
    """Makes boroughs uppercase. Boroughs outside the known set become NULL."""
    df = df.withColumn("agency", F.upper(F.col("agency")))
    for c in BOROUGH_COLS:
        upper = F.upper(F.col(c))
        df = df.withColumn(c, F.when(upper.isin(VALID_BOROUGHS), upper))
    return df

def add_derived(df: DataFrame) -> DataFrame:
    """Partition date and response time (NULL if still open or if closed before created)."""
    hours = (F.unix_timestamp("closed_date") - F.unix_timestamp("created_date")) / 3600
    return (
        df.withColumn("created_dt", F.to_date("created_date"))
        .withColumn("response_hours", F.when(hours >= 0, hours))
    )


def clean(df: DataFrame) -> DataFrame:
    return (
        df.transform(cast_types)
        .transform(placeholders_to_null)
        .transform(drop_unusable_rows)
        .transform(normalize)
        .transform(add_derived)
    )


def clean_day(spark: SparkSession, day: date) -> None:
    write_clean(clean(read_raw(spark, day)))
