"""
transform_weather.py

Bronze -> Silver PySpark transformation for OpenWeather ingestion data.

Reads raw JSON weather snapshots from data/raw/weather/, flattens and
type-casts them against an explicit schema, applies data quality checks,
deduplicates on (city, observation_timestamp) keeping the latest
ingestion, and writes the result as Parquet to data/processed/weather/.

Run:
    python src/transformation/weather/transform_weather.py
    spark-submit src/transformation/weather/transform_weather.py
"""

import logging
import os
import shutil
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


# ============================================================
# Logger
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger("weather_transformation")


# ============================================================
# Windows-only Hadoop setup (winutils.exe)
# ============================================================

def configure_hadoop_home_for_windows() -> None:
    """
    On Windows, Spark's local file writer (used for the Parquet
    output) needs winutils.exe to set file permissions, even though
    no real Hadoop cluster is involved.

    Rather than relying on a system-wide HADOOP_HOME environment
    variable, set it in-process, only on Windows, only if it isn't
    already set. This keeps the fix self-contained to this script and
    has no effect on Linux (e.g. inside Docker), where winutils.exe
    doesn't exist and isn't needed.

    The path defaults to C:\\hadoop but can be overridden by setting
    a HADOOP_HOME environment variable before running the script, in
    case winutils.exe lives somewhere else on your machine.
    """

    if not sys.platform.startswith("win"):
        return

    if os.environ.get("HADOOP_HOME"):
        logger.info(
            f"HADOOP_HOME already set to {os.environ['HADOOP_HOME']}, leaving as is."
        )
        return

    default_hadoop_home = r"C:\hadoop"
    winutils_path = Path(default_hadoop_home) / "bin" / "winutils.exe"

    if not winutils_path.exists():
        logger.warning(
            f"winutils.exe not found at {winutils_path}. "
            "Parquet writes on Windows will likely fail. "
            "Download winutils.exe for your Hadoop version and place it "
            f"in {Path(default_hadoop_home) / 'bin'}, or set a HADOOP_HOME "
            "environment variable pointing at the correct folder."
        )

    os.environ["HADOOP_HOME"] = default_hadoop_home
    logger.info(f"Set HADOOP_HOME={default_hadoop_home} for this run (Windows only).")


configure_hadoop_home_for_windows()


# ============================================================
# Project root discovery
# ============================================================

def find_project_root(marker: str = ".env", start: Path = None) -> Path:
    """
    Walk upward from `start` (default: this file's directory) until a
    folder containing `marker` (e.g. .env) is found.

    This avoids hardcoding how many folders deep the script lives, so
    it keeps working even if the file is moved within the project.
    """

    current = (start or Path(__file__).resolve().parent)

    for parent in [current, *current.parents]:
        if (parent / marker).exists():
            return parent

    # Fallback: if no marker was found anywhere above, assume the
    # current file's directory is the root.
    logger.warning(
        f"Could not find '{marker}' above {current}. "
        f"Falling back to {current} as project root."
    )
    return current


PROJECT_ROOT = find_project_root(".env")

INPUT_DIR = PROJECT_ROOT / "data" / "raw" / "weather"
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed" / "weather"


# ============================================================
# Explicit schema for the raw OpenWeather JSON
# ============================================================

def build_raw_weather_schema() -> StructType:
    """
    Explicit StructType matching the raw OpenWeather API response,
    as saved by the ingestion step (with the extra city / latitude /
    longitude / ingestion_timestamp fields appended).
    """

    coord_schema = StructType([
        StructField("lon", DoubleType(), True),
        StructField("lat", DoubleType(), True),
    ])

    weather_item_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("main", StringType(), True),
        StructField("description", StringType(), True),
        StructField("icon", StringType(), True),
    ])

    main_schema = StructType([
        StructField("temp", DoubleType(), True),
        StructField("feels_like", DoubleType(), True),
        StructField("temp_min", DoubleType(), True),
        StructField("temp_max", DoubleType(), True),
        StructField("pressure", IntegerType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("sea_level", IntegerType(), True),
        StructField("grnd_level", IntegerType(), True),
    ])

    wind_schema = StructType([
        StructField("speed", DoubleType(), True),
        StructField("deg", IntegerType(), True),
        StructField("gust", DoubleType(), True),
    ])

    clouds_schema = StructType([
        StructField("all", IntegerType(), True),
    ])

    sys_schema = StructType([
        StructField("country", StringType(), True),
        StructField("sunrise", LongType(), True),
        StructField("sunset", LongType(), True),
    ])

    return StructType([
        StructField("coord", coord_schema, True),
        StructField("weather", ArrayType(weather_item_schema), True),
        StructField("base", StringType(), True),
        StructField("main", main_schema, True),
        StructField("visibility", IntegerType(), True),
        StructField("wind", wind_schema, True),
        StructField("clouds", clouds_schema, True),
        StructField("dt", LongType(), True),
        StructField("sys", sys_schema, True),
        StructField("timezone", IntegerType(), True),
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("cod", IntegerType(), True),
        StructField("city", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("ingestion_timestamp", StringType(), True),
    ])


# ============================================================
# Spark session
# ============================================================

def create_spark_session(app_name: str = "weather_bronze_to_silver") -> SparkSession:
    """
    Create (or reuse) a local Spark session.
    """

    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .getOrCreate()
    )


# ============================================================
# Read raw data
# ============================================================

def read_raw_weather(spark: SparkSession, input_dir: Path) -> DataFrame:
    """
    Read every raw weather JSON file in `input_dir` into a single
    Spark DataFrame, using an explicit schema (no full inference).

    The raw JSON files are arrays of records (multiline JSON), so
    `multiLine=True` is required.
    """

    if not input_dir.exists():
        raise FileNotFoundError(
            f"Input directory does not exist: {input_dir}"
        )

    json_files = sorted(input_dir.glob("*.json"))

    if not json_files:
        raise FileNotFoundError(
            f"No raw weather JSON files found in: {input_dir}"
        )

    logger.info(f"Found {len(json_files)} raw JSON file(s) in {input_dir}")

    schema = build_raw_weather_schema()

    df = (
        spark.read
        .option("multiLine", True)
        .schema(schema)
        .json([str(f) for f in json_files])
    )

    return df


# ============================================================
# Flatten
# ============================================================

def flatten_weather(df: DataFrame) -> DataFrame:
    """
    Flatten the nested raw schema into the target analytical
    (Silver) column layout, converting Unix timestamps along the way.
    """

    flattened = df.select(
        F.col("city").alias("city"),
        F.col("sys.country").alias("country"),
        F.col("latitude").alias("latitude"),
        F.col("longitude").alias("longitude"),

        F.col("main.temp").alias("temperature"),
        F.col("main.feels_like").alias("feels_like"),
        F.col("main.temp_min").alias("temp_min"),
        F.col("main.temp_max").alias("temp_max"),

        F.col("main.pressure").alias("pressure"),
        F.col("main.sea_level").alias("sea_level"),
        F.col("main.grnd_level").alias("ground_level"),
        F.col("main.humidity").alias("humidity"),

        F.col("visibility").alias("visibility"),

        F.col("wind.speed").alias("wind_speed"),
        F.col("wind.deg").alias("wind_direction"),
        F.col("wind.gust").alias("wind_gust"),

        F.col("clouds.all").alias("cloudiness"),

        F.col("weather")[0]["id"].alias("weather_id"),
        F.col("weather")[0]["main"].alias("weather_main"),
        F.col("weather")[0]["description"].alias("weather_description"),
        F.col("weather")[0]["icon"].alias("weather_icon"),

        F.to_timestamp(F.col("dt")).alias("observation_timestamp"),
        F.to_timestamp(F.col("sys.sunrise")).alias("sunrise_timestamp"),
        F.to_timestamp(F.col("sys.sunset")).alias("sunset_timestamp"),

        F.col("timezone").alias("timezone"),
        F.col("id").alias("city_id"),

        F.to_timestamp(F.col("ingestion_timestamp")).alias("ingestion_timestamp"),
    )

    return flattened


# ============================================================
# Data quality checks
# ============================================================

def apply_data_quality_checks(df: DataFrame) -> DataFrame:
    """
    Tag every record as VALID or INVALID rather than dropping bad
    records. `data_quality_reason` lists the failing rule(s).
    """

    rules = {
        "city_is_null": F.col("city").isNull(),
        "latitude_is_null": F.col("latitude").isNull(),
        "longitude_is_null": F.col("longitude").isNull(),
        "temperature_is_null": F.col("temperature").isNull(),
        "humidity_out_of_range": (
            F.col("humidity").isNull()
            | (F.col("humidity") < 0)
            | (F.col("humidity") > 100)
        ),
        "cloudiness_out_of_range": (
            F.col("cloudiness").isNull()
            | (F.col("cloudiness") < 0)
            | (F.col("cloudiness") > 100)
        ),
        "visibility_negative": (
            F.col("visibility").isNull() | (F.col("visibility") < 0)
        ),
        "wind_speed_negative": (
            F.col("wind_speed").isNull() | (F.col("wind_speed") < 0)
        ),
    }

    df_with_flags = df
    reason_parts = []

    for reason, condition in rules.items():
        flag_col = f"_flag_{reason}"
        df_with_flags = df_with_flags.withColumn(flag_col, condition)
        reason_parts.append(
            F.when(F.col(flag_col), F.lit(reason))
        )

    # Build a comma-separated reason string from whichever flags fired
    reasons_array = F.array_except(
        F.array(*reason_parts),
        F.array(F.lit(None).cast(StringType())),
    )

    df_with_flags = df_with_flags.withColumn(
        "data_quality_reason",
        F.when(F.size(reasons_array) > 0, F.concat_ws(",", reasons_array))
         .otherwise(F.lit(None).cast(StringType())),
    )

    df_with_flags = df_with_flags.withColumn(
        "data_quality_status",
        F.when(F.col("data_quality_reason").isNull(), F.lit("VALID"))
         .otherwise(F.lit("INVALID")),
    )

    # Drop the intermediate boolean flag columns
    flag_cols = [f"_flag_{reason}" for reason in rules]
    df_with_flags = df_with_flags.drop(*flag_cols)

    return df_with_flags


# ============================================================
# Deduplication
# ============================================================

def deduplicate_weather(df: DataFrame) -> DataFrame:
    """
    Deduplicate on (city, observation_timestamp), keeping the record
    with the most recent ingestion_timestamp.
    """

    window_spec = Window.partitionBy(
        "city", "observation_timestamp"
    ).orderBy(F.col("ingestion_timestamp").desc())

    deduped = (
        df.withColumn("_row_number", F.row_number().over(window_spec))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )

    return deduped


# ============================================================
# Write output
# ============================================================

def write_silver_parquet(df: DataFrame, output_dir: Path) -> None:
    """
    Write the final Silver DataFrame as Parquet, overwriting any
    existing output (initial-version behavior).

    The output directory is deleted and recreated manually before
    writing, so Spark does not need to clear it via the Hadoop
    filesystem API (which can fail on permission mismatches
    between different Docker container users).
    """

    if output_dir.exists():
        shutil.rmtree(output_dir)
        logger.info(f"Cleared existing output directory: {output_dir}")

    output_dir.mkdir(parents=True, exist_ok=True)

    (
        df.write
        .mode("overwrite")
        .parquet(str(output_dir))
    )


# ============================================================
# Main pipeline
# ============================================================

def run_transformation() -> None:
    """
    Orchestrate the full Bronze -> Silver transformation pipeline.
    """

    logger.info("Starting transformation")
    logger.info(f"Project root: {PROJECT_ROOT}")
    logger.info(f"Input directory: {INPUT_DIR}")
    logger.info(f"Output directory: {OUTPUT_DIR}")

    spark = create_spark_session()

    try:
        raw_df = read_raw_weather(spark, INPUT_DIR)
        raw_df.cache()

        total_records = raw_df.count()
        logger.info(f"Number of raw records: {total_records}")

        if total_records == 0:
            logger.warning("No records found in raw input. Nothing to process.")
            return

        flattened_df = flatten_weather(raw_df)

        checked_df = apply_data_quality_checks(flattened_df)
        checked_df.cache()

        valid_records = checked_df.filter(
            F.col("data_quality_status") == "VALID"
        ).count()
        invalid_records = checked_df.filter(
            F.col("data_quality_status") == "INVALID"
        ).count()

        logger.info(f"Number of valid records: {valid_records}")
        logger.info(f"Number of invalid records: {invalid_records}")

        pre_dedup_count = checked_df.count()
        deduped_df = deduplicate_weather(checked_df)
        deduped_df.cache()

        final_count = deduped_df.count()
        duplicates_removed = pre_dedup_count - final_count

        logger.info(f"Number of duplicates removed: {duplicates_removed}")
        logger.info(f"Number of final records: {final_count}")

        write_silver_parquet(deduped_df, OUTPUT_DIR)

        logger.info(f"Output directory: {OUTPUT_DIR}")
        logger.info("Transformation completed")

    except FileNotFoundError as error:
        logger.error(f"Input error: {error}")

    except Exception as error:  # noqa: BLE001 - top-level pipeline guard
        logger.error(f"Unexpected error during transformation: {error}")
        raise

    finally:
        spark.stop()


# ============================================================
# Entry point
# ============================================================

if __name__ == "__main__":
    try:
        run_transformation()
    except Exception:
        logger.exception("Transformation failed.")
        sys.exit(1)