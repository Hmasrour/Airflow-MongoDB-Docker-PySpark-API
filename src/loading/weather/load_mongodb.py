"""
load_mongodb.py

Silver -> Gold loader for the weather pipeline.

Reads the cleaned Silver Parquet dataset (data/processed/weather/),
keeps only VALID records, reshapes each row into a nested MongoDB
document optimized for querying, and upserts it into MongoDB using
a (city, observation_timestamp) business key so re-running the
loader never creates duplicates.

Run:
    python src/loading/weather/load_mongodb.py
    spark-submit src/loading/weather/load_mongodb.py
"""

import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, Tuple

from dotenv import load_dotenv
from pymongo import ASCENDING, MongoClient, UpdateOne
from pymongo.errors import ConnectionFailure, PyMongoError
from pyspark.sql import DataFrame, SparkSession


# ============================================================
# Logger
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger("weather_mongodb_loading")


# ============================================================
# Project root detection
# ============================================================

def find_project_root(marker: str = ".env", start: Path = None) -> Path:
    """
    Walk upward from `start` (default: this file's directory) until a
    folder containing `marker` (e.g. .env) is found.

    Works the same way locally and inside Docker, since it never
    assumes a fixed folder depth.
    """

    current = (start or Path(__file__).resolve().parent)

    for parent in [current, *current.parents]:
        if (parent / marker).exists():
            return parent

    logger.warning(
        f"Could not find '{marker}' above {current}. "
        f"Falling back to {current} as project root."
    )
    return current


PROJECT_ROOT = find_project_root(".env")
load_dotenv(PROJECT_ROOT / ".env")

SILVER_INPUT_DIR = PROJECT_ROOT / "data" / "processed" / "weather"


# ============================================================
# Configuration
# ============================================================

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://mongodb:27017")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "weather_db")
MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION", "weather")

MONGODB_CONNECT_RETRIES = int(os.getenv("MONGODB_CONNECT_RETRIES", "5"))
MONGODB_CONNECT_RETRY_DELAY_SECONDS = int(
    os.getenv("MONGODB_CONNECT_RETRY_DELAY_SECONDS", "3")
)


def mask_uri(uri: str) -> str:
    """
    Return a MongoDB URI with any embedded credentials masked, so we
    never log a password even if one is present in the connection
    string (mongodb://user:password@host:port/...).
    """

    if "@" not in uri:
        return uri

    scheme_and_creds, _, rest = uri.partition("@")
    scheme, _, _creds = scheme_and_creds.partition("://")
    return f"{scheme}://***:***@{rest}"


# ============================================================
# Spark session + Silver read
# ============================================================

def create_spark_session(app_name: str = "weather_gold_loading") -> SparkSession:
    """
    Create (or reuse) a local Spark session, used only to read the
    Silver Parquet dataset.
    """

    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .getOrCreate()
    )


def read_silver_weather(spark: SparkSession, input_dir: Path) -> DataFrame:
    """
    Read the Silver Parquet dataset written by transform_weather.py.
    """

    if not input_dir.exists():
        raise FileNotFoundError(
            f"Silver input directory does not exist: {input_dir}. "
            "Run the PySpark transformation step first."
        )

    parquet_files = list(input_dir.glob("*.parquet"))

    if not parquet_files:
        raise FileNotFoundError(
            f"No Parquet files found in: {input_dir}. "
            "Run the PySpark transformation step first."
        )

    logger.info(f"Input directory: {input_dir}")

    df = spark.read.parquet(str(input_dir))

    logger.info(f"Silver schema:\n{df._jdf.schema().treeString()}")

    return df


# ============================================================
# Row -> MongoDB document
# ============================================================

def row_to_gold_document(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Reshape a flat Silver row into a nested Gold document optimized
    for application/query usage, rather than mirroring the Parquet
    table structure 1:1.
    """

    def iso_or_none(value: Any) -> Any:
        return value.isoformat() if hasattr(value, "isoformat") else value

    return {
        "city": row.get("city"),
        "city_id": row.get("city_id"),

        "location": {
            "type": "Point",
            # GeoJSON order is [longitude, latitude]
            "coordinates": [row.get("longitude"), row.get("latitude")],
        },

        "weather": {
            "temperature": row.get("temperature"),
            "feels_like": row.get("feels_like"),
            "temp_min": row.get("temp_min"),
            "temp_max": row.get("temp_max"),
            "pressure": row.get("pressure"),
            "sea_level": row.get("sea_level"),
            "ground_level": row.get("ground_level"),
            "humidity": row.get("humidity"),
            "visibility": row.get("visibility"),
        },

        "wind": {
            "speed": row.get("wind_speed"),
            "direction": row.get("wind_direction"),
            "gust": row.get("wind_gust"),
        },

        "cloudiness": row.get("cloudiness"),

        "condition": {
            "id": row.get("weather_id"),
            "main": row.get("weather_main"),
            "description": row.get("weather_description"),
            "icon": row.get("weather_icon"),
        },

        "timestamps": {
            "observation": iso_or_none(row.get("observation_timestamp")),
            "sunrise": iso_or_none(row.get("sunrise_timestamp")),
            "sunset": iso_or_none(row.get("sunset_timestamp")),
            "timezone_offset_seconds": row.get("timezone"),
            "ingestion": iso_or_none(row.get("ingestion_timestamp")),
        },

        "data_quality": {
            "status": row.get("data_quality_status"),
            "reason": row.get("data_quality_reason"),
        },

        # Flat copy of the business key fields, so MongoDB queries
        # and the unique index don't need to reach into nested
        # sub-documents.
        "country": row.get("country"),
        "observation_timestamp": iso_or_none(row.get("observation_timestamp")),
    }


def build_upsert_operations(rows: Iterable[Dict[str, Any]]) -> list:
    """
    Convert Silver rows into a list of pymongo UpdateOne operations,
    keyed on (city, observation_timestamp) so re-running the loader
    updates existing documents instead of duplicating them.
    """

    operations = []

    for row in rows:
        document = row_to_gold_document(row)

        filter_key = {
            "city": document["city"],
            "observation_timestamp": document["observation_timestamp"],
        }

        operations.append(
            UpdateOne(filter_key, {"$set": document}, upsert=True)
        )

    return operations


# ============================================================
# MongoDB connection (with retry)
# ============================================================

def connect_to_mongodb(
    uri: str,
    retries: int = MONGODB_CONNECT_RETRIES,
    delay_seconds: int = MONGODB_CONNECT_RETRY_DELAY_SECONDS,
) -> MongoClient:
    """
    Connect to MongoDB, retrying a fixed number of times with a fixed
    delay. MongoDB and the loader container may start at different
    times, so a simple retry loop avoids a hard failure on cold start.
    """

    logger.info(f"MongoDB URI: {mask_uri(uri)}")
    logger.info(f"MongoDB database: {MONGODB_DATABASE}")
    logger.info(f"MongoDB collection: {MONGODB_COLLECTION}")

    last_error: Exception = None

    for attempt in range(1, retries + 1):
        try:
            logger.info(f"Connecting to MongoDB (attempt {attempt}/{retries})")

            client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")

            logger.info("MongoDB connection successful")
            return client

        except (ConnectionFailure, PyMongoError) as error:
            last_error = error
            logger.warning(f"MongoDB connection attempt {attempt} failed: {error}")

            if attempt < retries:
                time.sleep(delay_seconds)

    raise ConnectionFailure(
        f"Could not connect to MongoDB after {retries} attempts: {last_error}"
    )


def ensure_indexes(collection) -> None:
    """
    Create the unique compound index enforcing the business key, plus
    a supporting single-field index on city for common lookups.
    """

    collection.create_index(
        [("city", ASCENDING), ("observation_timestamp", ASCENDING)],
        unique=True,
        name="uniq_city_observation_timestamp",
    )

    collection.create_index(
        [("city", ASCENDING)],
        name="idx_city",
    )

    logger.info("Ensured indexes: uniq_city_observation_timestamp, idx_city")


# ============================================================
# Load pipeline
# ============================================================

def split_valid_invalid(df: DataFrame) -> Tuple[DataFrame, int, int]:
    """
    Split the Silver DataFrame into valid and invalid records based
    on data_quality_status, returning the valid DataFrame plus counts
    of both.
    """

    valid_df = df.filter(df["data_quality_status"] == "VALID")
    invalid_count = df.filter(df["data_quality_status"] != "VALID").count()
    valid_count = valid_df.count()

    return valid_df, valid_count, invalid_count


def load_to_mongodb(valid_df: DataFrame, collection) -> Tuple[int, int]:
    """
    Convert valid Silver rows to Gold documents and upsert them into
    MongoDB in a single bulk_write call. Returns (inserted, updated).
    """

    rows = [row.asDict(recursive=True) for row in valid_df.collect()]

    if not rows:
        return 0, 0

    operations = build_upsert_operations(rows)

    result = collection.bulk_write(operations, ordered=False)

    inserted = result.upserted_count
    updated = result.modified_count

    return inserted, updated


def run_loading() -> None:
    """
    Orchestrate the full Silver -> Gold loading pipeline.
    """

    logger.info("Starting MongoDB Gold loading")
    logger.info(f"Project root: {PROJECT_ROOT}")

    spark = create_spark_session()
    client = None

    try:
        silver_df = read_silver_weather(spark, SILVER_INPUT_DIR)
        silver_df.cache()

        total_records = silver_df.count()
        logger.info(f"Number of Silver records: {total_records}")

        if total_records == 0:
            logger.warning("No Silver records found. Nothing to load.")
            return

        valid_df, valid_count, invalid_count = split_valid_invalid(silver_df)

        logger.info(f"Number of valid records: {valid_count}")
        logger.info(f"Number of invalid records skipped: {invalid_count}")

        if valid_count == 0:
            logger.warning("No valid records to load into MongoDB.")
            return

        client = connect_to_mongodb(MONGODB_URI)
        database = client[MONGODB_DATABASE]
        collection = database[MONGODB_COLLECTION]

        ensure_indexes(collection)

        inserted, updated = load_to_mongodb(valid_df, collection)
        processed = inserted + updated

        logger.info(f"Number of documents inserted: {inserted}")
        logger.info(f"Number of documents updated: {updated}")
        logger.info(f"Number of documents processed: {processed}")

        logger.info("MongoDB loading completed")

    except FileNotFoundError as error:
        logger.error(f"Input error: {error}")
        raise

    except ConnectionFailure as error:
        logger.error(f"MongoDB connection error: {error}")
        raise

    except Exception as error:  # noqa: BLE001 - top-level pipeline guard
        logger.error(f"Unexpected error during MongoDB loading: {error}")
        raise

    finally:
        if client is not None:
            client.close()
        spark.stop()


# ============================================================
# Entry point
# ============================================================

if __name__ == "__main__":
    try:
        run_loading()
    except Exception:
        logger.exception("MongoDB loading failed.")
        sys.exit(1)