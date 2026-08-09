import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv


# ============================================================
# Configuration
# ============================================================

def find_project_root(marker=".env", start: Path = None) -> Path:
    """
    Walk upward from `start` (default: this file's directory)
    until a folder containing `marker` (e.g. .env) is found.

    This avoids hardcoding how many folders deep the script lives,
    so it keeps working even if you move weather_api.py around.
    """

    current = (start or Path(__file__).resolve().parent)

    for parent in [current, *current.parents]:
        if (parent / marker).exists():
            return parent

    # Fallback: if no .env was found anywhere above, assume
    # the current file's directory is the root.
    return current


PROJECT_ROOT = find_project_root(".env")
ENV_PATH = PROJECT_ROOT / ".env"

if not ENV_PATH.exists():
    raise FileNotFoundError(
        f"No .env file found. Searched upward from "
        f"{Path(__file__).resolve()} and stopped at "
        f"{PROJECT_ROOT}. Create a .env file with "
        f"OPENWEATHER_API_KEY=your_key_here"
    )

load_dotenv(ENV_PATH)

API_KEY = os.getenv("OPENWEATHER_API_KEY")

BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

# Store raw data inside the project: data/raw/weather
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "weather"

if not API_KEY:
    raise ValueError(
        f"OPENWEATHER_API_KEY is missing from {ENV_PATH}. "
        "Make sure the .env file contains a line like: "
        "OPENWEATHER_API_KEY=your_key_here (no quotes, no spaces "
        "around the '=')."
    )


# ============================================================
# Logger
# ============================================================

logger = logging.getLogger("weather_ingestion")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# ============================================================
# Cities
# ============================================================

CITIES = [
    {
        "name": "Rabat",
        "lat": 34.0209,
        "lon": -6.8416
    },
    {
        "name": "Casablanca",
        "lat": 33.5731,
        "lon": -7.5898
    },
    {
        "name": "Marrakech",
        "lat": 31.6295,
        "lon": -7.9811
    },
    {
        "name": "Tangier",
        "lat": 35.7595,
        "lon": -5.8340
    }
]


# ============================================================
# Fetch weather
# ============================================================

def get_weather(
    lat: float,
    lon: float,
    units: str = "metric",
    lang: str = "en"
):
    """
    Fetch weather data for a specific latitude/longitude.
    """

    params = {
        "lat": lat,
        "lon": lon,
        "appid": API_KEY,
        "units": units,
        "lang": lang
    }

    response = None

    try:

        logger.info(
            f"Fetching weather for coordinates "
            f"({lat}, {lon})"
        )

        response = requests.get(
            BASE_URL,
            params=params,
            timeout=30
        )

        response.raise_for_status()

        logger.info(
            f"Weather data fetched successfully "
            f"for ({lat}, {lon})"
        )

        return response.json()

    except requests.exceptions.HTTPError as error:

        logger.error(
            f"HTTP error: {error} "
            f"| Status code: {response.status_code if response is not None else 'N/A'}"
        )

        # Useful when debugging API authentication
        if response is not None:
            logger.error(
                f"API response: {response.text}"
            )

    except requests.exceptions.ConnectionError:

        logger.error(
            "Connection error while contacting OpenWeather API"
        )

    except requests.exceptions.Timeout:

        logger.error(
            "Request timed out"
        )

    except requests.exceptions.RequestException as error:

        logger.error(
            f"Unexpected request error: {error}"
        )

    return None


# ============================================================
# Fetch all cities
# ============================================================

def fetch_all_cities_weather(
    cities,
    units: str = "metric",
    lang: str = "en"
):
    """
    Fetch weather data for multiple cities.
    """

    all_weather_data = []

    for city in cities:

        logger.info(
            f"Fetching weather for {city['name']}"
        )

        data = get_weather(
            city["lat"],
            city["lon"],
            units=units,
            lang=lang
        )

        if data:

            # Add metadata to the raw API response
            data["city"] = city["name"]
            data["latitude"] = city["lat"]
            data["longitude"] = city["lon"]

            data["ingestion_timestamp"] = (
                datetime.utcnow().isoformat()
            )

            all_weather_data.append(data)

        # Avoid hitting API rate limits
        time.sleep(1)

    return all_weather_data


# ============================================================
# Save raw data
# ============================================================

def save_raw_data(weather_data):
    """
    Save API response as raw JSON.

    We don't transform the API response here.
    """

    if not weather_data:
        logger.warning("No weather data to save.")
        return None

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    timestamp = datetime.utcnow().strftime(
        "%Y%m%d_%H%M%S"
    )

    output_file = (
        OUTPUT_DIR /
        f"weather_{timestamp}.json"
    )

    with open(
        output_file,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            weather_data,
            file,
            indent=4,
            ensure_ascii=False
        )

    logger.info(
        f"Raw weather data saved to {output_file}"
    )

    return output_file


# ============================================================
# Main
# ============================================================

def main():

    logger.info(
        "Starting weather data ingestion..."
    )

    weather_data = fetch_all_cities_weather(
        CITIES,
        units="metric",
        lang="en"
    )

    logger.info(
        f"Total cities successfully fetched: "
        f"{len(weather_data)}"
    )

    save_raw_data(weather_data)

    logger.info(
        "Weather ingestion completed."
    )


# ============================================================
# Entry point
# ============================================================

if __name__ == "__main__":
    main()