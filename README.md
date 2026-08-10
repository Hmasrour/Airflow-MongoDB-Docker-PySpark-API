# Weather ETL Pipeline

An end-to-end Data Engineering pipeline that orchestrates the extraction, transformation, and loading (ETL) of weather data from the OpenWeather API into a MongoDB database. The entire workflow is managed by Apache Airflow and runs fully containerized via Docker.

## Data Architecture

![Data Architecture](docs/architecture.jpg)

The pipeline implements a Medallion Architecture (Bronze -> Silver -> Gold) over 3 distinct stages:

### 1. Ingestion Layer (Bronze)
- **Technology:** Python `requests`
- **Action:** Fetches current weather data for a predefined list of cities (Rabat, Casablanca, Marrakech, Tangier) from the OpenWeather REST API.
- **Output:** Saves the raw, unmodified JSON responses locally to the `data/raw/weather/` directory.

### 2. Transformation Layer (Silver)
- **Technology:** Apache Spark (`PySpark`)
- **Action:** Reads the raw JSON files, flattens the nested structures into a tabular format, applies explicit schema typing, and performs data quality checks (e.g., bounds checking on humidity, temperature). It also handles deduplication based on city and observation time.
- **Output:** Writes cleaned data to the `data/processed/weather/` directory in Parquet format.

### 3. Loading Layer (Gold)
- **Technology:** `PyMongo`
- **Action:** Reads the Silver Parquet data, reshapes it into a document structure optimized for queries (including GeoJSON for coordinates), and upserts only the valid records into MongoDB.
- **Output:** Gold-level data stored in the `weather` collection of the `weather_db` database.

## Orchestration & Infrastructure

- **Apache Airflow:** Orchestrates the workflow. A DAG named `weather_pipeline` is scheduled to run `@hourly`, executing the Ingestion, Transformation, and Loading tasks in sequence using the `BashOperator`. Airflow uses the `LocalExecutor` backed by a PostgreSQL metadata database.
- **Docker Compose:** Manages the multi-container environment, including Airflow (Webserver, Scheduler, Init), PostgreSQL (Metadata), and MongoDB (Gold Store). A custom Airflow image (`Dockerfile.airflow`) is used to bake in Java 17 (for PySpark) and necessary Python dependencies.

## Project Structure

```text
.
├── dags/
│   └── weather_pipeline.py       # Airflow DAG definition
├── data/
│   ├── processed/weather/        # Silver layer (Parquet)
│   └── raw/weather/              # Bronze layer (JSON)
├── docs/
│   └── architecture.jpg          # Architecture diagram
├── src/
│   ├── ingestion/
│   │   └── weather_api.py        # Fetches from OpenWeather API
│   ├── loading/weather/
│   │   └── load_mongodb.py       # Upserts to MongoDB
│   └── transformation/
│       └── transform_weather.py  # PySpark transformation
├── .env                          # Environment variables (API keys, config)
├── docker-compose.yml            # Docker infrastructure definition
├── Dockerfile                    # Standalone job image
├── Dockerfile.airflow            # Custom Airflow image
└── requirements.txt              # Python dependencies
```

## Prerequisites

- Docker and Docker Compose installed.
- An OpenWeather API key.

## Setup & Execution

1. **Configure Environment:**
   Ensure you have a `.env` file at the root of the project with the following variables:
   ```env
   OPENWEATHER_API_KEY=your_openweather_api_key
   MONGODB_URI=mongodb://mongodb:27017
   MONGODB_DATABASE=weather_db
   MONGODB_COLLECTION=weather
   AIRFLOW_UID=50000
   ```

2. **Start the Infrastructure:**
   Build the images and start the containers in detached mode:
   ```bash
   docker compose up --build -d
   ```

3. **Access Airflow UI:**
   Navigate to `http://localhost:8081` in your browser.
   - **Username:** `airflow`
   - **Password:** `airflow`

4. **Run the Pipeline:**
   In the Airflow UI, locate the `weather_pipeline` DAG. Toggle the switch to unpause it, and trigger a run manually. You can monitor the progress of each task (Ingest -> Transform -> Load) in the Grid or Graph view.

5. **Verify Data in MongoDB:**
   Once the DAG completes successfully, you can query MongoDB to verify the data:
   ```bash
   docker exec -it weather_mongodb mongosh --eval "db.weather.find().pretty()"
   ```

## Development & Manual Runs

The individual ETL stages can also be executed manually outside of Airflow using Docker Compose profiles:

```bash
# Run Transformation manually
docker compose --profile jobs run transform-weather

# Run Loading manually
docker compose --profile jobs run load-mongodb
```
