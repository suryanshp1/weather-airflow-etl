from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago

# latitude and longitude for the desired location
LATITUDE = "51.5074"
LONGITUDE = "-0.1278"
POSTGRES_CONN_ID = "postgres_default"
API_CONN_ID = "open_meteo_api"

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
}

## DAg definition

with DAG(
    dag_id="weather_etl_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:
    
    @task
    def extract_weather_data():
        """
        Extracts weather data from the Open Meteo API.
        """
        # use HttpHook to call the API
        hook = HttpHook(method="GET", http_conn_id=API_CONN_ID)

        # run the API request
        endpoint  = f"/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true"
        response = hook.run(endpoint)
        
        if response.status_code != 200:
            raise Exception(f"API call failed with status code {response.status_code}")
        
        return response.json()
    
    @task
    def transform_weather_data(weather_data):
        """
        Transforms the weather data into a format that can be stored in the database.
        """
        # get the current weather data
        current_weather = weather_data.get("current_weather")

        transformed_data = {
            "latitude": LATITUDE,
            "longitude": LONGITUDE,
            "temperature": current_weather.get("temperature"),
            "wind_speed": current_weather.get("windspeed"),
            "wind_direction": current_weather.get("winddirection"),
            "weathercode": current_weather.get("weathercode"),
        }

        return transformed_data
    
    @task
    def load_weather_data(transformed_data):
        """
        Loads the weather data into the database.
        """
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Create a table if it doesn't exist
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS weather_data (
                latitude FLOAT,
                longitude FLOAT,
                temperature FLOAT,
                wind_speed FLOAT,
                wind_direction FLOAT,
                weathercode INT,
                timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            """
        )

        ## Insert the data into the table
        cursor.execute(
            """
            INSERT INTO weather_data (latitude, longitude, temperature, wind_speed, wind_direction, weathercode)
            VALUES (%s, %s, %s, %s, %s, %s);
            """,
            (
                transformed_data["latitude"],
                transformed_data["longitude"],
                transformed_data["temperature"],
                transformed_data["wind_speed"],
                transformed_data["wind_direction"],
                transformed_data["weathercode"],
            ),
        )

        conn.commit()
        cursor.close()
        conn.close()


    ## DAG workflow
    weather_data = extract_weather_data()
    transformed_data = transform_weather_data(weather_data)
    load_weather_data(transformed_data)