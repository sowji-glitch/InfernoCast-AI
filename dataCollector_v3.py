import os
import pandas as pd
from google.cloud import bigquery
from datetime import datetime
import sys
from collectRealData import collect_all_real_data
from generateSimulatedData import generate_all_simulated_data
import glob
import pandas_gbq

PROJECT_ID = os.environ["PROJECT_ID"]
DATASET_ID = 'napa_wildfire_demo'

client = bigquery.Client(project=PROJECT_ID)



def load_fire_history(project_id: str, dataset_id: str, table_id: str = "fire_history"):
    client = bigquery.Client(project=project_id)

    staging_table_id = f"{project_id}.{dataset_id}.{table_id}_staging"
    final_table_id = f"{project_id}.{dataset_id}.{table_id}"

    # Step 1: Collect CSVs
    csv_files = glob.glob("./data/fire_history*.csv")
    if not csv_files:
        print(" No CSV files found in ./data/")
        return

    print(f"Found {len(csv_files)} CSV files. Loading into staging table...")

    # Step 2: Load into staging table
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # overwrite staging
    )

    with open(csv_files[0], "rb") as f:  # load all files together
        load_job = client.load_table_from_file(f, staging_table_id, job_config=job_config)
    for csv_file in csv_files[1:]:
        with open(csv_file, "rb") as f:
            load_job = client.load_table_from_file(f, staging_table_id, job_config=job_config)

    load_job.result()  # wait for completion
    print(f" Loaded {len(csv_files)} CSV files into staging table {staging_table_id}")

    # Step 3: Ensure final table exists
    schema = [
        bigquery.SchemaField("fire_name", "STRING"),
        bigquery.SchemaField("fire_year", "INT64"),
        bigquery.SchemaField("alarm_date", "DATE"),
        bigquery.SchemaField("contained_date", "DATE"),
        bigquery.SchemaField("acres", "FLOAT64"),
        bigquery.SchemaField("cause", "STRING"),
        bigquery.SchemaField("latitude", "FLOAT64"),
        bigquery.SchemaField("longitude", "FLOAT64"),
        bigquery.SchemaField("county", "STRING"),
    ]
    try:
        client.get_table(final_table_id)
        print(f"Final table {final_table_id} already exists.")
    except Exception:
        table = bigquery.Table(final_table_id, schema=schema)
        client.create_table(table)
        print(f"Created final table {final_table_id}")

    # Step 4: MERGE staging into final table (upsert)
    merge_query = f"""
    MERGE `{final_table_id}` T
    USING `{staging_table_id}` S
    ON T.fire_name = S.fire_name
       AND T.fire_year = S.fire_year
       AND T.alarm_date = S.alarm_date
    WHEN MATCHED THEN
      UPDATE SET
        contained_date = S.contained_date,
        acres = S.acres,
        cause = S.cause,
        latitude = S.latitude,
        longitude = S.longitude,
        county = S.county
    WHEN NOT MATCHED THEN
      INSERT ROW
    """
    query_job = client.query(merge_query)
    query_job.result()
    print(f" Merged staging data into {final_table_id}")

    # Step 5: Drop staging table
    client.delete_table(staging_table_id, not_found_ok=True)
    print(f" Staging table {staging_table_id} deleted.")

    print(" Fire history data pipeline completed successfully.")



def upload_to_bigquery(df, table_name):
    """Upload DataFrame to BigQuery with proper schema handling"""
    if df.empty:
        print(f"No data to upload to {table_name}")
        return False
    
    df_copy = df.copy()
    
    if table_name == 'weather_data':
        df_copy['date'] = pd.to_datetime(df_copy['date']).dt.date
    elif table_name == 'fire_history':
        df_copy['alarm_date'] = pd.to_datetime(df_copy['alarm_date']).dt.date
        df_copy['contained_date'] = pd.to_datetime(df_copy['contained_date']).dt.date
    
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    
    try:
        print(f"Uploading {len(df_copy)} records to {table_name}...")
        
        pandas_gbq.to_gbq(
            df_copy,
            destination_table=f"{DATASET_ID}.{table_name}",
            project_id=PROJECT_ID,
            if_exists='replace',
            table_schema=None,
            progress_bar=False
        )
        
        table = client.get_table(table_id)
        print(f"Success: {table.num_rows} rows in {table_name}")
        return True
        
    except Exception as e:
        print(f"Upload failed for {table_name}: {str(e)}")
        return False
    


def main():
    """Main data collection workflow"""
    print("Napa Valley Wildfire Data Collection")
    print("=" * 40)

    if PROJECT_ID == '':
        print("ERROR: Set PROJECT_ID")
        return 1

    try:
        print("Select mode:")
        print("1. Use real APIs (OpenWeather + NOAA) - You need to enter API Keys - Account can be created for free")
        print("2. Use simulated data + upload already created CSV files from real data")
        choice = input("Enter 1 or 2: ").strip()

        if choice == "1":
            # Prompt for API keys
            openweather_api_key = input("Enter your OPENWEATHER_API_KEY: ").strip()
            noaa_token = input("Enter your NOAA_TOKEN: ").strip()

            if not openweather_api_key or not noaa_token:
                print(" Both API keys are required in mode 1. Exiting.")
                return 1

            os.environ["OPENWEATHER_API_KEY"] = openweather_api_key
            os.environ["NOAA_TOKEN"] = noaa_token

            print("1. Attempting real data collection...")
            weather_df, fire_df = collect_all_real_data()

            if len(weather_df) < 100:
                print("2. Insufficient real data, generating simulated data...")
                sim_weather, sim_fire = generate_all_simulated_data()
                weather_df = pd.concat([weather_df, sim_weather], ignore_index=True)
                fire_df = pd.concat([fire_df, sim_fire], ignore_index=True)

        elif choice == "2":
            print(" Running in simulated mode...")
            weather_df, fire_df = generate_all_simulated_data()

            # Upload existing fire history CSVs
            try:
                load_fire_history(PROJECT_ID, DATASET_ID)
            except Exception as e:
                print(f" Could not run fire history loader: {e}")

        else:
            print(" Invalid choice. Exiting.")
            return 1

        weather_df = weather_df.drop_duplicates(subset=['location', 'date']).sort_values(['date', 'location'])

        print(f"Final dataset: {len(weather_df)} weather records, {len(fire_df)} fire records")

        print("3. Uploading to BigQuery...")
        weather_success = upload_to_bigquery(weather_df, 'weather_data')
        fire_success = upload_to_bigquery(fire_df, 'fire_history')

        print("4. Creating local backups...")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        weather_df.to_csv(f'./data/weather_data_{timestamp}.csv', index=False)
        fire_df.to_csv(f'./data/fire_history_{timestamp}.csv', index=False)

        print("=" * 40)
        print(f"Weather data: {'SUCCESS' if weather_success else 'FAILED'}")
        print(f"Fire data: {'SUCCESS' if fire_success else 'FAILED'}")

        if weather_success and fire_success:
            print("Data collection completed successfully")
            print("\nNext steps:")
            print("1. Test multimodal queries in BigQuery console")
            print("2. Launch Jupyter notebook for analysis")
            return 0
        else:
            print("Some uploads failed")
            return 1

    except Exception as e:
        print(f"Error: {str(e)}")
        return 1


if __name__ == "__main__":
    exit(main())
