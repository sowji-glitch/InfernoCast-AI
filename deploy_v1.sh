#!/bin/bash

# BigQuery Napa Wildfire Hackathon Setup Script
# This script creates all necessary GCP resources for the multimodal wildfire demo

# Function to check if command succeeded
check_success() {
    if [ $? -eq 0 ]; then
        echo "  ✓ Success"
    else
        echo "  ✗ Failed or already exists - continuing..."
    fi
}

# Function to check if service exists
service_exists() {
    gcloud services list --enabled --filter="name:$1" --format="value(name)" | grep -q "$1"
}

# Function to check if service account exists  
service_account_exists() {
    gcloud iam service-accounts list --filter="email:$1" --format="value(email)" | grep -q "$1"
}

# Function to check if bucket exists
bucket_exists() {
    gsutil ls -b gs://$1 >/dev/null 2>&1
}

# Function to check if dataset exists
dataset_exists() {
    bq ls -d $1 >/dev/null 2>&1
}

# Function to check if table exists
table_exists() {
    bq show $1 >/dev/null 2>&1
}

# Function to check if connection exists
connection_exists() {
    bq show --connection --location=$1 $2 >/dev/null 2>&1
}

# Configuration - Update these values
PROJECT_ID=${1:-$PROJECT_ID}
BQ_REGION=${2:-"US"}  # BigQuery region (US multi-region)
BUCKET_REGION="us-central1"  # Cloud Storage region (specific region)
DATASET_ID="napa_wildfire_demo"
BUCKET_NAME="${PROJECT_ID}-napa-fire-data"
SERVICE_ACCOUNT_NAME="napa-fire-sa"
CONNECTION_ID="gcs-connection"
# Enter the below if you have them / can create them, else use the data already uploaded into the repository OR run the simulation script
OPENWEATHER_API_KEY="9e7fb3841b31a9af3084f5bc65084923"
NOAA_TOKEN="pCYXtUOQRrdSJVUskoqlbCcoNGASHnsY"

echo "Setting up Napa Wildfire Demo for project: $PROJECT_ID"
echo "BigQuery region: $BQ_REGION | Storage region: $BUCKET_REGION"
echo "=================================================="

# Set the project
echo "Setting project to $PROJECT_ID..."
gcloud config set project $PROJECT_ID
export PROJECT_ID=$PROJECT_ID

# Exporting API Keys
export OPENWEATHER_API_KEY=$OPENWEATHER_API_KEY
export NOAA_TOKEN=$NOAA_TOKEN

# Enable required APIs
echo "Enabling required APIs..."
apis=("bigquery.googleapis.com" "storage.googleapis.com" "bigqueryconnection.googleapis.com" "aiplatform.googleapis.com")
for api in "${apis[@]}"; do
    if service_exists "$api"; then
        echo "  API $api already enabled"
    else
        echo "  Enabling $api..."
        gcloud services enable $api
        check_success
    fi
done

# Create service account
echo "Creating service account..."
SERVICE_ACCOUNT_EMAIL="${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

if service_account_exists "$SERVICE_ACCOUNT_EMAIL"; then
    echo "  Service account $SERVICE_ACCOUNT_NAME already exists"
else
    echo "  Creating service account $SERVICE_ACCOUNT_NAME..."
    gcloud iam service-accounts create $SERVICE_ACCOUNT_NAME \
        --display-name="Napa Fire Demo Service Account" \
        --description="Service account for wildfire demo access" 2>/dev/null
    check_success
fi

# Grant necessary permissions to service account
echo "Granting permissions to service account..."
roles=("roles/bigquery.admin" "roles/storage.objectAdmin" "roles/aiplatform.user", "roles/storage.admin")
for role in "${roles[@]}"; do
    echo "  Granting $role..."
    gcloud projects add-iam-policy-binding $PROJECT_ID \
        --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" \
        --role="$role" >/dev/null 2>&1
    check_success
done

# Create Cloud Storage bucket with folders - BUCKET IN us-central1
echo "Creating Cloud Storage bucket with folder structure..."
if bucket_exists "$BUCKET_NAME"; then
    echo "  Bucket gs://$BUCKET_NAME already exists"
else
    echo "  Creating bucket gs://$BUCKET_NAME in region $BUCKET_REGION..."
    gsutil mb -p $PROJECT_ID -c STANDARD -l $BUCKET_REGION gs://$BUCKET_NAME/ 2>/dev/null
    check_success
fi

# Create folder structure by uploading placeholder files (will be replaced with real data)
echo "  Creating folder structure..."
folders=("images" "alerts" "raw-data")
for folder in "${folders[@]}"; do
    if gsutil ls gs://$BUCKET_NAME/$folder/ >/dev/null 2>&1; then
        echo "    Folder $folder/ already exists"
    else
        echo "    Creating folder $folder/..."
        echo "$folder/" | gsutil cp - gs://$BUCKET_NAME/$folder/.keep 2>/dev/null
        check_success
    fi
done

# Set bucket permissions
echo "Setting bucket permissions..."
gsutil iam ch serviceAccount:$SERVICE_ACCOUNT_EMAIL:objectAdmin gs://$BUCKET_NAME/ 2>/dev/null
check_success

# Create BigQuery dataset - US MULTI-REGION
echo "Creating BigQuery dataset..."
if dataset_exists "$PROJECT_ID:$DATASET_ID"; then
    echo "  Dataset $DATASET_ID already exists"
else
    echo "  Creating dataset $DATASET_ID in region $BQ_REGION..."
    bq mk --location=$BQ_REGION --dataset $PROJECT_ID:$DATASET_ID 2>/dev/null
    check_success
fi

# Create BigQuery connection for Cloud Storage - US REGION FOR BQ CONNECTION
echo "Creating BigQuery connection..."
if connection_exists "$BQ_REGION" "$CONNECTION_ID"; then
    echo "  Connection $CONNECTION_ID already exists"
else
    echo "  Creating connection $CONNECTION_ID in BigQuery region $BQ_REGION..."
    bq mk --connection \
        --display_name="GCS Connection for Napa Fire Demo" \
        --connection_type=CLOUD_RESOURCE \
        --location=$BQ_REGION \
        $CONNECTION_ID 2>/dev/null
    check_success
    
    # Wait for connection to propagate
    echo "  Waiting for connection to propagate..."
    sleep 10
fi

# Get the connection service account and grant it storage permissions
echo "Getting connection service account..."
# Retry connection SA retrieval
for i in {1..5}; do
    CONNECTION_SA=$(bq show --connection --location=$BQ_REGION --format=json $CONNECTION_ID 2>/dev/null | jq -r '.cloudResource.serviceAccountId' 2>/dev/null)
    
    if [ "$CONNECTION_SA" != "null" ] && [ -n "$CONNECTION_SA" ] && [ "$CONNECTION_SA" != "" ]; then
        echo "  Found connection service account: $CONNECTION_SA"
        break
    else
        echo "  Waiting for connection service account to be created (attempt $i/5)..."
        sleep 5
    fi
done

if [ "$CONNECTION_SA" != "null" ] && [ -n "$CONNECTION_SA" ] && [ "$CONNECTION_SA" != "" ]; then
    echo "  Granting storage permissions to connection service account..."
    gsutil iam ch serviceAccount:$CONNECTION_SA:objectViewer gs://$BUCKET_NAME/ 2>/dev/null
    gcloud projects add-iam-policy-binding $PROJECT_ID --member=serviceAccount:$CONNECTION_SA --role=roles/aiplatform.user 2>/dev/null
    check_success
else
    echo "  ⚠ Could not retrieve connection service account - external tables may not work"
    echo "    Manual fix: Go to BigQuery console > External connections > $CONNECTION_ID"
    echo "    Copy the service account and grant it Storage Object Viewer role on gs://$BUCKET_NAME"
fi
echo "  Waiting 100 seconds for permissions to propagate..."
sleep 100

# Create AI Model for inference and analysis of data
echo "Creating gemini_firesim_model Model..."

bq query --use_legacy_sql=false --replace=true "
CREATE OR REPLACE MODEL \`$PROJECT_ID.$DATASET_ID.gemini_firesim_model\`
REMOTE WITH CONNECTION \`projects/$PROJECT_ID/locations/US/connections/$CONNECTION_ID\`
OPTIONS (
  remote_service_type = 'CLOUD_AI_LARGE_LANGUAGE_MODEL_V1',
  endpoint = 'gemini-2.5-flash'
)" 2>/dev/null
check_success
echo "  If the model creation fails due to permissions still not in place, please create at the end manually in the BQ console using the SQL..."



# Create weather data table
echo "Creating weather data table..."
if table_exists "$PROJECT_ID:$DATASET_ID.weather_data"; then
    echo "  Table weather_data already exists - recreating..."
fi
bq query --use_legacy_sql=false --replace=true "
CREATE OR REPLACE TABLE \`$PROJECT_ID.$DATASET_ID.weather_data\` (
  location STRING OPTIONS(description='Weather station location name'),
  date DATE OPTIONS(description='Date of weather observation'),
  temp_max FLOAT64 OPTIONS(description='Maximum temperature in Fahrenheit'),
  humidity INT64 OPTIONS(description='Relative humidity percentage'),
  wind_speed FLOAT64 OPTIONS(description='Wind speed in mph'),
  wind_deg INT64 OPTIONS(description='Wind direction in degrees'),
  pressure INT64 OPTIONS(description='Atmospheric pressure in hPa'),
  visibility INT64 OPTIONS(description='Visibility in meters'),
  uvi FLOAT64 OPTIONS(description='UV index'),
  fire_risk_score FLOAT64 OPTIONS(description='Calculated fire risk score 0-1')
)
OPTIONS(
  description='Weather data for Napa Valley fire risk analysis',
  labels=[('environment', 'demo'), ('project', 'wildfire')]
);" 2>/dev/null
check_success

# Create fire history table
echo "Creating fire history table..."
if table_exists "$PROJECT_ID:$DATASET_ID.fire_history"; then
    echo "  Table fire_history already exists - recreating..."
fi
bq query --use_legacy_sql=false --replace=true "
CREATE OR REPLACE TABLE \`$PROJECT_ID.$DATASET_ID.fire_history\` (
  fire_name STRING OPTIONS(description='Name of the fire incident'),
  fire_year INT64 OPTIONS(description='Year the fire occurred'),
  alarm_date DATE OPTIONS(description='Date fire was first reported'),
  contained_date DATE OPTIONS(description='Date fire was contained'),
  acres FLOAT64 OPTIONS(description='Total acres burned'),
  cause STRING OPTIONS(description='Cause of fire ignition'),
  latitude FLOAT64 OPTIONS(description='Latitude of fire origin'),
  longitude FLOAT64 OPTIONS(description='Longitude of fire origin'),
  county STRING OPTIONS(description='County where fire occurred')
)
OPTIONS(
  description='Historical fire incidents in Napa County',
  labels=[('environment', 'demo'), ('project', 'wildfire')]
);" 2>/dev/null
check_success

# Create external table for satellite images - IMPROVED ERROR HANDLING
echo "Creating satellite images external table..."
if table_exists "$PROJECT_ID:$DATASET_ID.satellite_images"; then
    echo "  External table satellite_images already exists - recreating..."
fi

# Check if connection is ready before creating external table
if [ "$CONNECTION_SA" != "null" ] && [ -n "$CONNECTION_SA" ] && [ "$CONNECTION_SA" != "" ]; then
    echo "  Creating external table with connection..."
    bq query --use_legacy_sql=false --replace=true "
    CREATE OR REPLACE EXTERNAL TABLE \`$PROJECT_ID.$DATASET_ID.satellite_images\`
    WITH CONNECTION \`projects/$PROJECT_ID/locations/$BQ_REGION/connections/$CONNECTION_ID\`
    OPTIONS (
      object_metadata = 'SIMPLE',
      uris = ['gs://$BUCKET_NAME/images/*'],
      metadata_cache_mode = 'AUTOMATIC',
      max_staleness = INTERVAL 1 HOUR
    );" 2>/dev/null
    if [ $? -eq 0 ]; then
        echo "  ✓ Satellite images external table created"
    else
        echo "  ✗ Failed to create satellite images external table"
        echo "    This may be due to connection propagation delay"
        echo "    Manual fix: Re-run this command in BigQuery console:"
        echo "    CREATE OR REPLACE EXTERNAL TABLE \`$PROJECT_ID.$DATASET_ID.satellite_images\`"
        echo "    WITH CONNECTION \`projects/$PROJECT_ID/locations/$BQ_REGION/connections/$CONNECTION_ID\`"
        echo "    OPTIONS (object_metadata = 'SIMPLE', uris = ['gs://$BUCKET_NAME/images/**'], max_staleness = INTERVAL 1 HOUR);"
    fi
else
    echo "  ⚠ Skipping external table creation - connection service account not ready"
fi

# Create external table for weather alerts - IMPROVED ERROR HANDLING
echo "Creating weather alerts external table..."
if table_exists "$PROJECT_ID:$DATASET_ID.weather_alerts"; then
    echo "  External table weather_alerts already exists - recreating..."
fi

if [ "$CONNECTION_SA" != "null" ] && [ -n "$CONNECTION_SA" ] && [ "$CONNECTION_SA" != "" ]; then
    echo "  Creating external table with connection..."
    bq query --use_legacy_sql=false --replace=true "
    CREATE OR REPLACE EXTERNAL TABLE \`$PROJECT_ID.$DATASET_ID.weather_alerts\`
    WITH CONNECTION \`projects/$PROJECT_ID/locations/$BQ_REGION/connections/$CONNECTION_ID\`
    OPTIONS (
      object_metadata = 'SIMPLE',
      uris = ['gs://$BUCKET_NAME/alerts/*'],
      metadata_cache_mode = 'AUTOMATIC',
      max_staleness = INTERVAL 1 HOUR
    );" 2>/dev/null
    if [ $? -eq 0 ]; then
        echo "  ✓ Weather alerts external table created"
    else
        echo "  ✗ Failed to create weather alerts external table"
        echo "    This may be due to connection propagation delay"
        echo "    Manual fix: Re-run this command in BigQuery console:"
        echo "    CREATE OR REPLACE EXTERNAL TABLE \`$PROJECT_ID.$DATASET_ID.weather_alerts\`"
        echo "    WITH CONNECTION \`projects/$PROJECT_ID/locations/$BQ_REGION/connections/$CONNECTION_ID\`"
        echo "    OPTIONS (object_metadata = 'SIMPLE', uris = ['gs://$BUCKET_NAME/alerts/*'], max_staleness = INTERVAL 1 HOUR);"
    fi
else
    echo "  ⚠ Skipping external table creation - connection service account not ready"
fi

# Create fire risk calculation view
echo "Creating fire risk analysis view..."
if table_exists "$PROJECT_ID:$DATASET_ID.fire_risk_analysis"; then
    echo "  View fire_risk_analysis already exists - recreating..."
fi
bq query --use_legacy_sql=false --replace=true "
CREATE OR REPLACE VIEW \`$PROJECT_ID.$DATASET_ID.fire_risk_analysis\` AS
SELECT 
  location,
  date,
  temp_max,
  humidity,
  wind_speed,
  CASE 
    WHEN temp_max > 95 AND humidity < 15 AND wind_speed > 25 THEN 'EXTREME'
    WHEN temp_max > 85 AND humidity < 25 AND wind_speed > 15 THEN 'HIGH'
    WHEN temp_max > 75 AND humidity < 35 AND wind_speed > 10 THEN 'MODERATE'
    ELSE 'LOW'
  END as risk_category,
  fire_risk_score,
  -- Haines Index calculation (simplified)
  ROUND((temp_max - 32) * 0.556 + (100 - humidity) * 0.1, 2) as haines_index
FROM \`$PROJECT_ID.$DATASET_ID.weather_data\`
ORDER BY date DESC, fire_risk_score DESC;" 2>/dev/null
check_success

# Generate service account key
echo "Generating service account key..."
KEY_FILE="~/napa-fire-key.json"
if [ -f "$KEY_FILE" ]; then
    echo "  Service account key already exists at $KEY_FILE - skipping..."
else
    echo "  Creating service account key..."
    gcloud iam service-accounts keys create $KEY_FILE \
        --iam-account=$SERVICE_ACCOUNT_EMAIL 2>/dev/null
    check_success
fi

# Verify setup
echo ""
echo "Verifying setup..."
echo "  Checking dataset..."
if bq ls -d $PROJECT_ID:$DATASET_ID >/dev/null 2>&1; then
    echo "    ✓ Dataset exists"
else
    echo "    ✗ Dataset missing"
fi

echo "  Checking connection..."
if bq show --connection --location=$BQ_REGION $CONNECTION_ID >/dev/null 2>&1; then
    echo "    ✓ Connection exists"
else
    echo "    ✗ Connection missing"
fi

echo "  Checking bucket..."
if gsutil ls gs://$BUCKET_NAME >/dev/null 2>&1; then
    echo "    ✓ Bucket accessible"
else
    echo "    ✗ Bucket not accessible"
fi

echo "=================================================="
echo "Setup completed!"
echo ""
echo "Created resources:"
echo "- Project: $PROJECT_ID"
echo "- BigQuery Region: $BQ_REGION (US multi-region)"
echo "- Storage Region: $BUCKET_REGION (us-central1)" 
echo "- Dataset: $DATASET_ID"
echo "- Bucket: gs://$BUCKET_NAME"
echo "- Service Account: $SERVICE_ACCOUNT_EMAIL"
echo "- Connection: $CONNECTION_ID"
echo ""
echo "✓ COMPATIBLE SETUP: US BigQuery region can access us-central1 storage"
echo ""
echo "Tables created:"
echo "- weather_data (ready for real data)"
echo "- fire_history (ready for real data)" 
echo "- satellite_images (external table)"
echo "- weather_alerts (external table)"
echo "- fire_risk_analysis (view)"
echo ""
echo "Next steps:"
echo "1. Run upload_multimodal_files.py to upload alert files: /data/weather_messages/ and satellite images: /data/sentinel_images"
echo "2. Upload data into tables Run: python3 dataCollector_v3.py"  
echo "3. Test external tables:"
echo "   SELECT COUNT(*) FROM \`$PROJECT_ID.$DATASET_ID.weather_alerts\`;"
echo ""
echo "If external tables failed, wait 2-3 minutes and rerun:"