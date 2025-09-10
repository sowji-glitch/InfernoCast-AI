# InfernoCast AI: A Multimodal Wildfire Forecasting Platform

This project demonstrates how to fuse structured weather data, unstructured text alerts, and satellite imagery to create a real-time, AI-powered wildfire risk forecast.

[![Built with BigQuery AI](https://img.shields.io/badge/Built%20with-BigQuery%20AI-2196F3?logo=googlebigquery)](https://cloud.google.com/bigquery/docs/introduction-bq-ai)

---

### The Problem: A Siloed View of a Growing Threat

Wildfire prediction is a classic "siloed data" problem. Meteorologists analyze weather data, GIS experts study satellite maps, and fire chiefs read text-based alerts. These critical streams of information are rarely fused until a human manually puts them together—a process that is slow, expensive, and can miss crucial correlations, leading to a reactive rather than proactive response.

### The Solution: A Unified, AI-Powered Forecast

InfernoCast AI breaks down these barriers. We use BigQuery's native multimodal capabilities to create a single, unified view of the situation. This allows a generative AI model to reason across all three data types **simultaneously** to produce an insight that is more holistic and timely than any single source alone.

---

### Demo Video

A video demonstration of the project in action is available on YouTube:
**[Link to Your 3-Minute YouTube Video Here]**

---

### Technical Architecture

Our platform is built entirely on Google Cloud, leveraging BigQuery as the central fusion engine. The architecture is designed to be simple, scalable, and powerful.

![Architectural Diagram](link_to_your_diagram_image_in_the_repo.png)

**The data flows as follows:**
1.  **Ingestion:** Structured weather data is loaded into a native BigQuery table, while unstructured text alerts and satellite images are stored in Google Cloud Storage.
2.  **Unification:** BigQuery **Object Tables** create a SQL interface over the raw files in GCS.
3.  **Analysis:** A single, multimodal query joins the structured table with the Object Tables (using **ObjectRef**) and sends the combined data to the **Gemini Vision** model for a unified analysis.
4.  **Presentation:** The results are displayed in a Jupyter Notebook, demonstrating the actionable insights.

---

### Technology Stack & BigQuery Capabilities Used

This project directly implements the core technologies required for the "Multimodal Pioneer" track.

*    **Object Tables:** Used to create a structured SQL interface over our unstructured text alerts and satellite images in Google Cloud Storage. This is the first step in breaking the data silo.
*    **ObjectRef:** Used to create secure "pointers" from our structured data to the specific unstructured files, enabling the final multimodal `JOIN` and AI analysis.
*    **BigFrames:** Used to provide a Pythonic, pandas-like interface for our multimodal tables, making them accessible for data science and machine learning workflows.
*    **Gemini Pro Model:** Used as the core AI engine within BigQuery to perform the final, unified analysis across all three data types.

---

### Getting Started: How to Run

Follow these steps to set up and run the demo in your own Google Cloud environment.

#### 1. Prerequisites
*   A Google Cloud Project with billing enabled.
*   The BigQuery, Vertex AI, and Cloud Storage APIs enabled.
*   `gcloud` CLI and Python 3.8+ installed and authenticated.

#### 2. Configuration
Clone this repository to your local machine.
```bash
git clone [your-repo-url]
cd InfernoCast-AI

Install the required Python dependencies.
pip install -r requirements.txt

gcloud config set project "your-gcp-project-id"
export PROJECT_ID="your-gcp-project-id"
```
#### 3. Deployment & Data Loading
Run the provided deployment script. This will create the necessary BQ dataset, GCS bucket, and BigQuery Connection.
```bash
# The script requires your Project ID and a region (e.g., us)
./deploy_v1.sh your-gcp-project-id us
# Run the data collection script. This will fetch real weather data and upload all necessary files and tables to your GCP environment.
# Uploads your local weather messages and satellite images to Cloud Storage (provided as part of repo)
python upload_multimodal_files.py
python dataCollector_v3.py
```

#### 4. Check permissions & verify
```
# To test run the below query in BQ Console
SELECT * FROM "PROJECT_ID".napa_wildfire_demo.satellite_images

# If you do not see the images in the ouput of the query above, please set the permission in the next steps
# Get connection service account
bq show  --location us --connection gcs-connection

# Set permissions for the Service account
gcloud projects add-iam-policy-binding inferno3 \
    --member="serviceAccount:your-bq-service-account-id" \
    --role="roles/aiplatform.user"

gcloud projects add-iam-policy-binding inferno3 \
    --member="serviceAccount:your-bq-service-account-id" \
    --role="roles/storage.objectViewer"
```

#### 5. Launch the Demo
Open and run the Multimodal_analysis.ipynb notebook in a Jupyter environment. The notebook is designed to be run from top to bottom. You can also open it in VSCode and execute it from there
```
# Opens interactive notebook demonstrating all multimodal capabilities
jupyter notebook Multimodal_analysis_v1.ipynb
```
