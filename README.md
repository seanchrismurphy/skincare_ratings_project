# Skincare Rating Prediction Project

This project analyzes Reddit discussions from skincare subreddits to predict skincare product ratings. It collects data from multiple sources including Reddit and Adore Beauty to build a comprehensive dataset for analysis.

## Project Structure
- `data/`: Raw and processed datasets
  - `raw/`: Raw data from Reddit and Adore Beauty
  - `processed/`: Cleaned and transformed data
- `notebooks/`: Jupyter notebooks for analysis
- `src/`: Main source code
  - `ingestion/`: Data collection scripts
    - `reddit_scraper.py`: Scrapes posts and comments from skincare subreddits
    - `adore_beauty_scraper.py`: Scrapes product reviews from Adore Beauty
  - `processing/`: Data cleaning and transformation
  - `modeling/`: ML model development
- `airflow/`: Airflow DAGs for data pipeline
  - `dags/`: Contains DAG definitions
    - `reddit_scraper_dag.py`: Daily Reddit data collection
- `dbt/`: Data transformation models
- `mlflow/`: ML experiment tracking

## Features
- **Reddit Data Collection**: Scrapes posts and comments from multiple skincare subreddits (AsianBeauty, SkincareAddiction, 30PlusSkinCare)
- **Adore Beauty Scraping**: Collects product reviews with error handling and proxy rotation
- **Automated Data Pipeline**: Airflow DAG for daily data collection
- **Error Handling**: Robust error handling and retry mechanisms for API rate limits
- **Data Storage**: Organized storage of raw and processed data with timestamps

## Setup
1. Install requirements: `pip install -r requirements.txt`
2. Configure API keys in `src/config.py`:
   - Reddit API credentials
   - Adore Beauty API credentials (if applicable)
3. Run data ingestion manually:
   - Reddit: `python src/ingestion/reddit_scraper.py`
   - Adore Beauty: `python src/ingestion/adore_beauty_scraper.py`

## Airflow Setup
1. Install Airflow: `pip install apache-airflow`
2. Initialize the database: `airflow db init`
3. Create a user: `airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin`
4. Start the scheduler: `airflow scheduler`
5. Start the webserver: `airflow webserver`
6. Access the Airflow UI at http://localhost:8080

## Cloud Deployment
The project can be deployed to Google Cloud Platform using Cloud Composer for managed Airflow:
1. Create a GCP project and enable necessary APIs
2. Set up Cloud Storage for DAGs and data
3. Create a Cloud Composer environment
4. Upload DAGs and source code to Cloud Storage
5. Configure environment variables and connections

## Future Enhancements
- Move airflow dag to cloud deployment on GCP
- Implement ML model training pipeline
- Add data quality checks
- Expand to additional data sources
- Create a web dashboard for insights
