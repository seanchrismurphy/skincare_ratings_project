# Skincare Rating Prediction Project

This project analyzes Reddit discussions from r/koreanbeauty to predict skincare product ratings.

## Project Structure
- `data/`: Raw and processed datasets
- `notebooks/`: Jupyter notebooks for analysis
- `src/`: Main source code
- `airflow/`: Airflow DAGs for data pipeline
- `dbt/`: Data transformation models
- `mlflow/`: ML experiment tracking

## Setup
1. Install requirements: `pip install -r requirements.txt`
2. Configure API keys in `src/config.py`
3. Run data ingestion: `python src/ingestion/reddit_scraper.py`
