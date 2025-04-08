from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.ingestion.reddit_scraper import scrape_multiple_subreddits

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def scrape_reddit():
    """Task to scrape Reddit data"""
    SUBREDDITS = ['AsianBeauty', 'SkincareAddiction', '30PlusSkinCare']
    
    posts_df, comments_df = scrape_multiple_subreddits(
        SUBREDDITS,
        time_period='day',
        limit=100
    )
    
    # Save with date in filename
    date_str = datetime.now().strftime("%Y%m%d")
    
    # Define output directory
    output_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                             'data', 'raw')
    os.makedirs(output_dir, exist_ok=True)
    
    # Save files
    posts_df.to_csv(os.path.join(output_dir, f'reddit_posts_{date_str}.csv'), index=False)
    comments_df.to_csv(os.path.join(output_dir, f'reddit_comments_{date_str}.csv'), index=False)

with DAG(
    'reddit_scraper',
    default_args=default_args,
    description='Daily Reddit scraper for skincare subreddits',
    schedule_interval='0 0 * * *',  # Run at midnight every day
    catchup=False
) as dag:

    scrape_task = PythonOperator(
        task_id='scrape_reddit_data',
        python_callable=scrape_reddit,
    )

    scrape_task 