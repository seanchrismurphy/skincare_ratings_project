import os

def create_directory_structure():
    # Base project directory structure
    directories = [
        'data/raw',
        'data/processed',
        'notebooks',
        'src/ingestion',
        'src/transform',
        'src/training',
        'src/deployment',
        'airflow/dags',
        'dbt/models',
        'mlflow'
    ]
    
    # Create directories
    for dir_path in directories:
        os.makedirs(dir_path, exist_ok=True)
        
    # Create initial files
    files = {
        'README.md': '''# Skincare Rating Prediction Project

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
''',
        
        '.gitignore': '''# Python
__pycache__/
*.py[cod]
*$py.class
.env
venv/

# Jupyter
.ipynb_checkpoints

# Data
data/raw/*
data/processed/*
!data/raw/.gitkeep
!data/processed/.gitkeep

# MLflow
mlflow/mlruns/

# IDE
.vscode/
.idea/
''',
        
        'requirements.txt': '''praw==7.7.1
pandas==2.0.0
scikit-learn==1.2.2
fastapi==0.95.1
uvicorn==0.21.1
python-dotenv==1.0.0
requests==2.28.2
beautifulsoup4==4.12.2
jupyter==1.0.0
mlflow==2.3.0
apache-airflow==2.6.0
''',
        
        'src/config.py': '''import os
from dotenv import load_dotenv

load_dotenv()

# Reddit API Configuration
REDDIT_CLIENT_ID = os.getenv('REDDIT_CLIENT_ID')
REDDIT_CLIENT_SECRET = os.getenv('REDDIT_CLIENT_SECRET')
REDDIT_USER_AGENT = os.getenv('REDDIT_USER_AGENT')

# Subreddit Configuration
SUBREDDIT = 'koreanbeauty'
TIME_PERIOD = 'year'  # 'all', 'year', 'month', 'week', 'day'

# File Paths
RAW_DATA_DIR = 'data/raw'
PROCESSED_DATA_DIR = 'data/processed'

# Model Configuration
MODEL_PARAMS = {
    'random_state': 42,
    'test_size': 0.2
}
''',
        
        'src/ingestion/reddit_scraper.py': '''import praw
import pandas as pd
from datetime import datetime
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import *

def initialize_reddit():
    return praw.Reddit(
        client_id=REDDIT_CLIENT_ID,
        client_secret=REDDIT_CLIENT_SECRET,
        user_agent=REDDIT_USER_AGENT
    )

def scrape_subreddit():
    reddit = initialize_reddit()
    subreddit = reddit.subreddit(SUBREDDIT)
    
    posts_data = []
    
    for post in subreddit.top(time_filter=TIME_PERIOD):
        posts_data.append({
            'id': post.id,
            'title': post.title,
            'body': post.selftext,
            'score': post.score,
            'created_utc': datetime.fromtimestamp(post.created_utc),
            'num_comments': post.num_comments,
        })
    
    return pd.DataFrame(posts_data)

if __name__ == "__main__":
    df = scrape_subreddit()
    
    # Create timestamp for file naming
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{RAW_DATA_DIR}/reddit_posts_{timestamp}.csv"
    
    # Save to CSV
    df.to_csv(filename, index=False)
    print(f"Saved {len(df)} posts to {filename}")
'''
    }
    
    # Create all files
    for file_path, content in files.items():
        with open(file_path, 'w') as f:
            f.write(content)
            
    # Create empty .gitkeep files
    for dir_path in ['data/raw', 'data/processed']:
        with open(f'{dir_path}/.gitkeep', 'w') as f:
            pass

if __name__ == "__main__":
    create_directory_structure()
    print("Project structure created successfully!")