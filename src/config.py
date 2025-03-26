import os
from dotenv import load_dotenv

# Load environment variables from .env file
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
