import praw
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

def scrape_subreddit(SUBREDDIT, TIME_PERIOD):
    """
    Scrapes posts from a specified subreddit for a given time period.
    
    Args:
        SUBREDDIT (str): Name of the subreddit to scrape
        TIME_PERIOD (str): Time filter for posts (e.g. 'day', 'week', 'month', 'year', 'all')
    
    Returns:
        pd.DataFrame: DataFrame containing post data with columns:
            - id: Unique post identifier
            - title: Post title
            - body: Post text content
            - score: Number of upvotes
            - created_utc: Post creation timestamp
            - num_comments: Number of comments on the post
    """
    # Initialize Reddit API connection
    reddit = initialize_reddit()
    subreddit = reddit.subreddit(SUBREDDIT)
    
    # List to store post data
    posts_data = []
    
    # Iterate through top posts and extract relevant fields
    for post in subreddit.top(time_filter=TIME_PERIOD):
        posts_data.append({
            'id': post.id,
            'title': post.title,
            'body': post.selftext,
            'score': post.score,
            'created_utc': datetime.fromtimestamp(post.created_utc),
            'num_comments': post.num_comments,
        })
    
    # Convert list of post dictionaries to DataFrame
    return pd.DataFrame(posts_data)

if __name__ == "__main__":
    df = scrape_subreddit(SUBREDDIT, TIME_PERIOD)
    
    # Create timestamp for file naming
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{RAW_DATA_DIR}/reddit_posts_{timestamp}.csv"
    
    # Save to CSV
    df.to_csv(filename, index=False)
    print(f"Saved {len(df)} posts to {filename}")
