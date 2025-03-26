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

def scrape_subreddit(SUBREDDIT, TIME_PERIOD, limit=None):
    """
    Scrapes posts and comments from a specified subreddit.
    
    Args:
        SUBREDDIT (str): Name of the subreddit to scrape
        TIME_PERIOD (str): Time filter for posts
        limit (int): Number of posts to fetch (None for maximum available)
    """
    reddit = initialize_reddit()
    subreddit = reddit.subreddit(SUBREDDIT)
    
    posts_data = []
    
    # Use limit parameter in top() to get more posts
    for post in subreddit.top(time_filter=TIME_PERIOD, limit=limit):
        # Get all comments for this post
        post.comments.replace_more(limit=None)  # Expand all comment trees
        comments = []
        
        # Process all comments
        for comment in post.comments.list():
            comments.append({
                'comment_id': comment.id,
                'post_id': post.id,
                'body': comment.body,
                'score': comment.score,
                'created_utc': datetime.fromtimestamp(comment.created_utc),
                'author': str(comment.author),  # Convert author to string in case it's None
            })
        
        # Store post data
        posts_data.append({
            'id': post.id,
            'title': post.title,
            'body': post.selftext,
            'score': post.score,
            'created_utc': datetime.fromtimestamp(post.created_utc),
            'num_comments': post.num_comments,
            'comments': comments  # Add comments to post data
        })
    
    # Create separate DataFrames for posts and comments
    posts_df = pd.DataFrame([{k: v for k, v in post.items() if k != 'comments'} 
                           for post in posts_data])
    
    # Flatten comments into their own DataFrame
    comments_df = pd.DataFrame([
        comment
        for post in posts_data
        for comment in post['comments']
    ])
    
    return posts_df, comments_df

if __name__ == "__main__":
    # Get 1000 posts (or None for maximum available)
    posts_df, comments_df = scrape_subreddit(SUBREDDIT, TIME_PERIOD, limit=1000)
    
    # Create timestamp for file naming
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save posts to CSV
    posts_filename = f"{RAW_DATA_DIR}/reddit_posts_{timestamp}.csv"
    posts_df.to_csv(posts_filename, index=False)
    print(f"Saved {len(posts_df)} posts to {posts_filename}")
    
    # Save comments to CSV
    comments_filename = f"{RAW_DATA_DIR}/reddit_comments_{timestamp}.csv"
    comments_df.to_csv(comments_filename, index=False)
    print(f"Saved {len(comments_df)} comments to {comments_filename}")
