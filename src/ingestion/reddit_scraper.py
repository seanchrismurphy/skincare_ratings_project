import praw
import pandas as pd
from datetime import datetime
import sys
import os
import time
from prawcore.exceptions import TooManyRequests, RequestException

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import *

def initialize_reddit():
    return praw.Reddit(
        client_id=REDDIT_CLIENT_ID,
        client_secret=REDDIT_CLIENT_SECRET,
        user_agent=REDDIT_USER_AGENT
    )

def scrape_subreddit(SUBREDDIT, TIME_PERIOD, limit=1000):
    """
    Scrapes posts from a specified subreddit with rate limiting and error handling.
    """
    reddit = initialize_reddit()
    subreddit = reddit.subreddit(SUBREDDIT)
    
    posts_data = []
    
    try:
        # Get posts with rate limiting
        for post in subreddit.top(time_filter=TIME_PERIOD, limit=limit):
            try:
                # Add delay between requests
                time.sleep(.5)  # 2 second delay between posts
                
                # Get comments with error handling
                comments = []
                try:
                    post.comments.replace_more(limit=None)
                    for comment in post.comments.list():
                        comments.append({
                            'comment_id': comment.id,
                            'post_id': post.id,
                            'body': comment.body,
                            'score': comment.score,
                            'created_utc': datetime.fromtimestamp(comment.created_utc),
                            'author': str(comment.author)
                        })
                except Exception as e:
                    print(f"Error getting comments for post {post.id}: {str(e)}")
                
                # Store post data
                posts_data.append({
                    'id': post.id,
                    'title': post.title,
                    'body': post.selftext,
                    'score': post.score,
                    'created_utc': datetime.fromtimestamp(post.created_utc),
                    'num_comments': post.num_comments,
                    'comments': comments
                })
                
                print(f"Processed post {len(posts_data)}: {post.id}")
                
            except TooManyRequests:
                print("Hit rate limit, waiting 60 seconds...")
                time.sleep(60)
                continue
            except Exception as e:
                print(f"Error processing post: {str(e)}")
                continue
                
    except RequestException as e:
        print(f"Network error: {str(e)}")
        print("Please check your internet connection and try again.")
        if posts_data:  # If we have some data, save it
            print("Saving partial data...")
        else:
            return pd.DataFrame(), pd.DataFrame()
    
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
    # Add retry logic for the main execution
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            posts_df, comments_df = scrape_subreddit(SUBREDDIT, TIME_PERIOD, limit=100)
            
            if len(posts_df) > 0:
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
                
                break  # Success, exit the retry loop
                
        except Exception as e:
            retry_count += 1
            print(f"Attempt {retry_count} failed: {str(e)}")
            if retry_count < max_retries:
                print(f"Waiting 60 seconds before retry...")
                time.sleep(60)
            else:
                print("Max retries reached. Please try again later.")
