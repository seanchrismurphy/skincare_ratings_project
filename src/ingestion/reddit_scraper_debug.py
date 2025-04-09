import praw
import pandas as pd
from datetime import datetime
import sys
import os
import time
import logging
from prawcore.exceptions import TooManyRequests, RequestException

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('reddit_scraper_debug')

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

# Try to import config, but provide fallbacks
try:
    from config import *
    logger.info("Successfully imported config variables")
except ImportError:
    logger.warning("Could not import config, using environment variables or defaults")
    # Fallback to environment variables
    REDDIT_CLIENT_ID = os.environ.get('REDDIT_CLIENT_ID')
    REDDIT_CLIENT_SECRET = os.environ.get('REDDIT_CLIENT_SECRET')
    REDDIT_USER_AGENT = os.environ.get('REDDIT_USER_AGENT', 'skincare_ratings_project/1.0')
    
    # Check if credentials are available
    if not all([REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET]):
        logger.error("Reddit API credentials not found in environment variables")
        logger.error("Please set REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET environment variables")
        logger.error("Or create a config.py file with these variables")

def initialize_reddit():
    """Initialize and return a Reddit API client"""
    logger.info("Initializing Reddit client")
    logger.info(f"Client ID: {REDDIT_CLIENT_ID[:5]}... (truncated)")
    logger.info(f"User Agent: {REDDIT_USER_AGENT}")
    
    try:
        reddit = praw.Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_CLIENT_SECRET,
            user_agent=REDDIT_USER_AGENT
        )
        # Test the connection
        reddit.user.me()
        logger.info("Successfully connected to Reddit API")
        return reddit
    except Exception as e:
        logger.error(f"Failed to initialize Reddit client: {str(e)}")
        raise

def scrape_subreddit(SUBREDDIT, TIME_PERIOD='day', limit=100):
    """
    Scrapes posts from a specified subreddit with rate limiting and error handling.
    Returns DataFrames for posts and comments.
    """
    logger.info(f"Scraping subreddit: r/{SUBREDDIT}, time period: {TIME_PERIOD}, limit: {limit}")
    
    try:
        reddit = initialize_reddit()
    except Exception as e:
        logger.error(f"Could not initialize Reddit client: {str(e)}")
        return pd.DataFrame(), pd.DataFrame()
    
    try:
        subreddit = reddit.subreddit(SUBREDDIT)
        logger.info(f"Successfully accessed subreddit: r/{SUBREDDIT}")
    except Exception as e:
        logger.error(f"Failed to access subreddit r/{SUBREDDIT}: {str(e)}")
        return pd.DataFrame(), pd.DataFrame()
    
    posts_data = []
    
    try:
        # Get posts with rate limiting
        logger.info(f"Fetching top posts from r/{SUBREDDIT}")
        for i, post in enumerate(subreddit.top(time_filter=TIME_PERIOD, limit=limit)):
            try:
                # Add delay between requests
                time.sleep(1)  # 1 second delay between posts
                
                logger.info(f"Processing post {i+1}/{limit}: {post.id}")
                
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
                    logger.info(f"Retrieved {len(comments)} comments for post {post.id}")
                except Exception as e:
                    logger.error(f"Error getting comments for post {post.id}: {str(e)}")
                
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
                
                logger.info(f"Processed post {i+1}/{limit}: {post.id}")
                
            except TooManyRequests:
                logger.warning("Hit rate limit, waiting 60 seconds...")
                time.sleep(60)
                continue
            except Exception as e:
                logger.error(f"Error processing post: {str(e)}")
                continue
                
    except RequestException as e:
        logger.error(f"Network error: {str(e)}")
        logger.error("Please check your internet connection and try again.")
        if posts_data:  # If we have some data, save it
            logger.info("Saving partial data...")
        else:
            return pd.DataFrame(), pd.DataFrame()
    
    # Create separate DataFrames for posts and comments
    logger.info(f"Creating DataFrames from {len(posts_data)} posts")
    
    if not posts_data:
        logger.warning("No posts were collected. Returning empty DataFrames.")
        return pd.DataFrame(), pd.DataFrame()
    
    posts_df = pd.DataFrame([{k: v for k, v in post.items() if k != 'comments'} 
                           for post in posts_data])
    
    # Flatten comments into their own DataFrame
    comments_df = pd.DataFrame([
        comment
        for post in posts_data
        for comment in post['comments']
    ])
    
    logger.info(f"Created posts DataFrame with {len(posts_df)} rows")
    logger.info(f"Created comments DataFrame with {len(comments_df)} rows")
    
    return posts_df, comments_df

def scrape_multiple_subreddits(subreddits, time_period='day', limit=100):
    """
    Scrapes multiple subreddits and combines the results
    """
    logger.info(f"Scraping multiple subreddits: {subreddits}, time period: {time_period}, limit: {limit}")
    
    all_posts = []
    all_comments = []
    
    for subreddit in subreddits:
        logger.info(f"\nScraping r/{subreddit}...")
        try:
            posts_df, comments_df = scrape_subreddit(subreddit, time_period, limit)
            
            # Add subreddit column to both DataFrames
            if not posts_df.empty:
                posts_df['subreddit'] = subreddit
                all_posts.append(posts_df)
                logger.info(f"Added {len(posts_df)} posts from r/{subreddit}")
            else:
                logger.warning(f"No posts collected from r/{subreddit}")
            
            if not comments_df.empty:
                comments_df['subreddit'] = subreddit
                all_comments.append(comments_df)
                logger.info(f"Added {len(comments_df)} comments from r/{subreddit}")
            else:
                logger.warning(f"No comments collected from r/{subreddit}")
                
            logger.info(f"Successfully scraped r/{subreddit}")
            time.sleep(5)  # Wait between subreddits
            
        except Exception as e:
            logger.error(f"Error scraping r/{subreddit}: {str(e)}")
            continue
    
    # Combine results
    if all_posts:
        combined_posts = pd.concat(all_posts, ignore_index=True)
        logger.info(f"Combined {len(combined_posts)} posts from all subreddits")
    else:
        combined_posts = pd.DataFrame()
        logger.warning("No posts collected from any subreddit")
    
    if all_comments:
        combined_comments = pd.concat(all_comments, ignore_index=True)
        logger.info(f"Combined {len(combined_comments)} comments from all subreddits")
    else:
        combined_comments = pd.DataFrame()
        logger.warning("No comments collected from any subreddit")
    
    return combined_posts, combined_comments

if __name__ == "__main__":
    # Define subreddits to scrape
    SUBREDDITS = ['AsianBeauty', 'SkincareAddiction', '30PlusSkinCare']
    max_retries = 3
    retry_count = 0
    
    # Create data directory if it doesn't exist
    data_dir = os.path.join(project_root, 'data', 'raw')
    os.makedirs(data_dir, exist_ok=True)
    logger.info(f"Data will be saved to: {data_dir}")
    
    while retry_count < max_retries:
        try:
            logger.info(f"Attempt {retry_count + 1} of {max_retries}")
            posts_df, comments_df = scrape_multiple_subreddits(
                SUBREDDITS,
                time_period='day',
                limit=100
            )
            
            if len(posts_df) > 0:
                # Create timestamp for file naming
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                
                # Save posts to CSV
                posts_filename = os.path.join(data_dir, f"reddit_posts_{timestamp}.csv")
                posts_df.to_csv(posts_filename, index=False)
                logger.info(f"Saved {len(posts_df)} posts to {posts_filename}")
                
                # Save comments to CSV
                comments_filename = os.path.join(data_dir, f"reddit_comments_{timestamp}.csv")
                comments_df.to_csv(comments_filename, index=False)
                logger.info(f"Saved {len(comments_df)} comments to {comments_filename}")
                
                break  # Success, exit the retry loop
            else:
                logger.warning("No posts collected. Will retry if attempts remain.")
                retry_count += 1
                if retry_count < max_retries:
                    logger.info(f"Waiting 60 seconds before retry...")
                    time.sleep(60)
                
        except Exception as e:
            retry_count += 1
            logger.error(f"Attempt {retry_count} failed: {str(e)}")
            if retry_count < max_retries:
                logger.info(f"Waiting 60 seconds before retry...")
                time.sleep(60)
            else:
                logger.error("Max retries reached. Please try again later.") 