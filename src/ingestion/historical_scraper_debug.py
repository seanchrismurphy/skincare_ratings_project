#!/usr/bin/env python
import sys
import os
import time
import pandas as pd
from datetime import datetime, timedelta
import argparse
from tqdm import tqdm
import logging
import json
import calendar
import traceback

# Add the project root to the Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(project_root)

# Import the reddit_scraper module directly using a relative import
try:
    from .reddit_scraper import scrape_subreddit
    print("Successfully imported reddit_scraper module")
except ImportError as e:
    print(f"Error importing reddit_scraper module: {e}")
    print(f"Current directory: {current_dir}")
    print(f"Project root: {project_root}")
    print(f"Python path: {sys.path}")
    sys.exit(1)

# Set up logging with DEBUG level
logging.basicConfig(
    level=logging.DEBUG,  # Changed to DEBUG level
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("historical_scraper_debug.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def load_checkpoint(checkpoint_file):
    """Load the last processed date from checkpoint file"""
    if os.path.exists(checkpoint_file):
        try:
            with open(checkpoint_file, 'r') as f:
                checkpoint_data = json.load(f)
                return datetime.fromisoformat(checkpoint_data['last_date'])
        except Exception as e:
            logger.error(f"Error loading checkpoint: {e}")
            logger.error(traceback.format_exc())
    return None

def save_checkpoint(checkpoint_file, current_date):
    """Save the current date to checkpoint file"""
    try:
        with open(checkpoint_file, 'w') as f:
            json.dump({'last_date': current_date.isoformat()}, f)
        logger.info(f"Checkpoint saved: {current_date.isoformat()}")
    except Exception as e:
        logger.error(f"Error saving checkpoint: {e}")
        logger.error(traceback.format_exc())

def get_month_range(start_date, end_date):
    """Generate a list of month start dates between start_date and end_date"""
    month_starts = []
    current = start_date.replace(day=1)
    
    while current <= end_date:
        month_starts.append(current)
        # Move to the first day of the next month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    
    return month_starts

def scrape_historical_data_by_month(subreddits, start_date, end_date, output_dir, limit=500, checkpoint_file=None):
    """
    Scrapes historical data from multiple subreddits, one month at a time.
    
    Args:
        subreddits (list): List of subreddit names to scrape
        start_date (datetime): Start date for scraping
        end_date (datetime): End date for scraping
        output_dir (str): Directory to save the output files
        limit (int): Maximum number of posts to scrape per month
        checkpoint_file (str): Path to checkpoint file for resuming
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    logger.debug(f"Output directory: {output_dir}")
    
    # Load checkpoint if available
    if checkpoint_file:
        last_processed_date = load_checkpoint(checkpoint_file)
        if last_processed_date:
            logger.info(f"Resuming from checkpoint: {last_processed_date.isoformat()}")
            # Start from the month after the last processed date
            if last_processed_date.month == 12:
                start_date = last_processed_date.replace(year=last_processed_date.year + 1, month=1, day=1)
            else:
                start_date = last_processed_date.replace(month=last_processed_date.month + 1, day=1)
    
    # Get list of month start dates
    month_starts = get_month_range(start_date, end_date)
    total_months = len(month_starts)
    
    logger.info(f"Scraping {total_months} months of data from {len(subreddits)} subreddits...")
    logger.info(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    # Iterate through each month
    for month_start in tqdm(month_starts, desc="Months"):
        # Calculate the end of the month
        if month_start.month == 12:
            month_end = month_start.replace(year=month_start.year + 1, month=1, day=1) - timedelta(days=1)
        else:
            month_end = month_start.replace(month=month_start.month + 1, day=1) - timedelta(days=1)
        
        # Adjust month_end if it's beyond our end_date
        if month_end > end_date:
            month_end = end_date
        
        month_str = month_start.strftime("%Y-%m")
        logger.info(f"\nScraping data for {month_str} ({month_start.strftime('%Y-%m-%d')} to {month_end.strftime('%Y-%m-%d')})")
        
        # Check if files already exist for this month
        posts_file = os.path.join(output_dir, f'reddit_posts_{month_str}.csv')
        comments_file = os.path.join(output_dir, f'reddit_comments_{month_str}.csv')
        
        if os.path.exists(posts_file) and os.path.exists(comments_file):
            logger.info(f"Files already exist for {month_str}, skipping...")
            # Save checkpoint
            if checkpoint_file:
                save_checkpoint(checkpoint_file, month_end)
            continue
        
        # Initialize empty lists to store all data for this month
        all_posts = []
        all_comments = []
        
        # Iterate through each subreddit
        for subreddit in subreddits:
            logger.info(f"  Scraping r/{subreddit}...")
            
            try:
                # Scrape data for this month
                logger.debug(f"  Calling scrape_subreddit with subreddit={subreddit}, TIME_PERIOD='month', limit={limit}")
                posts_df, comments_df = scrape_subreddit(
                    subreddit, 
                    TIME_PERIOD='month', 
                    limit=limit
                )
                
                logger.debug(f"  Received posts_df shape: {posts_df.shape if not posts_df.empty else 'empty'}")
                logger.debug(f"  Received comments_df shape: {comments_df.shape if not comments_df.empty else 'empty'}")
                
                # Filter posts to only include those within our date range
                if not posts_df.empty:
                    logger.debug(f"  Posts DataFrame columns: {posts_df.columns.tolist()}")
                    posts_df['created_utc'] = pd.to_datetime(posts_df['created_utc'])
                    posts_df = posts_df[(posts_df['created_utc'] >= month_start) & 
                                       (posts_df['created_utc'] <= month_end)]
                    
                    # Add month and subreddit columns
                    posts_df['scrape_month'] = month_str
                    posts_df['subreddit'] = subreddit
                    all_posts.append(posts_df)
                    logger.debug(f"  Filtered posts_df shape: {posts_df.shape}")
                else:
                    logger.warning(f"  No posts returned for r/{subreddit}")
                
                # Filter comments to only include those within our date range
                if not comments_df.empty:
                    logger.debug(f"  Comments DataFrame columns: {comments_df.columns.tolist()}")
                    comments_df['created_utc'] = pd.to_datetime(comments_df['created_utc'])
                    comments_df = comments_df[(comments_df['created_utc'] >= month_start) & 
                                            (comments_df['created_utc'] <= month_end)]
                    
                    # Add month and subreddit columns
                    comments_df['scrape_month'] = month_str
                    comments_df['subreddit'] = subreddit
                    all_comments.append(comments_df)
                    logger.debug(f"  Filtered comments_df shape: {comments_df.shape}")
                else:
                    logger.warning(f"  No comments returned for r/{subreddit}")
                
                logger.info(f"  Successfully scraped r/{subreddit}")
                
                # Add a delay between subreddits to avoid rate limiting
                time.sleep(10)  # Increased delay between subreddits
                
            except Exception as e:
                logger.error(f"  Error scraping r/{subreddit}: {str(e)}")
                logger.error(traceback.format_exc())
                continue
        
        # Save data for this month
        if all_posts:
            month_posts = pd.concat(all_posts, ignore_index=True)
            logger.debug(f"  Final month_posts shape: {month_posts.shape}")
            month_posts.to_csv(posts_file, index=False)
            logger.info(f"  Saved {len(month_posts)} posts to {posts_file}")
        else:
            logger.warning(f"  No posts collected for {month_str}")
        
        if all_comments:
            month_comments = pd.concat(all_comments, ignore_index=True)
            logger.debug(f"  Final month_comments shape: {month_comments.shape}")
            month_comments.to_csv(comments_file, index=False)
            logger.info(f"  Saved {len(month_comments)} comments to {comments_file}")
        else:
            logger.warning(f"  No comments collected for {month_str}")
        
        # Save checkpoint
        if checkpoint_file:
            save_checkpoint(checkpoint_file, month_end)
        
        # Add a longer delay between months to avoid rate limiting
        time.sleep(60)  # Increased delay between months
    
    logger.info("\nHistorical data scraping completed!")

def main():
    """
    Main function to identify skincare products from Reddit data.
    """
    parser = argparse.ArgumentParser(description='Scrape historical Reddit data')
    parser.add_argument('--subreddits', nargs='+', default=['AsianBeauty', 'SkincareAddiction', '30PlusSkinCare'],
                        help='List of subreddits to scrape')
    parser.add_argument('--years', type=int, default=5,
                        help='Number of years of historical data to scrape')
    parser.add_argument('--limit', type=int, default=500,
                        help='Maximum number of posts to scrape per month')
    parser.add_argument('--output-dir', default='data/historical',
                        help='Directory to save the output files')
    parser.add_argument('--checkpoint-file', default='data/historical/checkpoint.json',
                        help='Path to checkpoint file for resuming')
    parser.add_argument('--test-mode', action='store_true',
                        help='Run in test mode with just one month of data')
    
    args = parser.parse_args()
    
    # Calculate date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=args.years * 365)
    
    # In test mode, just scrape the most recent month
    if args.test_mode:
        logger.info("Running in TEST MODE - only scraping the most recent month")
        if end_date.month == 1:
            start_date = end_date.replace(year=end_date.year - 1, month=12, day=1)
        else:
            start_date = end_date.replace(month=end_date.month - 1, day=1)
    
    # Create output directory - use absolute path from project root
    output_dir = os.path.join(project_root, args.output_dir)
    logger.debug(f"Absolute output directory: {output_dir}")
    
    # Create checkpoint directory if it doesn't exist
    checkpoint_file = os.path.join(project_root, args.checkpoint_file)
    checkpoint_dir = os.path.dirname(checkpoint_file)
    if checkpoint_dir and not os.path.exists(checkpoint_dir):
        os.makedirs(checkpoint_dir, exist_ok=True)
    
    # Print environment information
    logger.info(f"Python version: {sys.version}")
    logger.info(f"Project root: {project_root}")
    logger.info(f"Current directory: {os.getcwd()}")
    logger.info(f"Output directory: {output_dir}")
    
    # Scrape historical data
    scrape_historical_data_by_month(
        args.subreddits,
        start_date,
        end_date,
        output_dir,
        args.limit,
        checkpoint_file
    )

if __name__ == "__main__":
    main() 