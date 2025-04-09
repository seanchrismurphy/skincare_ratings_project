import cloudscraper
from bs4 import BeautifulSoup
import pandas as pd
import json
import time
import random
from datetime import datetime
import os
from fake_useragent import UserAgent
import requests
from urllib3.exceptions import ProxyError

class AdoreReviewScraper:
    def __init__(self, use_proxies=False):
        self.user_agent = UserAgent()
        self.use_proxies = use_proxies
        self.proxy_list = self.get_proxy_list() if use_proxies else []
        self.initialize_scraper()
        self.failed_proxies = set()

    def get_proxy_list(self):
        """Get a list of free proxies"""
        try:
            response = requests.get('https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt')
            proxies = response.text.split('\n')
            return [p.strip() for p in proxies if p.strip()]
        except:
            print("Could not fetch proxy list, will proceed without proxies")
            return []

    def get_random_proxy(self):
        """Get a random proxy from the list, excluding failed ones"""
        available_proxies = [p for p in self.proxy_list if p not in self.failed_proxies]
        if available_proxies:
            return random.choice(available_proxies)
        return None

    def initialize_scraper(self):
        """Initialize or reinitialize the scraper with new identity"""
        self.scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'mobile': False
            }
        )
        
        # Update headers
        self.scraper.headers.update({
            'User-Agent': self.user_agent.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
        })
        
        # Only add proxy if we're using them
        if self.use_proxies:
            proxy = self.get_random_proxy()
            if proxy:
                self.scraper.proxies = {
                    'http': f'http://{proxy}',
                    'https': f'http://{proxy}'
                }
                print(f"Using proxy: {proxy}")

    def rotate_identity(self):
        """Rotate user agent and optionally proxy"""
        print("Rotating identity...")
        self.initialize_scraper()
        time.sleep(random.uniform(2, 4))

    def extract_reviews(self, product_data):
        """Extract reviews from product data into a separate DataFrame"""
        reviews = product_data.get('review', [])
        reviews_data = []
        
        for review in reviews:
            review_data = {
                'review_id': hash(review.get('reviewBody', '')),  # Create unique ID
                'product_sku': product_data.get('sku'),
                'product_name': product_data.get('name'),
                'brand': product_data.get('brand', {}).get('name'),
                'author': review.get('author', {}).get('name'),
                'title': review.get('name'),
                'body': review.get('reviewBody'),
                'rating': review.get('reviewRating', {}).get('ratingValue'),
                'date_published': review.get('datePublished'),
            }
            reviews_data.append(review_data)
            
        return reviews_data

    def get_product_reviews(self, url, max_retries=3):
        """Extract product data and reviews from a product page"""
        retries = 0
        while retries < max_retries:
            try:
                # Random delay between requests
                time.sleep(random.uniform(2, 5))
                
                # Get the page
                response = self.scraper.get(url, timeout=10)
                
                if response.status_code in [403, 429, 503]:
                    retries += 1
                    print(f"Got error {response.status_code} (attempt {retries}/{max_retries})")
                    self.rotate_identity()
                    time.sleep(random.uniform(5, 10))
                    continue
                    
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Find the script containing product data
                script = soup.find('script', {'id': 'product_structured_data'})
                if not script:
                    print(f"No product data found for {url}")
                    return None
                
                # Parse the JSON data
                product_data = json.loads(script.string)
                reviews_data = self.extract_reviews(product_data)
                return reviews_data
                
            except ProxyError as e:
                print(f"Proxy error: {str(e)}")
                if self.scraper.proxies:
                    current_proxy = self.scraper.proxies['http'].split('://')[-1]
                    self.failed_proxies.add(current_proxy)
                    print(f"Marked proxy as failed: {current_proxy}")
                self.rotate_identity()
                continue
                
            except Exception as e:
                print(f"Error processing {url}: {str(e)}")
                retries += 1
                if retries < max_retries:
                    self.rotate_identity()
                    continue
                return None
        
        print(f"Failed to process {url} after {max_retries} attempts")
        return None

    def scrape_reviews_from_urls(self, urls, output_file=None):
        """Scrape reviews for a list of URLs"""
        all_reviews = []
        
        for i, url in enumerate(urls, 1):
            print(f"\nProcessing product {i}/{len(urls)}: {url}")
            reviews_data = self.get_product_reviews(url)
            
            if reviews_data:
                all_reviews.extend(reviews_data)
                print(f"Found {len(reviews_data)} reviews")
                
                # Save progress more frequently (every 5 products)
                if i % 5 == 0:
                    self.save_reviews(all_reviews, output_file, interim=True)
                    
                # Rotate identity periodically
                if i % 10 == 0:
                    self.rotate_identity()
            
            # Random delay between products
            sleep_time = random.uniform(3, 7)
            print(f"Waiting {sleep_time:.1f} seconds before next product...")
            time.sleep(sleep_time)
        
        # Save final results
        if all_reviews:
            return self.save_reviews(all_reviews, output_file)
        
        return pd.DataFrame()

    def save_reviews(self, reviews_data, output_file=None, interim=False):
        """Save reviews to CSV file"""
        # Create DataFrame
        reviews_df = pd.DataFrame(reviews_data)
        
        if output_file is None:
            # Create filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"data/raw/reviews_{timestamp}.csv"
        
        # If this is an interim save, add _interim to filename
        if interim:
            base, ext = os.path.splitext(output_file)
            output_file = f"{base}_interim{ext}"
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        # Save to CSV
        reviews_df.to_csv(output_file, index=False)
        print(f"Saved {len(reviews_df)} reviews to {output_file}")
        
        return reviews_df

if __name__ == "__main__":
    scraper = AdoreReviewScraper(use_proxies=False)
    
    # Find most recent product URLs file with explicit path handling
    raw_data_dir = os.path.join(os.getcwd(), "data", "raw")
    
    # Create directory if it doesn't exist
    if not os.path.exists(raw_data_dir):
        print(f"Creating directory: {raw_data_dir}")
        os.makedirs(raw_data_dir, exist_ok=True)
    
    print(f"Looking for URL files in: {raw_data_dir}")
    
    try:
        url_files = [f for f in os.listdir(raw_data_dir) if f.startswith('product_urls_')]
        
        if not url_files:
            print("No product URL files found!")
            print(f"Current directory contents: {os.listdir(raw_data_dir)}")
            exit()
        
        latest_url_file = sorted(url_files)[-1]
        url_filepath = os.path.join(raw_data_dir, latest_url_file)
        
        print(f"Loading URLs from: {latest_url_file}")
        
        # Read URLs with error handling
        try:
            with open(url_filepath, 'r') as f:
                urls = [line.strip() for line in f if line.strip()]
            
            print(f"Found {len(urls)} URLs to process")
            
            if not urls:
                print("URL file is empty!")
                exit()
                
        except Exception as e:
            print(f"Error reading URL file: {str(e)}")
            exit()
        
        # Create output filename based on input filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(
            raw_data_dir, 
            f"reviews_{timestamp}.csv"
        )
        
        print(f"\nWill save results to: {output_file}")
        
        try:
            # Scrape reviews with periodic saving
            reviews_df = scraper.scrape_reviews_from_urls(
                urls=urls,
                output_file=output_file
            )
            
            print("\nScraping completed!")
            print(f"Total reviews collected: {len(reviews_df)}")
            
        except KeyboardInterrupt:
            print("\nScraping interrupted by user. Partial results have been saved.")
        except Exception as e:
            print(f"\nError during scraping: {str(e)}")
            print("Partial results have been saved.")
            
    except Exception as e:
        print(f"Critical error: {str(e)}")
        print(f"Current working directory: {os.getcwd()}")
        print(f"Directory contents: {os.listdir('.')}")