import cloudscraper
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time
import os
import sys
from urllib.parse import urljoin
import random

class AdoreBeautyScraper:
    def __init__(self):
        self.base_url = "https://www.adorebeauty.com.au"
        self.skincare_url = "https://www.adorebeauty.com.au/c/skin-care.html"
        # Create a cloudscraper session
        self.scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'mobile': False
            }
        )
        self.product_urls = set()
        self.consecutive_errors = 0  # Track consecutive errors

    def get_product_urls_from_page(self, page_number):
        """Extract product URLs from a single page"""
        url = f"{self.skincare_url}?p={page_number}"
        print(f"Scanning page {page_number}...")
        
        try:
            # Use cloudscraper instead of requests
            response = self.scraper.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find all product containers
            product_containers = soup.find_all('div', class_='relative rounded-md border-[1px] border-[#e1dfdf]')
            
            # Reset consecutive errors on success
            if len(product_containers) > 0:
                self.consecutive_errors = 0
            
            # Extract URLs from each container
            for container in product_containers:
                product_link = container.find('a', href=True)
                if product_link and '/p/' in product_link['href']:
                    full_url = urljoin(self.base_url, product_link['href'])
                    self.product_urls.add(full_url)
            
            print(f"Found {len(product_containers)} products on page {page_number}")
            return len(product_containers) > 0  # Return True if products were found
            
        except Exception as e:
            print(f"Error processing page {page_number}: {str(e)}")
            self.consecutive_errors += 1
            print(f"Consecutive errors: {self.consecutive_errors}")
            return True  # Return True to continue to next page

    def collect_all_product_urls(self, max_pages=None):
        """Iterate through all pages and collect product URLs"""
        page_number = 1
        
        while True:
            if max_pages and page_number > max_pages:
                print(f"Reached maximum pages limit ({max_pages})")
                break
                
            if self.consecutive_errors >= 5:
                print("Too many consecutive errors. Stopping...")
                break
            
            # Random delay between pages
            if page_number > 1:
                delay = random.uniform(1, 3)
                print(f"Waiting {delay:.1f} seconds...")
                time.sleep(delay)
            
            # Get URLs from current page
            found_products = self.get_product_urls_from_page(page_number)
            
            # If no products found, stop
            if not found_products:
                print(f"No more products found after page {page_number-1}")
                break
                
            page_number += 1
        
        print(f"\nTotal unique product URLs collected: {len(self.product_urls)}")
        return list(self.product_urls)

    def save_urls_to_file(self, filename=None):
        """Save collected URLs to a file"""
        if filename is None:
            # Create filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"product_urls_{timestamp}.txt"
            
        filepath = os.path.join("data", "raw", filename)
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        with open(filepath, 'w') as f:
            for url in self.product_urls:
                f.write(f"{url}\n")
        
        print(f"Saved {len(self.product_urls)} URLs to {filepath}")

if __name__ == "__main__":
    # Initialize scraper
    scraper = AdoreBeautyScraper()
    
    # Collect URLs (limit to 5 pages for testing)
    scraper.collect_all_product_urls(max_pages=200)
    
    # Save URLs to file
    scraper.save_urls_to_file()