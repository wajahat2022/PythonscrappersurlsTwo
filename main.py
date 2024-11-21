import pandas as pd
import aiohttp
import asyncio
from bs4 import BeautifulSoup
import re
import logging
import validators

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# File paths
INPUT_FILE = 'bookslinks.csv'
OUTPUT_FILE = 'collectedMails.csv'
REJECTED_FILE = 'rejected_urls.csv'

# Data containers
urls = []
all_data = []
rejected_urls = []

# Load URLs from the CSV file
with open(INPUT_FILE, 'r') as f:
    for line in f:
        url = line.strip()
        if validators.url(url):  # Validate URLs
            urls.append(url)
        else:
            logging.warning(f"Invalid URL skipped: {url}")

# Request headers
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
}

# Function to fetch and parse a URL asynchronously
async def fetch(session, url):
    try:
        async with session.get(url, headers=HEADERS, timeout=10) as response:
            if response.status == 200:
                content = await response.text()
                soup = BeautifulSoup(content, 'html.parser')
                email_matches = re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", soup.text)
                all_data.append({'url': url, 'Company Email': email_matches if email_matches else None})
                logging.info(f"Successfully processed {url}")
            else:
                logging.error(f"HTTP error {response.status} for URL: {url}")
                rejected_urls.append({'url': url, 'reason': f"HTTP {response.status}"})
    except Exception as e:
        logging.error(f"Error processing {url}: {e}")
        rejected_urls.append({'url': url, 'reason': str(e)})

# Main function to handle async tasks
async def main():
    async with aiohttp.ClientSession() as session:
        tasks = [fetch(session, url) for url in urls]
        await asyncio.gather(*tasks)

# Run the asyncio event loop
asyncio.run(main())

# Save results to files
if all_data:
    pd.DataFrame(all_data).to_csv(OUTPUT_FILE, index=False)
    logging.info(f"Data saved to {OUTPUT_FILE}")
else:
    logging.info("No data collected.")

if rejected_urls:
    pd.DataFrame(rejected_urls).to_csv(REJECTED_FILE, index=False)
    logging.info(f"Rejected URLs saved to {REJECTED_FILE}")
else:
    logging.info("No URLs were rejected.")

logging.info("Complete.")
