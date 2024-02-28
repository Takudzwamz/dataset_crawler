from io import BytesIO
import os
import requests
from pymongo import MongoClient
from bs4 import BeautifulSoup
import time
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# MongoDB setup
client = MongoClient(os.environ['MONGO_URI'])
db = client.crawler_db
collection = db['apartment_ads']

# Ping MongoDB to test the connection
try:
    client.admin.command('ping')
    print("Successfully connected to MongoDB!")
except Exception as e:
    print(f"MongoDB connection error: {e}")
    

# Define headers
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
}

def fetch_page_content(url):
    response = requests.get(url, headers=HEADERS)
    if response.status_code == 200:
        return response.text
    else:
        print(f"Failed to fetch page content: {url}")
        return None

def extract_ad_urls(page_content):
    soup = BeautifulSoup(page_content, 'html.parser')
    ad_links = soup.select('a[itemprop="url"]')  # Update CSS selector as needed
    return ["https://www.avito.ru" + link.get('href') for link in ad_links]

def download_image_and_get_binary(image_url):
    try:
        response = requests.get(image_url)
        if response.status_code == 200:
            return BytesIO(response.content).getvalue()
        else:
            print(f"Failed to download image: {image_url}")
            return None
    except Exception as e:
        print(f"Error downloading image: {image_url} - {e}")
        return None

def extract_ad_details(ad_url):
    page_content = fetch_page_content(ad_url)
    if page_content:
        soup = BeautifulSoup(page_content, 'html.parser')
        
        title = soup.find('div', class_='style-sticky-header-prop-PT2mw').text.strip() if soup.find('div', class_='style-sticky-header-prop-PT2mw') else 'No Title'
        price = soup.find('div', class_='style-price-value-mHi1T').text.strip() if soup.find('div', class_='style-price-value-mHi1T') else 'No Price'
        
        image_urls = [img['src'] for img in soup.select('ul.images-preview-previewWrapper-R_a4U img')]
        image_binaries = [download_image_and_get_binary(url) for url in image_urls if url is not None]
        image_binaries = [binary for binary in image_binaries if binary is not None]
        
        return {
            "title": title,
            "price": price,
            "image_data": image_binaries,
            "ad_url": ad_url
        }
    else:
        return None

def main_crawler():
    max_pages = 100  # Adjust based on the last page number
    for current_page in range(1, max_pages + 1):
        page_url = f"https://www.avito.ru/sankt_peterburg_i_lo/kvartiry/prodam-ASgBAgICAUSSA8YQ?context=H4sIAAAAAAAA_zTKwQnCMBSA4VXCO3tQQZTXJVwhYOxJhBhPIrSeBUfoCiFYLBY6w_82EhGPH3xel3o56UZlH4-Hra-DVD8Gn84x7L5czFWSj3VI_7Baq9BZw0Sht4aBkcHxZLSHo5CtnTk6Jt5kXmS72d1RrKWX6voJAAD__2gGC0x3AAAA&f=ASgBAgICAkSSA8YQ4sgTAg&p={current_page}"
        
        page_content = fetch_page_content(page_url)
        if page_content:
            ad_urls = extract_ad_urls(page_content)
            for ad_url in ad_urls:
                ad_details = extract_ad_details(ad_url)
                if ad_details:
                    collection.insert_one(ad_details)
                    print(f"Inserted ad details for: {ad_details['title']}")
        else:
            print("Failed to retrieve or process page, stopping crawler.")
            break
        
        time.sleep(1)  # Polite delay

if __name__ == "__main__":
    main_crawler()
