from seleniumwire import webdriver  # Import from seleniumwire
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import os
import time
from dotenv import load_dotenv
from pymongo import MongoClient
from io import BytesIO
import requests
from PIL import Image
from selenium.common.exceptions import NoSuchElementException, TimeoutException

# Load environment variables
load_dotenv()

# MongoDB setup
client = MongoClient(os.environ['MONGO_URI'])
db = client.crawler_db
collection = db['apartment_ads']
processed_collection = db['processed_ads']  # Additional collection for tracking processed ads


# Initialize Selenium Wire WebDriver
options = {
    'disable_encoding': True,  # This option prevents automatic decoding of the response body and allows binary data to be accessed
}
driver = webdriver.Chrome(seleniumwire_options=options)

def fetch_page_content(url):
    driver.get(url)
    time.sleep(3)  # Adjust as necessary for page load
    return driver.page_source

def extract_ad_urls(page_content):
    soup = BeautifulSoup(page_content, 'html.parser')
    ad_links = soup.select('a[itemprop="url"]')
    return ["https://www.avito.ru" + link.get('href') for link in ad_links]

def download_and_convert_image(image_url):
    try:
        response = requests.get(image_url, stream=True)
        if response.status_code == 200:
            # Convert WEBP to a more common format (e.g., PNG) if necessary
            image = Image.open(BytesIO(response.content))
            with BytesIO() as buffer:
                image.save(buffer, format="PNG")  # Save as PNG
                return buffer.getvalue()
    except Exception as e:
        print(f"Failed to download or convert image: {image_url}, Error: {e}")
    return None


def extract_ad_details(ad_url):
    driver.get(ad_url)
    try:
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div.style-sticky-header-prop-PT2mw')))
    except TimeoutException:
        print("Timed out waiting for page to load")
        return None

    image_data = []
    seen_images = set()

    # Ensure the carousel is loaded
    try:
        WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.CSS_SELECTOR, 'div[data-marker="image-frame/image-wrapper"] img')))
    except TimeoutException:
        print("Main image not found or took too long to load.")

    while True:
        try:
            # Extract the current main image URL
            main_image_element = driver.find_element(By.CSS_SELECTOR, 'div[data-marker="image-frame/image-wrapper"] img')
            main_image_url = main_image_element.get_attribute('src')
            # Check if this image has already been processed
            if main_image_url not in seen_images:
                seen_images.add(main_image_url)
                main_image_binary = download_and_convert_image(main_image_url)
                if main_image_binary:
                    image_data.append(main_image_binary)
                # Try to click the next button
                next_button = driver.find_element(By.CSS_SELECTOR, 'div.image-frame-controlButtonArea-_3TO9.image-frame-controlButton_right-HeBIM[data-delta="1"]')
                driver.execute_script("arguments[0].click();", next_button)
                time.sleep(1)  # Wait for the next image to load
            else:
                # If seen, we've looped through all images, break the loop
                break
        except NoSuchElementException:
            # If there's no next button, we're at the end of the carousel
            break
        except Exception as e:
            print(f"Error navigating the image carousel: {e}")
            break

    # Extract title and price using BeautifulSoup
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    title = soup.find('div', class_='style-sticky-header-prop-PT2mw').text.strip() if soup.find('div', class_='style-sticky-header-prop-PT2mw') else 'No Title'
    price = soup.find('div', class_='style-price-value-mHi1T').text.strip() if soup.find('div', class_='style-price-value-mHi1T') else 'No Price'

    return {
        "title": title,
        "price": price,
        "image_data": image_data,
        "ad_url": ad_url
    }

def ad_already_processed(ad_url):
    # Check if the ad URL is in the processed collection
    return processed_collection.find_one({"ad_url": ad_url}) is not None

def main_crawler():
    max_pages = 100
    for current_page in range(1, max_pages + 1):
        page_url = f"https://www.avito.ru/sankt_peterburg_i_lo/kvartiry/prodam-ASgBAgICAUSSA8YQ?context=H4sIAAAAAAAA_zTKwQnCMBSA4VXCO3tQQZTXJVwhYOxJhBhPIrSeBUfoCiFYLBY6w_82EhGPH3xel3o56UZlH4-Hra-DVD8Gn84x7L5czFWSj3VI_7Baq9BZw0Sht4aBkcHxZLSHo5CtnTk6Jt5kXmS72d1RrKWX6voJAAD__2gGC0x3AAAA&f=ASgBAgICAkSSA8YQ4sgTAg&p={current_page}"
        driver.get(page_url)
        time.sleep(3)
        page_content = driver.page_source
        if page_content:
            ad_urls = extract_ad_urls(page_content)
            for ad_url in ad_urls:
                if not ad_already_processed(ad_url):  # Check if ad has been processed
                    try:
                        ad_details = extract_ad_details(ad_url)
                        if ad_details:
                            collection.insert_one(ad_details)
                            processed_collection.insert_one({"ad_url": ad_url})  # Mark as processed
                            print(f"Inserted ad details for: {ad_details['title']}")
                    except Exception as e:
                        print(f"Error processing ad {ad_url}: {e}")
                else:
                    print(f"Ad already processed: {ad_url}")
        else:
            print("Failed to retrieve or process page, stopping crawler.")
        time.sleep(1)

if __name__ == "__main__":
    try:
        main_crawler()
    finally:
        driver.quit()



