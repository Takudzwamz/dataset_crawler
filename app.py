# from io import BytesIO
# import os
# import requests
# from pymongo import MongoClient
# from bs4 import BeautifulSoup
# import time
# from dotenv import load_dotenv

# # Load environment variables
# load_dotenv()

# # MongoDB setup
# client = MongoClient(os.environ['MONGO_URI'])
# db = client.crawler_db
# collection = db['apartment_ads']

# # Ping MongoDB to test the connection
# try:
#     client.admin.command('ping')
#     print("Successfully connected to MongoDB!")
# except Exception as e:
#     print(f"MongoDB connection error: {e}")
    

# # Define headers
# HEADERS = {
#     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
# }

# def fetch_page_content(url):
#     response = requests.get(url, headers=HEADERS)
#     if response.status_code == 200:
#         return response.text
#     else:
#         print(f"Failed to fetch page content: {url}")
#         return None

# def extract_ad_urls(page_content):
#     soup = BeautifulSoup(page_content, 'html.parser')
#     ad_links = soup.select('a[itemprop="url"]')  # Update CSS selector as needed
#     return ["https://www.avito.ru" + link.get('href') for link in ad_links]

# def download_image_and_get_binary(image_url):
#     try:
#         response = requests.get(image_url)
#         if response.status_code == 200:
#             return BytesIO(response.content).getvalue()
#         else:
#             print(f"Failed to download image: {image_url}")
#             return None
#     except Exception as e:
#         print(f"Error downloading image: {image_url} - {e}")
#         return None

# def extract_ad_details(ad_url):
#     page_content = fetch_page_content(ad_url)
#     if page_content:
#         soup = BeautifulSoup(page_content, 'html.parser')
        
#         title = soup.find('div', class_='style-sticky-header-prop-PT2mw').text.strip() if soup.find('div', class_='style-sticky-header-prop-PT2mw') else 'No Title'
#         price = soup.find('div', class_='style-price-value-mHi1T').text.strip() if soup.find('div', class_='style-price-value-mHi1T') else 'No Price'
        
#         image_urls = [img['src'] for img in soup.select('ul.images-preview-previewWrapper-R_a4U img')]
#         image_binaries = [download_image_and_get_binary(url) for url in image_urls if url is not None]
#         image_binaries = [binary for binary in image_binaries if binary is not None]
        
#         return {
#             "title": title,
#             "price": price,
#             "image_data": image_binaries,
#             "ad_url": ad_url
#         }
#     else:
#         return None

# def main_crawler():
#     max_pages = 100  # Adjust based on the last page number
#     for current_page in range(1, max_pages + 1):
#         page_url = f"https://www.avito.ru/sankt_peterburg_i_lo/kvartiry/prodam-ASgBAgICAUSSA8YQ?context=H4sIAAAAAAAA_zTKwQnCMBSA4VXCO3tQQZTXJVwhYOxJhBhPIrSeBUfoCiFYLBY6w_82EhGPH3xel3o56UZlH4-Hra-DVD8Gn84x7L5czFWSj3VI_7Baq9BZw0Sht4aBkcHxZLSHo5CtnTk6Jt5kXmS72d1RrKWX6voJAAD__2gGC0x3AAAA&f=ASgBAgICAkSSA8YQ4sgTAg&p={current_page}"
        
#         page_content = fetch_page_content(page_url)
#         if page_content:
#             ad_urls = extract_ad_urls(page_content)
#             for ad_url in ad_urls:
#                 ad_details = extract_ad_details(ad_url)
#                 if ad_details:
#                     collection.insert_one(ad_details)
#                     print(f"Inserted ad details for: {ad_details['title']}")
#         else:
#             print("Failed to retrieve or process page, stopping crawler.")
#             break
        
#         time.sleep(1)  # Polite delay

# if __name__ == "__main__":
#     main_crawler()

from seleniumwire import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import os
import time
import csv
from pathlib import Path
from io import BytesIO
import requests
from PIL import Image
from selenium.common.exceptions import NoSuchElementException, TimeoutException

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# MongoDB setup removed as we're focusing on local storage

# Initialize Selenium Wire WebDriver
options = {
    'disable_encoding': True,
}
driver = webdriver.Chrome(seleniumwire_options=options)

def fetch_page_content(url):
    driver.get(url)
    time.sleep(3)
    return driver.page_source

def extract_ad_urls(page_content):
    soup = BeautifulSoup(page_content, 'html.parser')
    ad_links = soup.select('a[itemprop="url"]')
    return ["https://www.avito.ru" + link.get('href') for link in ad_links]

def save_image_locally(image_url, ad_id):
    try:
        response = requests.get(image_url, stream=True)
        if response.status_code == 200:
            image = Image.open(BytesIO(response.content))
            image_path = f"images/{ad_id}"
            Path(image_path).mkdir(parents=True, exist_ok=True)
            image_filename = os.path.join(image_path, os.path.basename(image_url) + ".png")
            image.save(image_filename, format="PNG")
            return image_filename
    except Exception as e:
        print(f"Failed to save image locally: {image_url}, Error: {e}")
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

    try:
        WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.CSS_SELECTOR, 'div[data-marker="image-frame/image-wrapper"] img')))
    except TimeoutException:
        print("Main image not found or took too long to load.")

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    title = soup.find('div', class_='style-sticky-header-prop-PT2mw').text.strip() if soup.find('div', class_='style-sticky-header-prop-PT2mw') else 'No Title'
    price = soup.find('div', class_='style-price-value-mHi1T').text.strip() if soup.find('div', class_='style-price-value-mHi1T') else 'No Price'
    ad_id = f"{title}_{price}".replace(" ", "_").replace("/", "-").replace(",", "")

    while True:
        try:
            main_image_element = driver.find_element(By.CSS_SELECTOR, 'div[data-marker="image-frame/image-wrapper"] img')
            main_image_url = main_image_element.get_attribute('src')
            if main_image_url not in seen_images:
                seen_images.add(main_image_url)
                image_filename = save_image_locally(main_image_url, ad_id)
                if image_filename:
                    image_data.append(image_filename)
                next_button = driver.find_element(By.CSS_SELECTOR, 'div.image-frame-controlButtonArea-_3TO9.image-frame-controlButton_right-HeBIM[data-delta="1"]')
                driver.execute_script("arguments[0].click();", next_button)
                time.sleep(1)
            else:
                break
        except NoSuchElementException:
            break
        except Exception as e:
            print(f"Error navigating the image carousel: {e}")
            break

    return {
        "title": title,
        "price": price,
        "image_directory": f"images/{ad_id}",
        "ad_url": ad_url
    }

def main_crawler():
    max_pages = 2  # Adjust as needed
    processed_ads = set()

    # Initialize CSV file
    with open('ad_details.csv', mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Title", "Price", "Image Directory", "Ad URL"])

    for current_page in range(1, max_pages + 1):
        page_url = "https://www.avito.ru/sankt_peterburg_i_lo/kvartiry/prodam-ASgBAgICAUSSA8YQ?context=H4sIAAAAAAAA_zTKwQnCMBSA4VXCO3tQQZTXJVwhYOxJhBhPIrSeBUfoCiFYLBY6w_82EhGPH3xel3o56UZlH4-Hra-DVD8Gn84x7L5czFWSj3VI_7Baq9BZw0Sht4aBkcHxZLSHo5CtnTk6Jt5kXmS72d1RrKWX6voJAAD__2gGC0x3AAAA&f=ASgBAgICAkSSA8YQ4sgTAg&p={current_page}"
        
        driver.get(page_url)
        time.sleep(3)
        page_content = driver.page_source
        
        if page_content:
            ad_urls = extract_ad_urls(page_content)
            for ad_url in ad_urls:
                if ad_url not in processed_ads:
                    ad_details = extract_ad_details(ad_url)
                    if ad_details:
                        # Append ad details to CSV
                        with open('ad_details.csv', mode='a', newline='', encoding='utf-8') as file:
                            writer = csv.writer(file)
                            writer.writerow([ad_details["title"], ad_details["price"], ad_details["image_directory"], ad_details["ad_url"]])
                        processed_ads.add(ad_url)
                        print(f"Processed ad: {ad_details['title']}")
                else:
                    print(f"Ad already processed: {ad_url}")
        else:
            print("Failed to retrieve or process page, stopping crawler.")
            break

        time.sleep(1)

if __name__ == "__main__":
    try:
        main_crawler()
    finally:
        driver.quit()
