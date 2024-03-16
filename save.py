import os
import csv
from dotenv import load_dotenv
from pymongo import MongoClient
from PIL import Image
from io import BytesIO

# Load environment variables
load_dotenv()

# MongoDB setup
MONGO_URI = os.getenv('MONGO_URI')
client = MongoClient(MONGO_URI)
db = client.crawler_db
collection = db['apartment_ads']

def read_processed_ads():
    try:
        with open('processed_ads.txt', 'r') as file:
            return set(file.read().splitlines())
    except FileNotFoundError:
        return set()

def write_processed_ads(processed_ads):
    with open('processed_ads.txt', 'w') as file:
        for ad_url in processed_ads:
            file.write(f"{ad_url}\n")

def save_images_locally(doc, root_dir):
    ad_title = doc["title"].replace("/", "-")[:50]  # Ensure title is file system friendly and limit length
    ad_dir = os.path.join(root_dir, ad_title)
    os.makedirs(ad_dir, exist_ok=True)

    for img_idx, img_binary in enumerate(doc["image_data"]):
        img = Image.open(BytesIO(img_binary))
        image_path = os.path.join(ad_dir, f"image_{img_idx}.png")
        img.save(image_path, "PNG")
        print(f"Saved {image_path}")

    return ad_dir

def process_ads_and_save():
    processed_ads = read_processed_ads()
    documents = collection.find({"ad_url": {"$nin": list(processed_ads)}})
    root_dir = "dataset_images"
    os.makedirs(root_dir, exist_ok=True)

    csv_file = "ads_details.csv"
    csv_exists = os.path.exists(csv_file)

    for doc in documents:
        folder_path = save_images_locally(doc, root_dir)
        ad_data = [doc["title"], doc["price"], doc["ad_url"], folder_path]

        # Append the current ad's details to the CSV file immediately
        with open(csv_file, mode="a", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            if not csv_exists:
                writer.writerow(["Title", "Price", "Ad URL", "Local Folder"])
                csv_exists = True  # Ensure headers aren't written again
            writer.writerow(ad_data)

        processed_ads.add(doc["ad_url"])
        # Update the processed_ads.txt file immediately
        write_processed_ads(processed_ads)
        print(f"Processed and saved ad: {doc['title']}")

process_ads_and_save()




