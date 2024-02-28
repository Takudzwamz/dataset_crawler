import os
from dotenv import load_dotenv
from pymongo import MongoClient
from PIL import Image
from io import BytesIO

# Load environment variables
load_dotenv()
# MongoDB setup
client = MongoClient(os.environ['MONGO_URI'])
db = client.crawler_db
collection = db['apartment_ads']

# Fetch three documents
documents = collection.find().limit(3)  # Adjust the query as necessary

# Create a root directory for the images
root_dir = 'apartment_images'
os.makedirs(root_dir, exist_ok=True)

for doc_idx, document in enumerate(documents, start=1):
    # Create a directory for each document's images
    doc_dir = os.path.join(root_dir, f'{doc_idx:03}-apartment-data')
    os.makedirs(doc_dir, exist_ok=True)

    # Assuming 'image_data' contains the binary images directly
    for img_idx, img_binary in enumerate(document['image_data']):
        # Directly use the binary data, assuming it's already in the correct format
        try:
            img = Image.open(BytesIO(img_binary))
            img_path = os.path.join(doc_dir, f'image_{img_idx}.png')
            img.save(img_path, 'PNG')  # Save the image as PNG
            print(f"Image {img_idx} saved successfully in {doc_dir}.")
        except Exception as e:
            print(f"Failed to open or save image {img_idx} in {doc_dir}: {e}")
