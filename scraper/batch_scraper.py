"""
OpenFoodFacts Product Scraper and Uploader
==========================================

This script defines the OpenFoodFactsScraper class, which automates:
- Scraping food product data from the OpenFoodFacts API (country-specific).
- Classifying barcodes into EAN-8 or EAN-13 categories.
- Filtering, deduplicating, validating, and uploading product data to an AWS S3 bucket.
- Handling scraping interruptions via checkpointing and auto-resume.
- Managing API throttling (HTTP 429) and connection errors gracefully.

Requirements:
-------------
- AWS credentials and S3 bucket configured (.env + S3Manager).
- scrape_food function available for detailed product fetching.

Usage:
------
Run the script directly to scrape and upload a target number of products:
    python openfoodfacts_scraper_main.py

Author:
    [Your Name or Organization]

Date:
    [YYYY-MM-DD]
"""

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import json
import time
import requests
from dotenv import load_dotenv
from openfoodfacts_scraper import scrape_food
from Infrastructure.aws.s3.s3_manager import S3Manager
import tempfile
import shutil

API_URL = "https://world.openfoodfacts.org/cgi/search.pl"
API_URL_FR = "https://fr.openfoodfacts.org/cgi/search.pl"
RESULTS_DIR = tempfile.mkdtemp(prefix="openfoodfacts_tmp_")
CHECKPOINT_FILE = os.path.join(RESULTS_DIR, "checkpoint.json")


class OpenFoodFactsScraper:
    """
    Class to manage scraping of OpenFoodFacts products and uploading to AWS S3,
    with barcode classification (EAN-8 vs EAN-13), filtering, checkpointing and deduplication.
    """

    def __init__(self, target_total=300, page_size=50, country: str = "france"):
        """
        Initialize the OpenFoodFactsScraper.

        Args:
            target_total (int): Number of products to scrape (default=300).
            page_size (int): Number of products to fetch per API page (default=50).
            country (str): Country tag used to filter products (default="france").

        Loads .env configuration, initializes S3 connection, loads previous checkpoint if it exists,
        and fetches already uploaded barcodes to avoid duplicates.
        """
        self.country = country
        load_dotenv()
        self.s3_manager = S3Manager()
        self.target_total = target_total
        self.page_size = page_size
        self.current_page = 1
        self.barcodes = set()
        self.start_product_index = 0
        self.classified_barcodes = {"EAN_8": [], "EAN_13": []}
        self.already_uploaded = set()
        self.load_checkpoint()
        self.fetch_existing_barcodes_from_s3()

    def get_barcodes_from_api(self, page: int = 1, page_size: int = 20) -> list:
        """
        Fetch a list of product barcodes from OpenFoodFacts API, sorted by scan popularity.

        Handles HTTP 429 throttling by retrying after a delay.

        Args:
            page (int): Page number to request (default=1).
            page_size (int): Number of products to fetch per page (default=20).

        Returns:
            list: A list of barcode strings.
        """
        print(f"\n Requête vers l’API OFF (page={page}, page_size={page_size})")
        params = {
            "search_simple": 1,
            "action": "process",
            "json": 1,
            "page": page,
            "page_size": page_size,
            "sort_by": "unique_scans_n",
            "tagtype_0": "countries",
            "tag_contains_0": "contains",
            "tag_0": self.country
        }
        try:
            headers = {"User-Agent": "Mozilla/5.0 (HabitScraper/1.0)"}
            response = requests.get(API_URL, params=params, headers=headers)
            print(f" Status HTTP : {response.status_code}")

            if response.status_code == 429:
                print("⏳ Trop de requêtes. Pause de 60 secondes…")
                time.sleep(60)
                return self.get_barcodes_from_api(page, page_size)

            response.raise_for_status()
            data = response.json()
        except Exception as e:
            print(f"❌ Erreur lors de la requête : {e}")
            return []

        barcodes = [p.get("code") for p in data.get("products", []) if p.get("code") and p.get("product_name")]
        for code in barcodes:
            print(f" Code-barres trouvé : {code}")
        return barcodes

    def classify_barcodes(self, barcodes: list) -> dict:
        """
        Classify barcodes into two groups based on their length: EAN-8 (8 digits) and EAN-13 (13 digits).

        Args:
            barcodes (list): List of barcode strings.

        Returns:
            dict: {"EAN_8": [...], "EAN_13": [...]}
        """
        return {
            "EAN_8": [code for code in barcodes if len(code) == 8],
            "EAN_13": [code for code in barcodes if len(code) == 13]
        }

    def fetch_existing_barcodes_from_s3(self):
        """
        Fetch all barcodes already uploaded to S3.

        This prevents re-uploading already existing products.
        Saves all known barcodes into a local set (self.already_uploaded).
        """
        print(" Lecture initiale des fichiers présents sur S3…")
        for folder in ["EAN8", "EAN13"]:
            prefix = f"openfoodfacts/{folder}/"
            files = self.s3_manager.list(prefix=prefix)
            for f in files:
                filename = os.path.basename(f)
                if filename.endswith(".json"):
                    self.already_uploaded.add(filename.replace(".json", ""))
        print(f"✅ {len(self.already_uploaded)} produits déjà présents sur S3.")

    def scrape_and_upload_foods(self, barcodes: list):
        """
        Scrape detailed product information for each barcode and upload valid products to S3.

        Steps for each barcode:
        - Check if barcode already exists in S3.
        - Scrape product data using scrape_food.
        - If valid, upload the product JSON to the appropriate S3 folder (EAN8 or EAN13).

        Args:
            barcodes (list): List of barcodes to scrape and upload.
        """
        for i, barcode in enumerate(barcodes[self.start_product_index:], start=self.start_product_index + 1):
            print(f"\n Scraping produit {i}/{len(barcodes)} – {barcode}")

            if barcode in self.already_uploaded:
                print(f" Déjà présent sur S3, skip : {barcode}")
                continue

            try:
                data = scrape_food(barcode, verbose=True)
            except Exception as e:
                print(f"[ERROR] Scraping failed: {e}")
                time.sleep(30)
                continue

            if not data.get("is_valid", False):
                print(f"❌ Produit invalide : {barcode}")
                continue

            local_path = os.path.join(RESULTS_DIR, f"{barcode}.json")
            with open(local_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            folder = "EAN8" if len(barcode) == 8 else "EAN13"
            s3_key = f"openfoodfacts/{folder}/{barcode}.json"
            self.s3_manager.upload(local_path, s3_key)
            self.already_uploaded.add(barcode)  # Ajout à la mémoire en temps réel

            time.sleep(2)

    def load_checkpoint(self):
        """
        Load scraping checkpoint data if it exists (page, barcodes, product index).

        Allows scraping to resume automatically after interruption.
        """
        if os.path.exists(CHECKPOINT_FILE):
            with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                self.current_page = data["current_page"]
                self.barcodes = set(data["barcodes"])
                self.start_product_index = data.get("start_product_index", 0)

    def save_checkpoint(self):
        """
        Save current scraping state to a checkpoint file.

        Stored data:
        - Current API page
        - Collected barcodes
        - Current starting index
        """
        checkpoint = {
            "current_page": self.current_page,
            "barcodes": list(self.barcodes),
            "start_product_index": self.start_product_index
        }
        os.makedirs(RESULTS_DIR, exist_ok=True)
        with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
            json.dump(checkpoint, f, ensure_ascii=False, indent=2)

    def run(self):
        """
        Main scraping execution flow.

        - Repeatedly fetches barcodes from API until target_total is reached.
        - Classifies barcodes into EAN-8 and EAN-13.
        - Scrapes detailed product information and uploads valid products to S3.
        - Saves checkpoints during scraping for recovery.
        - Cleans up temporary directories after completion.
        """
        print(f" Objectif : scraper {self.target_total} codes-barres depuis OpenFoodFacts…")

        try:
            while len(self.barcodes) < self.target_total:
                barcodes = self.get_barcodes_from_api(page=self.current_page, page_size=self.page_size)
                if not barcodes:
                    print("❌ Fin des résultats détectée.")
                    break

                initial_count = len(self.barcodes)
                self.barcodes.update(barcodes)
                added = len(self.barcodes) - initial_count

                print(f" Codes extraits : {added} (total cumulé : {len(self.barcodes)})")
                self.current_page += 1
                self.save_checkpoint()
                time.sleep(2)

            barcodes_list = list(self.barcodes)[:self.target_total]
            self.classified_barcodes = self.classify_barcodes(barcodes_list)

            print("\n Répartition des codes-barres :")
            print(f" - EAN-8  : {len(self.classified_barcodes['EAN_8'])}")
            print(f" - EAN-13 : {len(self.classified_barcodes['EAN_13'])}")

            print(f"\n Lancement du scraping et upload des produits…")
            self.scrape_and_upload_foods(barcodes_list)

        finally:
            if os.path.exists(RESULTS_DIR):
                shutil.rmtree(RESULTS_DIR)
                print(f" Dossier temporaire supprimé : {RESULTS_DIR}")


if __name__ == "__main__":
    scraper = OpenFoodFactsScraper(target_total=5000, page_size=50)
    scraper.run()

# Codes extraits : 49 (total cumulé : 239)
# Requête vers l’API OFF (page=5, page_size=50)

# 6111018501480
# 3504182960123