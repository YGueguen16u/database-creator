"""
OpenFoodFacts Bulk Scraper and Uploader

This script defines the OpenFoodFactsScraper class, which automates the scraping of food product data
from the OpenFoodFacts API and uploads validated entries to an AWS S3 bucket.

Features:
- Fetches barcodes by popularity from OpenFoodFacts API (country-specific).
- Filters, classifies (EAN-8 vs EAN-13), and deduplicates products.
- Scrapes detailed product information using the scrape_food function.
- Uploads product JSON files to S3 under structured folders ("EAN8", "EAN13").
- Implements checkpointing for progress recovery in case of interruption.
- Handles API throttling (HTTP 429), request failures, and invalid products.
- Automatically cleans up local temporary files at the end.

Requirements:
- AWS credentials and an accessible S3 bucket.
- Environment variables configured via a .env file.
- S3Manager and scrape_food utilities implemented separately.

Usage:
    Run the script directly to scrape a specified number of products and upload them to S3.
    Example:
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
        """Fetch barcodes from OpenFoodFacts API sorted by most scanned."""
        print(f"\n🔗 Requête vers l’API OFF (page={page}, page_size={page_size})")
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
            print(f"📅 Status HTTP : {response.status_code}")

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
            print(f"🔢 Code-barres trouvé : {code}")
        return barcodes

    def classify_barcodes(self, barcodes: list) -> dict:
        """Separate barcodes into EAN-8 and EAN-13 groups."""
        return {
            "EAN_8": [code for code in barcodes if len(code) == 8],
            "EAN_13": [code for code in barcodes if len(code) == 13]
        }

    def fetch_existing_barcodes_from_s3(self):
        """
        Fetch all existing barcodes already uploaded to S3 (once).
        Store them in a set for fast lookup.
        """
        print("🔍 Lecture initiale des fichiers présents sur S3…")
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
        Scrape product data from OpenFoodFacts for each barcode and upload valid
        entries to AWS S3 under categorized folders (EAN8 / EAN13).
        """
        for i, barcode in enumerate(barcodes[self.start_product_index:], start=self.start_product_index + 1):
            print(f"\n🔎 Scraping produit {i}/{len(barcodes)} – {barcode}")

            if barcode in self.already_uploaded:
                print(f"⚠️ Déjà présent sur S3, skip : {barcode}")
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
        """Load progress checkpoint if available."""
        if os.path.exists(CHECKPOINT_FILE):
            with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                self.current_page = data["current_page"]
                self.barcodes = set(data["barcodes"])
                self.start_product_index = data.get("start_product_index", 0)

    def save_checkpoint(self):
        """Save scraping progress to disk."""
        checkpoint = {
            "current_page": self.current_page,
            "barcodes": list(self.barcodes),
            "start_product_index": self.start_product_index
        }
        os.makedirs(RESULTS_DIR, exist_ok=True)
        with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
            json.dump(checkpoint, f, ensure_ascii=False, indent=2)

    def run(self):
        """Main scraping loop."""
        print(f"🌍 Objectif : scraper {self.target_total} codes-barres depuis OpenFoodFacts…")

        try:
            while len(self.barcodes) < self.target_total:
                barcodes = self.get_barcodes_from_api(page=self.current_page, page_size=self.page_size)
                if not barcodes:
                    print("❌ Fin des résultats détectée.")
                    break

                initial_count = len(self.barcodes)
                self.barcodes.update(barcodes)
                added = len(self.barcodes) - initial_count

                print(f"📦 Codes extraits : {added} (total cumulé : {len(self.barcodes)})")
                self.current_page += 1
                self.save_checkpoint()
                time.sleep(2)

            barcodes_list = list(self.barcodes)[:self.target_total]
            self.classified_barcodes = self.classify_barcodes(barcodes_list)

            print("\n🔢 Répartition des codes-barres :")
            print(f" - EAN-8  : {len(self.classified_barcodes['EAN_8'])}")
            print(f" - EAN-13 : {len(self.classified_barcodes['EAN_13'])}")

            print(f"\n🔧 Lancement du scraping et upload des produits…")
            self.scrape_and_upload_foods(barcodes_list)

        finally:
            if os.path.exists(RESULTS_DIR):
                shutil.rmtree(RESULTS_DIR)
                print(f"🧹 Dossier temporaire supprimé : {RESULTS_DIR}")


if __name__ == "__main__":
    scraper = OpenFoodFactsScraper(target_total=5000, page_size=50)
    scraper.run()

#📦 Codes extraits : 49 (total cumulé : 239)
#🔗 Requête vers l’API OFF (page=5, page_size=50)

# 6111018501480
# 3504182960123