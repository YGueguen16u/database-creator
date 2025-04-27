"""
NumericFormatAnalyzer
=====================

This class analyzes the formats of numeric fields within OpenFoodFacts transformed product datasets
stored on an S3 bucket, focusing on:

- `nutrients_100g`
- `serving_size`
- `quantity`

The goal is to:

- Collect all raw patterns encountered.
- Extract all numeric values and associated units.
- Identify and record cases where numeric values or units are missing.
- Generate organized JSON reports:
  - Raw formats by field.
  - Extracted numbers.
  - Extracted units.
  - Missing numbers and units, with the associated barcodes and raw text.

Outputs are automatically versioned and saved under structured folders (`raw/`, `numbers/`, `units/`)
inside the local directory `log/product_analyzer_raw/numeric_analysis/`.

Use case:
---------
This module is useful to validate and prepare OpenFoodFacts data before ingestion into a structured database,
detecting inconsistencies and enabling better cleaning/normalization strategies.
"""

import os
import re
import json
import sys
from collections import defaultdict

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from Infrastructure.aws.s3.s3_manager import S3Manager


class NumericFormatAnalyzer:
    """
    Analyze numeric formats for 'nutrients_100g', 'serving_size', and 'quantity'
    across all OpenFoodFacts transformed products (EAN8 and EAN13).
    Save organized JSON reports (raw formats, numbers, units), with missing detections per barcode.
    """

    def __init__(self, s3_manager: S3Manager, reference_path: str, source_bucket: str = "habit-ai-storage"):
        """
        Initialize the NumericFormatAnalyzer.

        Args:
            s3_manager (S3Manager): An initialized S3Manager instance to interact with AWS S3.
            reference_path (str): Local path to the JSON file containing the nutrient reference table.
            source_bucket (str, optional): The S3 bucket name where the OpenFoodFacts data is stored. Defaults to "habit-ai-storage".

        Initializes internal data structures to collect raw patterns, numbers, and units.
        """
        self.s3_manager = s3_manager
        self.source_bucket = source_bucket
        self.reference_path = reference_path
        self.target_folders = ["openfoodfactstransformed/EAN8/", "openfoodfactstransformed/EAN13/"]
        self.keys = []

        self.reference_nutrients = self._load_reference()

        # Data structures
        self.raw_results = defaultdict(lambda: [])
        self.numbers_results = defaultdict(lambda: {"numbers_found": [], "missing": {}})
        self.units_results = defaultdict(lambda: {"units_found": [], "missing": {}})

    def _load_reference(self):
        """
        Load and standardize the nutrient reference table.

        Returns:
            dict: A mapping of standardized nutrient names (lowercased, parentheses removed) to their metadata.

        This ensures that nutrients can be matched reliably even if the original keys have minor variations.
        """
        with open(self.reference_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        standardized = {}
        for item in data:
            name = self._standardize_text(item["name"])
            standardized[name] = item
        return standardized

    def _standardize_text(self, text: str) -> str:
        """
        Standardize a given text by lowercasing, stripping spaces, and removing any parentheses content.

        Args:
            text (str): Input text.

        Returns:
            str: Cleaned and standardized version of the text.

        This is mainly used to normalize nutrient names or units.
        """
        if not text:
            return ""
        text = text.lower().strip()
        text = re.sub(r"\s*\([^)]*\)", "", text)
        return text

    def _find_numbers_and_units(self, text: str):
        """
        Extract numeric values and measurement units from a string.

        Args:
            text (str): The raw input text (e.g., "1,008 kj (241 kcal)").

        Returns:
            Tuple[List[str], List[str]]:
                - List of extracted numeric values (as strings).
                - List of extracted units.

        Handles decimal commas by converting them to dots and supports multiple values in the same string.
        """
        if not text:
            return [], []

        text = text.replace(",", ".")
        numbers = re.findall(r"\d+(?:\.\d+)?", text)
        units = re.findall(r"([a-zA-Zµ]+)", text)

        return numbers, units

    def _analyze_nutrients(self, nutrients: dict, barcode: str):
        """
        Analyze the nutrient values in a product.

        Args:
            nutrients (dict): The "nutrients_100g" dictionary from a product.
            barcode (str): The barcode of the product (for tracking missing values).

        For each nutrient:
        - Save the raw format.
        - Extract and save numbers and units.
        - If missing, record the barcode and the problematic value.
        """
        for nutrient, value in nutrients.items():
            if value:
                standardized = self._standardize_text(nutrient)
                if standardized in self.reference_nutrients:
                    value_str = str(value)

                    # Save raw
                    self.raw_results[standardized].append(value_str)

                    numbers, units = self._find_numbers_and_units(value_str)

                    if numbers:
                        self.numbers_results[standardized]["numbers_found"].extend(numbers)
                    else:
                        self.numbers_results[standardized]["missing"][barcode] = value_str

                    if units:
                        self.units_results[standardized]["units_found"].extend(units)
                    else:
                        self.units_results[standardized]["missing"][barcode] = value_str

    def _analyze_field(self, field_name: str, value: str, barcode: str):
        """
        Analyze a simple numeric field (serving_size or quantity).

        Args:
            field_name (str): Name of the field ("serving_size" or "quantity").
            value (str): The field value to analyze.
            barcode (str): The barcode of the product.

        Same logic as for nutrients: extract numbers and units, track missing data.
        """
        if value:
            value_str = str(value)

            self.raw_results[field_name].append(value_str)

            numbers, units = self._find_numbers_and_units(value_str)

            if numbers:
                self.numbers_results[field_name]["numbers_found"].extend(numbers)
            else:
                self.numbers_results[field_name]["missing"][barcode] = value_str

            if units:
                self.units_results[field_name]["units_found"].extend(units)
            else:
                self.units_results[field_name]["missing"][barcode] = value_str

    def _save_all_reports(self):
        """
        Save all collected analysis results into structured versioned JSON files.

        The reports are separated into:
        - Raw patterns per field.
        - Extracted numbers (with missing cases).
        - Extracted units (with missing cases).

        Reports are saved in the following folder structure:
        - log/product_analyzer_raw/numeric_analysis/raw/
        - log/product_analyzer_raw/numeric_analysis/numbers/
        - log/product_analyzer_raw/numeric_analysis/units/
        """
        # Deduplicate and sort
        for section in self.raw_results:
            self.raw_results[section] = sorted(list(set(self.raw_results[section])))

        for section in self.numbers_results:
            self.numbers_results[section]["numbers_found"] = sorted(list(set(self.numbers_results[section]["numbers_found"])))

        for section in self.units_results:
            self.units_results[section]["units_found"] = sorted(list(set(self.units_results[section]["units_found"])))

        base_path = os.path.dirname(__file__)
        base_dir = os.path.join(base_path, "log", "product_analyzer_raw", "numeric_analysis")
        raw_dir = os.path.join(base_dir, "raw")
        numbers_dir = os.path.join(base_dir, "numbers")
        units_dir = os.path.join(base_dir, "units")

        # Create folders
        for path in [raw_dir, numbers_dir, units_dir]:
            os.makedirs(path, exist_ok=True)

        # Save raw
        raw_path = os.path.join(raw_dir, "numeric_format_raw.json")
        self._save_json(self.raw_results, raw_path)

        # Save numbers
        numbers_main_path = os.path.join(numbers_dir, "numeric_format_numbers.json")
        numbers_missing_path = os.path.join(numbers_dir, "missing_numbers_by_barcode.json")
        self._save_json({k: v["numbers_found"] for k, v in self.numbers_results.items()}, numbers_main_path)
        self._save_json({k: v["missing"] for k, v in self.numbers_results.items()}, numbers_missing_path)

        # Save units
        units_main_path = os.path.join(units_dir, "numeric_format_units.json")
        units_missing_path = os.path.join(units_dir, "missing_units_by_barcode.json")
        self._save_json({k: v["units_found"] for k, v in self.units_results.items()}, units_main_path)
        self._save_json({k: v["missing"] for k, v in self.units_results.items()}, units_missing_path)

        print(f"\n✅ Reports saved at:\n - {raw_dir}\n - {numbers_dir}\n - {units_dir}")

    def _save_json(self, content: dict, path: str):
        """
        Save a dictionary into a JSON file with versioning.

        Args:
            content (dict): The data to save.
            path (str): The base path (before adding versioning).

        If a file with the same name exists, it will increment the version number (_0001, _0002, etc.).
        """
        versioned_path = self._get_next_versioned_filename(path)
        with open(versioned_path, "w", encoding="utf-8") as f:
            json.dump(content, f, indent=2, ensure_ascii=False)

    def _get_next_versioned_filename(self, path: str) -> str:
        """
        Generate the next available versioned filename for a given path.

        Args:
            path (str): The original file path.

        Returns:
            str: A versioned file path (e.g., "numeric_format_raw_0001.json") that doesn't yet exist.
        """
        base, ext = os.path.splitext(path)
        counter = 0
        new_path = f"{base}_{counter:04d}{ext}"
        while os.path.exists(new_path):
            counter += 1
            new_path = f"{base}_{counter:04d}{ext}"
        return new_path

    def analyze_all(self):
        """
        Launch the full analysis across all OpenFoodFacts transformed products.

        - Fetches all EAN8 and EAN13 JSON files from S3.
        - For each file:
          - Analyzes nutrients_100g, serving_size, and quantity fields.
          - Records raw formats, numbers, units, and missing information.

        Finally, all results are saved into structured, versioned JSON reports.
        """
        for folder in self.target_folders:
            self.keys.extend(self.s3_manager.list(prefix=folder))

        print(f"\n🧹 {len(self.keys)} files found to analyze in OpenFoodFacts transformed folders.")

        for key in self.keys:
            try:
                obj = self.s3_manager.s3.get_object(Bucket=self.source_bucket, Key=key)
                product = json.loads(obj['Body'].read())

                barcode = product.get("barcode", "unknown")

                nutrients = product.get("nutrients_100g", {})
                self._analyze_nutrients(nutrients, barcode)

                serving_size = product.get("serving_size", "")
                self._analyze_field("serving_size", serving_size, barcode)

                quantity = product.get("quantity", "")
                self._analyze_field("quantity", quantity, barcode)

            except Exception as e:
                print(f"❌ Failed to analyze {key}: {e}")

        self._save_all_reports()


if __name__ == "__main__":
    s3 = S3Manager()
    analyzer = NumericFormatAnalyzer(
        s3_manager=s3,
        reference_path="analyze/nutrients_reference_annotated.json"
    )
    analyzer.analyze_all()