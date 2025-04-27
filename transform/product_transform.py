"""
OpenFoodFacts Product Transformer
==================================

This module defines the ProductTransformer class, which automates the transformation
of raw OpenFoodFacts products into a cleaner, structured format ready for downstream processing.

Features:
---------
- Cleans and standardizes product names.
- Extracts and validates nutri-scores and green-scores (letters a-e).
- Splits "energy" into "energy_kj" and "energy_kcal".
- Parses all nutrients, serving size, and quantity into separate (quantity, unit) structures.
- Removes invalid or unparsable fields.
- Uploads transformed products to AWS S3 under a new structured prefix ("openfoodfactstransformed/").

Requirements:
-------------
- AWS credentials and an accessible S3 bucket.
- Environment variables configured via a .env file.
- S3Manager implemented separately.

Usage:
------
Run directly to transform all OpenFoodFacts raw products into the transformed format:
    python transform_openfoodfacts_products.py

Author:
    [Your Name or Organization]

Date:
    [YYYY-MM-DD]
"""

import json
import os
import sys
import re
from typing import List

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from Infrastructure.aws.s3.s3_manager import S3Manager


class ProductTransformer:
    def __init__(self, s3_manager: S3Manager):
        """
        Initialize the ProductTransformer.

        Args:
            s3_manager (S3Manager): Instance of S3Manager to handle S3 operations.
        """
        self.s3_manager = s3_manager
        self.source_bucket = s3_manager.bucket
        self.dest_bucket = s3_manager.bucket  # same bucket but different prefix

    def _standardize_text(self, text: str) -> str:
        """
        Standardize a text string by:
        - Lowercasing
        - Stripping leading/trailing spaces
        - Replacing special characters (non-breaking spaces, smart quotes, en-dashes).

        Args:
            text (str): Input text.

        Returns:
            str: Standardized text.
        """
        if not text:
            return ""
        return (
            str(text)
            .strip()
            .replace("\u00a0", " ")
            .replace("\u2019", "'")
            .replace("\u2013", "-")
            .lower()
        )

    def _transform_name(self, name: str) -> str:
        """
        Simplify the product name by removing additional segments after the first '–' (en dash).

        Args:
            name (str): Raw product name.

        Returns:
            str: Cleaned product name.
        """
        if not name:
            return name
        parts = name.split("–")
        if len(parts) >= 2:
            return parts[0].strip()
        return name.strip()

    def _extract_score_letter(self, value: str) -> str:
        """
        Extract a single Nutri-score or Green-score letter (a, b, c, d, e) from a text.

        Args:
            value (str): Raw score text.

        Returns:
            str or None: The score letter if found, else None.
        """
        if not value:
            return None
        value = self._standardize_text(value)
        match = re.search(r"[abcde]", value)
        if match:
            return match.group(0)
        return None

    def _transform_carbon_impact(self, value: str):
        """
        Convert a carbon impact string into a float.

        Args:
            value (str): Raw carbon impact value.

        Returns:
            float or None: Numeric carbon impact or None if invalid.
        """
        if not value:
            return None
        try:
            float_value = float(value)
            return float_value
        except ValueError:
            return None

    def _split_quantity_and_unit(self, value: str):
        """
        Split a string like "450g" or "1 serving (20g)" into a numeric quantity and unit.

        Args:
            value (str): Raw value.

        Returns:
            tuple: (quantity as float, unit as str) or (None, None) if parsing fails.
        """
        if not value:
            return None, None
        value = value.replace(",", ".")
        match = re.match(r"([\d\.]+)\s*([a-zA-Zµ]+)", value)
        if match:
            quantity = float(match.group(1))
            unit = match.group(2).lower()
            return quantity, unit
        return None, None

    def _transform_nutrients(self, nutrients: dict) -> dict:
        """
        Transform the nutrients dictionary:
        - Normalize keys.
        - For "energy", split into "energy_kj_100g" and "energy_kcal_100g".
        - For other nutrients, parse into {quantity, unit} if possible.

        Args:
            nutrients (dict): Raw nutrients dictionary.

        Returns:
            dict: Transformed nutrients with structured values.
        """
        new_nutrients = {}

        for key, value in nutrients.items():
            if not value or value == "?":
                continue

            key_standardized = self._standardize_text(key)

            if key_standardized == "energy":
                # Special case for Energy (split kj and kcal)
                kj_match = re.search(r"([\d\.,]+)\s*kj", value, re.IGNORECASE)
                kcal_match = re.search(r"([\d\.,]+)\s*kcal", value, re.IGNORECASE)
                if kj_match:
                    kj_value = kj_match.group(1).replace(",", ".")
                    new_nutrients["energy_kj_100g"] = {"quantity": float(kj_value), "unit": "kj"}
                if kcal_match:
                    kcal_value = kcal_match.group(1).replace(",", ".")
                    new_nutrients["energy_kcal_100g"] = {"quantity": float(kcal_value), "unit": "kcal"}
                continue

            if isinstance(value, str):
                quantity, unit = self._split_quantity_and_unit(value)
                if quantity is not None and unit:
                    new_nutrients[self._standardize_text(key)] = {"quantity": quantity, "unit": unit}
                # If parsing fails (no number or unit), nutrient is skipped

        return new_nutrients

    def _transform_serving_size_or_quantity(self, value: str):
        """
        Transform a 'serving_size' or 'quantity' field:
        - Parse into {quantity, unit} format if possible.

        Args:
            value (str): Raw value.

        Returns:
            dict or None: Structured data if successful, None otherwise.
        """
        if not value:
            return None
        quantity, unit = self._split_quantity_and_unit(value)
        if quantity is not None and unit:
            return {"quantity": quantity, "unit": unit}
        return None

    def list_all_json_keys(self) -> List[str]:
        """
        List all JSON files (keys) in the openfoodfacts folders (EAN8 and EAN13) from S3.

        Returns:
            List[str]: List of S3 keys pointing to raw product JSON files.
        """
        keys = []
        for folder in ["EAN8", "EAN13"]:
            prefix = f"openfoodfacts/{folder}/"
            keys.extend([k for k in self.s3_manager.list(prefix=prefix) if k.endswith(".json")])
        return keys

    def transform_and_save_all(self):
        """
        Transform all OpenFoodFacts product files:
        - Load each file.
        - Apply transformation.
        - Upload the cleaned version to S3 under the "openfoodfactstransformed/" prefix.
        """
        keys = self.list_all_json_keys()
        for key in keys:
            try:
                obj = self.s3_manager.s3.get_object(Bucket=self.source_bucket, Key=key)
                product = json.loads(obj['Body'].read())
                transformed = self._transform_product(product)

                dest_key = key.replace("openfoodfacts/", "openfoodfactstransformed/")
                self.s3_manager.upload_json(self.dest_bucket, dest_key, transformed)
                print(f"✅ Transformed and uploaded: {dest_key}")
            except Exception as e:
                print(f"❌ Failed to transform {key}: {e}")

    def _transform_product(self, product: dict) -> dict:
        """
        Apply all transformation steps to a single product dictionary:
        - Clean name
        - Validate nutri/green scores
        - Parse carbon impact
        - Structure nutrients, serving size, and quantity fields.

        Args:
            product (dict): Raw product dictionary.

        Returns:
            dict: Transformed product dictionary.
        """
        product = product.copy()

        if "name" in product:
            product["name"] = self._transform_name(product["name"])

        if "nutri_score" in product:
            product["nutri_score"] = self._extract_score_letter(product["nutri_score"])

        if "green_score_letter" in product:
            product["green_score_letter"] = self._extract_score_letter(product["green_score_letter"])

        if "carbon_impact_per_100g" in product:
            product["carbon_impact_per_100g"] = self._transform_carbon_impact(product["carbon_impact_per_100g"])

        if "nutrients_100g" in product and isinstance(product["nutrients_100g"], dict):
            product["nutrients_100g"] = self._transform_nutrients(product["nutrients_100g"])

        if "serving_size" in product:
            transformed = self._transform_serving_size_or_quantity(product["serving_size"])
            if transformed:
                product["serving_size"] = transformed
            else:
                product["serving_size"] = None

        if "quantity" in product:
            transformed = self._transform_serving_size_or_quantity(product["quantity"])
            if transformed:
                product["quantity"] = transformed
            else:
                product["quantity"] = None

        return product


if __name__ == "__main__":
    s3 = S3Manager()
    transformer = ProductTransformer(s3_manager=s3)
    transformer.transform_and_save_all()