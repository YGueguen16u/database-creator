import json
import os
import sys
import re
from typing import List

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from Infrastructure.aws.s3.s3_manager import S3Manager


class ProductTransformer:
    def __init__(self, s3_manager: S3Manager):
        self.s3_manager = s3_manager
        self.source_bucket = s3_manager.bucket
        self.dest_bucket = s3_manager.bucket  # same bucket but different prefix

    def _standardize_text(self, text: str) -> str:
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
        if not name:
            return name
        parts = name.split("–")
        if len(parts) >= 2:
            return parts[0].strip()
        return name.strip()

    def _extract_score_letter(self, value: str) -> str:
        if not value:
            return None
        value = self._standardize_text(value)
        match = re.search(r"[abcde]", value)
        if match:
            return match.group(0)
        return None

    def _transform_carbon_impact(self, value: str):
        if not value:
            return None
        try:
            float_value = float(value)
            return float_value
        except ValueError:
            return None

    def _split_quantity_and_unit(self, value: str):
        """Split a text like '450g' or '1 serving (20 g)' into quantity + unit."""
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
        if not value:
            return None
        quantity, unit = self._split_quantity_and_unit(value)
        if quantity is not None and unit:
            return {"quantity": quantity, "unit": unit}
        return None

    def list_all_json_keys(self) -> List[str]:
        keys = []
        for folder in ["EAN8", "EAN13"]:
            prefix = f"openfoodfacts/{folder}/"
            keys.extend([k for k in self.s3_manager.list(prefix=prefix) if k.endswith(".json")])
        return keys

    def transform_and_save_all(self):
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