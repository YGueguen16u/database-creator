import json
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from typing import List, Dict
from Infrastructure.aws.s3.s3_manager import S3Manager


class ProductAnalyzer:
    """
    A class to analyze the basic quality of OpenFoodFacts product files stored in an S3 bucket.

    It verifies for each product:
    - Name presence
    - Categories presence
    - Nutri-score validity
    - Green-score validity
    - Presence of main nutrients (Energy, Proteins, Fat, Carbohydrates)

    Attributes:
        s3_manager (S3Manager): An instance of the S3Manager to interact with AWS S3.
    """

    def __init__(self, s3_manager: S3Manager, reference_path: str):
        self.s3_manager = s3_manager
        self.annotated_nutrients = self._load_reference(reference_path)
        self.reference_nutrients = set(n["name"] for n in self.annotated_nutrients)
        self.required_main_nutrients = {"energy", "fat", "proteins", "carbohydrates"}

    def _load_reference(self, path: str) -> List[Dict[str, str]]:
        """Load and standardize nutrient names."""
        with open(path, "r", encoding="utf-8") as f:
            nutrients = json.load(f)
        for nutrient in nutrients:
            nutrient["name"] = self._standardize_text(nutrient["name"])
        return nutrients

    def _standardize_text(self, text: str) -> str:
        """Standardizes a string: strip, lowercase, fix special characters."""
        if not text:
            return ""
        return (
            text.strip()
            .replace("‚", ",")
            .replace("’", "'")
            .replace("–", "-")
            .replace("\u00a0", " ")
            .replace("\u2019", "'")
            .replace("\u2013", "-")
            .lower()
        )

    def count_products_per_folder(self, keys: List[str]) -> Dict[str, int]:
        """Count how many products are found in each main folder (EAN8, EAN13, etc.)."""
        folder_counts = {}
        for key in keys:
            parts = key.split("/")
            if len(parts) > 1:
                folder = parts[1]  # ex: 'EAN8' ou 'EAN13'
                folder_counts[folder] = folder_counts.get(folder, 0) + 1
        return folder_counts

    def list_all_json_keys(self) -> List[str]:
        """List all JSON keys from EAN8 and EAN13 folders in S3."""
        keys = []
        for folder in ["EAN8", "EAN13"]:
            prefix = f"openfoodfacts/{folder}/"
            keys.extend([k for k in self.s3_manager.list(prefix=prefix) if k.endswith(".json")])
        return keys

    def analyze_product(self, product: dict) -> Dict[str, bool]:
        """Analyze a single product and return the results of all checks."""
        return {
            "name_present": self._check_name_present(product),
            "categories_present": self._check_categories_present(product),
            "nutri_score_valid": self._check_nutri_score_valid(product),
            "green_score_valid": self._check_green_score_valid(product),
            "main_nutrients_present": self._check_main_nutrients_present(product),
        }

    def _check_name_present(self, product: dict) -> bool:
        return bool(product.get("name"))

    def _check_categories_present(self, product: dict) -> bool:
        categories = product.get("categories", "")
        return bool(categories and categories.strip())

    def _check_nutri_score_valid(self, product: dict) -> bool:
        valid_scores = {"a", "b", "c", "d", "e"}
        nutri_score = product.get("nutri_score")
        if nutri_score is None:
            return True
        return self._standardize_text(nutri_score) in valid_scores

    def _check_green_score_valid(self, product: dict) -> bool:
        green_score = product.get("green_score_letter")
        if green_score is None:
            return True
        standardized = self._standardize_text(green_score)
        return standardized.startswith("green-score ") and standardized[-1] in {"a", "b", "c", "d", "e"}

    def _check_main_nutrients_present(self, product: dict) -> bool:
        nutrients = product.get("nutrients_100g", {})
        available_nutrients = set(self._standardize_text(k) for k in nutrients.keys())
        valid_available = {nutrient for nutrient in available_nutrients if nutrient in self.reference_nutrients}
        return self.required_main_nutrients.issubset(valid_available)

    def _get_error_detail(self, field: str, product: dict) -> Dict[str, str]:
        """Return a detailed error object with explanation, value found, and field category."""
        if field == "name_present":
            return {
                "error": "Missing or empty 'name' field.",
                "value_found": product.get("name", None),
                "field_name": "name"
            }
        if field == "categories_present":
            return {
                "error": "Missing or empty 'categories' field.",
                "value_found": product.get("categories", None),
                "field_name": "categories"
            }
        if field == "nutri_score_valid":
            return {
                "error": "Invalid nutri-score value.",
                "value_found": product.get("nutri_score", None),
                "field_name": "nutri_score"
            }
        if field == "green_score_valid":
            return {
                "error": "Invalid green-score format.",
                "value_found": product.get("green_score_letter", None),
                "field_name": "green_score_letter"
            }
        if field == "main_nutrients_present":
            nutrients = product.get("nutrients_100g", {})
            available_nutrients = set(self._standardize_text(k) for k in nutrients.keys())
            valid_available = {nutrient for nutrient in available_nutrients if nutrient in self.reference_nutrients}
            missing_main = self.required_main_nutrients - valid_available
            return {
                "error": f"Missing required nutrients: {', '.join(sorted(missing_main))}",
                "value_found": list(nutrients.keys()),
                "field_name": "nutrients_100g"
            }
        return {
            "error": "Unknown error.",
            "value_found": None,
            "field_name": "unknown"
        }

    def analyze_all_products(self) -> Dict[str, Dict[str, bool]]:
        """
        Analyze all products in the S3 bucket and save results in different files.

        Returns:
            Dict[str, Dict[str, bool]]: Full analysis results.
        """
        keys = self.list_all_json_keys()
        folder_counts = self.count_products_per_folder(keys)
        results = {}
        errors = {}
        valids = {}
        failed_files = []

        for key in keys:
            try:
                obj = self.s3_manager.s3.get_object(Bucket=self.s3_manager.bucket, Key=key)
                content = json.loads(obj['Body'].read())
                checks = self.analyze_product(content)
                results[key] = checks

                if all(checks.values()):
                    valids[key] = checks
                else:
                    detailed_checks = {}
                    for field, value in checks.items():
                        if value:
                            detailed_checks[field] = True
                        else:
                            detailed_checks[field] = self._get_error_detail(field, content)
                    errors[key] = detailed_checks

            except Exception as e:
                print(f"❌ Failed to analyze {key}: {e}")
                failed_files.append(key)  # 🛠 Ajout dans la liste

        self._save_results(valids, errors, results, failed_files, folder_counts)
        print(f"\n Product analysis complete. {len(results)} products analyzed, {len(failed_files)} failed.")
        return results

    def _get_next_versioned_filename(self, path: str) -> str:
        """If file exists, increment version number with 4-digit padding."""
        base, ext = os.path.splitext(path)
        counter = 0
        new_path = f"{base}_{counter:04d}{ext}"
        while os.path.exists(new_path):
            counter += 1
            new_path = f"{base}_{counter:04d}{ext}"
        return new_path

    def _save_results(self, valids: Dict[str, dict], errors: Dict[str, dict], all_results: Dict[str, dict],
                      failed_files: List[str], folder_counts: Dict[str, int]):
        """Save analysis results into separate JSON files with versioning, organized by folder."""
        base_path = os.path.dirname(__file__)
        output_base_dir = os.path.join(base_path, "log", "product_analyzer_raw")

        # Define subfolders
        folders = {
            "valid": os.path.join(output_base_dir, "products_all_valid"),
            "errors": os.path.join(output_base_dir, "products_with_errors"),
            "summary": os.path.join(output_base_dir, "products_summary"),
            "failed": os.path.join(output_base_dir, "products_failed_to_analyze")
        }

        # Create all subfolders if they don't exist
        for folder in folders.values():
            os.makedirs(folder, exist_ok=True)

        # Save valid products
        valid_path = os.path.join(folders["valid"], "products_all_valid.json")
        valid_path = self._get_next_versioned_filename(valid_path)
        with open(valid_path, "w", encoding="utf-8") as f:
            json.dump(valids, f, indent=2, ensure_ascii=False)

        # Save products with errors
        errors_path = os.path.join(folders["errors"], "products_with_errors.json")
        errors_path = self._get_next_versioned_filename(errors_path)
        with open(errors_path, "w", encoding="utf-8") as f:
            json.dump(errors, f, indent=2, ensure_ascii=False)

        # Save summary report
        summary_path = os.path.join(folders["summary"], "products_summary.json")
        summary_path = self._get_next_versioned_filename(summary_path)
        summary = {
            "total_products_analyzed": len(all_results),
            "products_all_valid": len(valids),
            "products_with_errors": len(errors),
            "products_failed_to_analyze": len(failed_files),
            "products_per_folder": folder_counts
        }
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)

        # Save failed files separately (only if needed)
        if failed_files:
            failed_path = os.path.join(folders["failed"], "products_failed_to_analyze.json")
            failed_path = self._get_next_versioned_filename(failed_path)
            with open(failed_path, "w", encoding="utf-8") as f:
                json.dump(failed_files, f, indent=2, ensure_ascii=False)

        # 🖨️ Summary of saved files
        print(f"\n✅ Results saved:")
        print(f" - {valid_path}")
        print(f" - {errors_path}")
        print(f" - {summary_path}")
        if failed_files:
            print(f" - {failed_path}")

if __name__ == "__main__":
    s3 = S3Manager()
    analyzer = ProductAnalyzer(s3_manager=s3, reference_path="analyze/nutrients_reference_annotated.json")
    analyzer.analyze_all_products()