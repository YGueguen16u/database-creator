"""
ErrorValueExtractor
===================

This class analyzes the latest OpenFoodFacts `products_with_errors.json` file generated during data validation.
It focuses on extracting all 'value_found' fields, standardizing them, and counting their occurrences
across all erroneous fields and categories.

Outputs:
--------
- A JSON report summarizing:
    - Each distinct erroneous value (standardized),
    - How many times it occurred,
    - In which fields it was detected.

The output is automatically versioned and saved into:
    log/product_analyzer_raw/error_analysis/error_value_analysis_XXXX.json

Use case:
---------
- Identify the most common problematic values during data extraction.
- Target corrections or improve parsing rules.
- Monitor the evolution of recurring errors across runs.
"""

import json
import os
import sys
from collections import defaultdict, Counter

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class ErrorValueExtractor:
    """
    A class to extract, standardize and count all 'value_found' fields
    from the latest products_with_errors.json, with associated fields/categories.

    Saves:
    - A versioned file in log/product_analyzer_raw/error_analysis/error_value_analysis_XXXX.json
    """

    def __init__(self):
        """
        Initialize the ErrorValueExtractor.

        - Detects the latest version of the 'products_with_errors' JSON file.
        - Prepares the output directory for saving the results.
        """
        base_dir = os.path.dirname(__file__)
        self.errors_folder = os.path.join(base_dir, "log", "product_analyzer_raw", "products_with_errors")
        self.output_folder = os.path.join(base_dir, "log", "product_analyzer_raw", "error_analysis")
        os.makedirs(self.output_folder, exist_ok=True)
        self.errors_path = self._get_latest_errors_file()

    def _standardize_text(self, text: str) -> str:
        """
        Standardize a given text by:
        - Lowercasing it,
        - Stripping leading/trailing whitespaces,
        - Normalizing common special characters (e.g., replacing different dash types, apostrophes, non-breaking spaces).

        Args:
            text (str): Input text to standardize.

        Returns:
            str: The cleaned, standardized version of the text.

        Ensures consistency when counting error values across diverse formatting.
        """
        if not text:
            return ""
        return (
            str(text)
            .strip()
            .replace("‚", ",")
            .replace("’", "'")
            .replace("–", "-")
            .replace("\u00a0", " ")
            .replace("\u2019", "'")
            .replace("\u2013", "-")
            .lower()
        )

    def _get_latest_errors_file(self) -> str:
        """Get the latest versioned products_with_errors_XXXX.json file."""
        if not os.path.exists(self.errors_folder):
            raise FileNotFoundError(f"❌ Errors folder not found: {self.errors_folder}")

        files = [f for f in os.listdir(self.errors_folder) if f.startswith("products_with_errors") and f.endswith(".json")]
        if not files:
            raise FileNotFoundError(f"❌ No products_with_errors files found in {self.errors_folder}")

        files_sorted = sorted(files, key=lambda f: int(f.split("_")[-1].split(".")[0]))
        latest_file = files_sorted[-1]
        latest_path = os.path.join(self.errors_folder, latest_file)
        print(f"Latest error file detected: {latest_file}")
        return latest_path

    def _get_next_versioned_filename(self) -> str:
        """
        Find and return the latest 'products_with_errors_XXXX.json' file based on version number.

        Raises:
            FileNotFoundError: If no error files are found in the expected directory.

        Returns:
            str: The full path to the latest error JSON file.

        Allows to always analyze the most recent validation output automatically.
        """
        base_name = "error_value_analysis"
        ext = ".json"
        counter = 0
        new_path = os.path.join(self.output_folder, f"{base_name}_{counter:04d}{ext}")

        while os.path.exists(new_path):
            counter += 1
            new_path = os.path.join(self.output_folder, f"{base_name}_{counter:04d}{ext}")

        return new_path

    def extract_and_count_values(self):
        """
        Generate the next available filename for the output report (with versioning).

        Returns:
            str: A path for a new output file like 'error_value_analysis_0000.json', 'error_value_analysis_0001.json', etc.

        Ensures no file overwriting by incrementing version numbers if needed.
        """
        with open(self.errors_path, "r", encoding="utf-8") as f:
            errors = json.load(f)

        value_counter = Counter()
        value_fields = defaultdict(set)

        for product_errors in errors.values():
            for field, detail in product_errors.items():
                if isinstance(detail, dict) and "value_found" in detail:
                    value = detail["value_found"]
                    field_name = detail.get("field_name", "unknown")

                    if isinstance(value, list):
                        for item in value:
                            standardized = self._standardize_text(item)
                            if standardized:
                                value_counter[standardized] += 1
                                value_fields[standardized].add(field_name)
                    else:
                        standardized = self._standardize_text(value)
                        if standardized:
                            value_counter[standardized] += 1
                            value_fields[standardized].add(field_name)

        final_result = {}
        for value, count in value_counter.items():
            final_result[value] = {
                "count": count,
                "fields": sorted(list(value_fields[value]))
            }

        output_path = self._get_next_versioned_filename()
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(final_result, f, indent=2, ensure_ascii=False)

        print(f"\n✅ Error value analysis saved to: {output_path}")

if __name__ == "__main__":
    extractor = ErrorValueExtractor()
    extractor.extract_and_count_values()