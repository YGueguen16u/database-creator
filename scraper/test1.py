import os
import requests
from bs4 import BeautifulSoup

def scrape_food(barcode: str, verbose: bool = False):
    try:
        url = f"https://world.openfoodfacts.org/product/{barcode}"
        response = requests.get(url)

        # ⚫⃝ Créer le dossier debug si nécessaire
        os.makedirs("debug_html", exist_ok=True)

        # 🔍 Dump HTML si verbose activé dès le début
        if verbose:
            with open(f"debug_html/debug_{barcode}.html", "w", encoding="utf-8") as f:
                f.write(response.text)

        soup = BeautifulSoup(response.content, "html.parser")

        name_tag = soup.find("h1", {"property": "food:name"})
        name = name_tag.text.strip() if name_tag else "Unknown"

        barcode_span = soup.find("span", id="barcode")
        extracted_barcode = barcode_span.text.strip() if barcode_span else barcode

        brand_span = soup.find("span", id="field_brands_value")
        brands = brand_span.text.strip() if brand_span else None

        categories_span = soup.find("span", id="field_categories_value")
        categories = categories_span.text.strip() if categories_span else None

        countries_span = soup.find("span", id="field_countries_value")
        countries = countries_span.text.strip() if countries_span else None

        nutri_score_tag = soup.find("h4", class_="grade_e_title")
        nutri_score = nutri_score_tag.text.strip() if nutri_score_tag else None

        serving_size_strong = soup.find("strong", string=lambda t: t and "Serving size" in t)
        serving_size = serving_size_strong.next_sibling.strip() if serving_size_strong else None

        allergens_strong = soup.find("strong", string=lambda t: t and "Allergens" in t)
        allergens = allergens_strong.next_sibling.strip() if allergens_strong else None

        ingredients_panel = soup.find("div", class_="panel_text", string=lambda t: t and "French:" in t)
        ingredients_text = ingredients_panel.text.replace("French:", "").strip() if ingredients_panel else None

        ingredients_structured = []
        ingredient_blocks = soup.select("#panel_ingredients_list .accordion-navigation")

        for block in ingredient_blocks:
            h4 = block.find("h4")
            if h4:
                text = h4.get_text(strip=True)
                if ":" in text:
                    ing_name, percentage = text.split(":", 1)
                    ingredients_structured.append({
                        "ingredient": ing_name.strip("\u2013\u2014 ").strip(),
                        "percentage": percentage.strip()
                    })

        quantity_span = soup.find("span", id="field_quantity_value")
        quantity = quantity_span.text.strip() if quantity_span else None

        packaging_span = soup.find("span", id="field_packaging_value")
        packaging = packaging_span.text.strip() if packaging_span else None

        labels_span = soup.find("span", id="field_labels_value")
        labels = labels_span.text.strip() if labels_span else None

        origin_span = soup.find("span", id="field_origin_value")
        origin = origin_span.text.strip() if origin_span else None

        manufacturing_span = soup.find("span", id="field_manufacturing_places_value")
        manufacturing_places = manufacturing_span.text.strip() if manufacturing_span else None

        stores_span = soup.find("span", id="field_stores_value")
        stores = stores_span.text.strip() if stores_span else None

        green_score_title = soup.find("h4", class_="grade_d_title")
        green_score = green_score_title.text.strip() if green_score_title else None

        score_panel = soup.find("div", id="panel_environment_score_total_content")
        final_green_score = None
        if score_panel:
            score_texts = score_panel.get_text()
            if "Final score:" in score_texts:
                final_green_score = score_texts.split("Final score:")[1].split("\n")[0].strip()

        carbon_footprint = None
        carbon_tag = soup.find("h4", class_="evaluation_bad_title")
        if carbon_tag and "Equal to driving" in carbon_tag.text:
            carbon_footprint = carbon_tag.find_next("span").text.strip()

        carbon_score_100g = carbon_tag.find_next("span").text.strip() if carbon_tag else None

        conservation_span = soup.find("span", id="field_conservation_conditions_value")
        conservation_conditions = conservation_span.text.strip() if conservation_span else None

        customer_service_span = soup.find("span", id="field_customer_service_value")
        customer_service = customer_service_span.text.strip() if customer_service_span else None

        nutrition_table = soup.find("table", {"aria-label": "Nutrition facts"})
        nutrition_data = {}
        if nutrition_table:
            rows = nutrition_table.find_all("tr")
            for row in rows:
                cols = row.find_all("td")
                if len(cols) >= 2:
                    nutrient_name = cols[0].get_text(strip=True)
                    value_100g = cols[1].get_text(strip=True)
                    nutrition_data[nutrient_name] = value_100g

        result = {
            "barcode": extracted_barcode,
            "name": name,
            "brands": brands,
            "categories": categories,
            "countries_sold": countries,
            "nutri_score": nutri_score,
            "green_score_letter": green_score,
            "green_score_final": final_green_score,
            "carbon_impact_per_100g": carbon_score_100g,
            "carbon_equiv_distance": carbon_footprint,
            "serving_size": serving_size,
            "quantity": quantity,
            "packaging": packaging,
            "labels": labels,
            "origin": origin,
            "manufacturing_places": manufacturing_places,
            "stores": stores,
            "allergens": allergens,
            "ingredients_text": ingredients_text,
            "ingredients_structured": ingredients_structured,
            "conservation_conditions": conservation_conditions,
            "customer_service": customer_service,
            "nutrients_100g": nutrition_data,
            "is_valid": name != "Unknown" and bool(nutrition_data)
        }

        # Dump automatique si invalide (même sans verbose)
        if not result["is_valid"]:
            with open(f"debug_html/error_{barcode}.html", "w", encoding="utf-8") as f:
                f.write(response.text)

        return result

    except Exception as e:
        if verbose:
            print(f"[ERROR] Scraping failed: {e}")
        return {"barcode": barcode, "error": str(e), "is_valid": False}

# Exemple d'utilisation
if __name__ == "__main__":
    from pprint import pprint
    pprint(scrape_food("3017620425035", verbose=True))