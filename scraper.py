#!/usr/bin/env python3
import json
import time
import argparse
from seleniumbase import Driver
from tqdm import tqdm

# Header mappings from Russian to English
HEADER_MAPPINGS = {
    "#": "number",
    "Муниципальное образование": "municipality",
    "Наименование территориальной зоны": "zone_name",
    "Код территориальной зоны": "zone_code",
    "Назначение и описание территориальной зоны": "zone_description",
    "Дополнительная информация к Территориальной зоне (общие требования)": "additional_info"
}


def fetch_data_with_browser(max_pages):
    """Fetch data from all pages up to max_pages using SeleniumBase browser."""
    all_data = []
    base_url = "https://rgis.mosreg.ru/v3/swagger/geoportal/docs/list"

    print("Opening browser to fetch data...")

    browser = Driver(uc=True, headless=True, disable_csp=True)

    try:
        browser.get("https://rgis.mosreg.ru/v3/#/docs/50")
        try:
            browser.wait_for_element("css selector", ".content-wrapper", timeout=10)
            print("Authentication page loaded successfully")
        except Exception as e:
            print(f"Warning: Timed out waiting for authentication page to load: {str(e)}")

        print(f"Fetching data from {max_pages} pages...")

        for page in tqdm(range(1, max_pages + 1), desc="Fetching pages"):
            url = f"{base_url}?id=50&page={page}&show=100"

            browser.get(url)

            try:
                browser.wait_for_element("tag name", "pre", timeout=10)

                json_text = browser.find_element("tag name", "pre").text
                page_data = json.loads(json_text)
                all_data.extend(page_data)
            except Exception as e:
                print(f"Error fetching page {page}: {str(e)}")

        #  time.sleep(0.5)

        return all_data

    finally:
        # Close the browser
        browser.quit()


def process_data(data):
    """Process the data and add English header mappings."""
    processed_data = []

    for item in data:
        # Create a new entry with both original columns and English headers
        processed_item = {
            # Add English headers
            HEADER_MAPPINGS["#"]: item["columns"][0],
            HEADER_MAPPINGS["Муниципальное образование"]: item["columns"][1],
            HEADER_MAPPINGS["Наименование территориальной зоны"]: item["columns"][2],
            HEADER_MAPPINGS["Код территориальной зоны"]: item["columns"][3],
            HEADER_MAPPINGS["Назначение и описание территориальной зоны"]: item["columns"][4],
            HEADER_MAPPINGS["Дополнительная информация к Территориальной зоне (общие требования)"]: item["columns"][5],

            # Keep original columns for reference
            "original_columns": item["columns"],

            # Save card ID
            "card_id": item["meta"]["card"] if "card" in item["meta"] else None
        }

        processed_data.append(processed_item)

    return processed_data


def main():
    """Main function to run the scraper."""
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Scrape data from rgis.mosreg.ru')
    parser.add_argument('--max-pages', type=int, default=10,
                        help='Maximum number of pages to scrape (default: 10)')
    parser.add_argument('--output', type=str, default='output.json',
                        help='Output JSON file path (default: output.json)')

    args = parser.parse_args()

    # Fetch data from all pages using the browser
    data = fetch_data_with_browser(args.max_pages)

    # Process the data
    processed_data = process_data(data)

    # Save to output file
    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump(processed_data, f, ensure_ascii=False, indent=2)

    print(f"Scraped {len(processed_data)} items and saved to {args.output}")


if __name__ == "__main__":
    main()
