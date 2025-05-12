#!/usr/bin/env python3
import json
import time
import argparse
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
from seleniumbase import Driver
from tqdm import tqdm

HEADER_MAPPINGS = {
    "#": "number",
    "Муниципальное образование": "municipality",
    "Наименование территориальной зоны": "zone_name",
    "Код территориальной зоны": "zone_code",
    "Назначение и описание территориальной зоны": "zone_description",
    "Дополнительная информация к Территориальной зоне (общие требования)": "additional_info"
}


def fetch_data_with_browser(page_range):
    start_page, end_page = page_range
    all_data = []
    base_url = "https://rgis.mosreg.ru/v3/swagger/geoportal/docs/list"

    print(f"Worker process: Opening browser to fetch pages {start_page} to {end_page}...")

    browser = Driver(uc=True, headless=True, disable_csp=True)

    try:
        browser.get("https://rgis.mosreg.ru/v3/#/docs/50")
        try:
            browser.wait_for_element("id", "data", timeout=10)
            print(f"Worker process: Authentication page loaded successfully for pages {start_page}-{end_page}")
        except Exception as e:
            print(f"Worker process: Warning: Timed out waiting for authentication page to load: {str(e)}")

        # Use tqdm for progress within this worker
        page_range_with_progress = tqdm(
            range(start_page, end_page + 1),
            desc=f"Worker fetching pages {start_page}-{end_page}",
            position=0,
            leave=True
        )

        for page in page_range_with_progress:
            url = f"{base_url}?id=50&page={page}&show=100"
            browser.get(url)

            try:
                browser.wait_for_element("tag name", "pre", timeout=10)
                json_text = browser.find_element("tag name", "pre").text
                page_data = json.loads(json_text)
                all_data.extend(page_data)
            except Exception as e:
                print(f"Worker process: Error fetching page {page}: {str(e)}")

        return all_data

    finally:
        browser.quit()


def process_data(data):
    processed_data = []

    for item in data:
        processed_item = {
            HEADER_MAPPINGS["#"]: item["columns"][0],
            HEADER_MAPPINGS["Муниципальное образование"]: item["columns"][1],
            HEADER_MAPPINGS["Наименование территориальной зоны"]: item["columns"][2],
            HEADER_MAPPINGS["Код территориальной зоны"]: item["columns"][3],
            # HEADER_MAPPINGS["Назначение и описание территориальной зоны"]: item["columns"][4],
            # HEADER_MAPPINGS["Дополнительная информация к Территориальной зоне (общие требования)"]: item["columns"][5],

            # Save geometry ID if available
            "geometry_id": item["meta"]["geometry"] if "geometry" in item["meta"] else None
        }

        if processed_item["geometry_id"]:
            processed_data.append(processed_item)

    return processed_data


def distribute_pages(max_pages, workers):
    workers = min(workers, max_pages)

    pages_per_worker = max_pages // workers
    remainder = max_pages % workers

    page_ranges = []
    start_page = 1

    for i in range(workers):
        worker_pages = pages_per_worker + (1 if i < remainder else 0)
        end_page = start_page + worker_pages - 1

        page_ranges.append((start_page, end_page))
        start_page = end_page + 1

    return page_ranges


def parallel_fetch_data(max_pages, workers):
    page_ranges = distribute_pages(max_pages, workers)

    print(f"Distributing {max_pages} pages across {len(page_ranges)} workers:")
    for i, (start, end) in enumerate(page_ranges):
        print(f"  Worker {i + 1}: Pages {start} to {end} ({end - start + 1} pages)")

    all_data = []

    with ProcessPoolExecutor(max_workers=len(page_ranges)) as executor:
        future_to_page_range = {
            executor.submit(fetch_data_with_browser, page_range): page_range
            for page_range in page_ranges
        }

        for future in tqdm(as_completed(future_to_page_range),
                           total=len(page_ranges),
                           desc="Workers completed"):
            page_range = future_to_page_range[future]
            try:
                worker_data = future.result()
                print(f"Worker for pages {page_range[0]}-{page_range[1]} completed, fetched {len(worker_data)} items")
                all_data.extend(worker_data)
            except Exception as e:
                print(f"Worker for pages {page_range[0]}-{page_range[1]} generated an exception: {str(e)}")

    return all_data


def main():
    parser = argparse.ArgumentParser(description='Scrape data from rgis.mosreg.ru')
    parser.add_argument('--max-pages', type=int, default=10,
                        help='Maximum number of pages to scrape (default: 10)')
    parser.add_argument('--output', type=str, default='data.json',
                        help='Output JSON file path (default: data.json)')
    parser.add_argument('--workers', type=int, default=1,
                        help='Number of parallel workers/browser instances (default: 1)')

    args = parser.parse_args()

    if args.max_pages < 1:
        print("Error: max-pages must be at least 1")
        return

    if args.workers < 1:
        print("Error: workers must be at least 1")
        return

    print(f"Starting scraper with configuration:")
    print(f"  Max pages: {args.max_pages}")
    print(f"  Output file: {args.output}")
    print(f"  Parallel workers: {args.workers}")

    start_time = time.time()

    if args.workers > 1:
        print(f"Using parallel processing with {args.workers} workers")
        data = parallel_fetch_data(args.max_pages, args.workers)
    else:
        print("Using single worker processing")
        data = fetch_data_with_browser((1, args.max_pages))

    processed_data = process_data(data)

    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump(processed_data, f, ensure_ascii=False, indent=2)

    end_time = time.time()
    duration = end_time - start_time

    print(f"Scraped {len(processed_data)} items and saved to {args.output}")
    print(f"Total execution time: {duration:.2f} seconds")


if __name__ == "__main__":
    multiprocessing.set_start_method('spawn', force=True)
    main()
