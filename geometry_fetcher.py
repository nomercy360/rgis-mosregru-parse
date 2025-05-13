#!/usr/bin/env python3
import json
import argparse
import aiohttp
import asyncio
import time
from tqdm.asyncio import tqdm

BASE_URL = "https://rgis.mosreg.ru/v3/swagger/map/numberarea"


async def fetch_geometry(session, geometry_id, semaphore):
    async with semaphore:
        url = f"{BASE_URL}?numberarea={geometry_id}"

        try:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    # Extract only the geometry part
                    if "geometry" in data:
                        return geometry_id, data["geometry"]
                    else:
                        print(f"Warning: No geometry found for geometry_id {geometry_id}")
                        return geometry_id, None
                else:
                    print(f"Error fetching geometry_id {geometry_id}: HTTP {response.status}")
                    return geometry_id, None
        except Exception as e:
            print(f"Exception fetching geometry_id {geometry_id}: {str(e)}")
            return geometry_id, None


async def process_batch(session, geometry_ids, max_concurrent_requests, progress_bar):
    # Create a semaphore to limit concurrent requests
    semaphore = asyncio.Semaphore(max_concurrent_requests)

    # Create tasks for all geometry IDs
    tasks = []
    for geometry_id in geometry_ids:
        if geometry_id:  # Skip None or empty geometry_ids
            task = fetch_geometry(session, geometry_id, semaphore)
            tasks.append(task)

    results = {}
    for future in tqdm.as_completed(tasks, total=len(tasks), desc="Fetching geometries", position=0, leave=True):
        geometry_id, geometry = await future
        results[geometry_id] = geometry
        progress_bar.update(1)

    return results


async def fetch_all_geometries(input_file, output_file, max_concurrent_requests):
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    properties_map = {}
    for item in data:
        if item.get('geometry_id'):
            properties_map[item.get('geometry_id')] = {
                'zone_code': item.get('zone_code'),
                'municipality': item.get('municipality')
            }

    geometry_ids = [item.get('geometry_id') for item in data if item.get('geometry_id')]
    print(f"Found {len(geometry_ids)} card IDs to process")

    geometries = {}

    progress_bar = tqdm(total=len(geometry_ids), desc="Overall progress", position=1, leave=True)

    async with aiohttp.ClientSession() as session:
        batch_results = await process_batch(session, geometry_ids, max_concurrent_requests, progress_bar)
        geometries.update(batch_results)

    progress_bar.close()

    geometries = {k: v for k, v in geometries.items() if v is not None}

    geojson = create_geojson(geometries, properties_map)

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(geojson, f, ensure_ascii=False, indent=2)

    print(f"Successfully fetched {len(geometries)} geometries out of {len(geometry_ids)} card IDs")
    print(f"Results saved to {output_file} in GeoJSON format")


def create_geojson(geometries, properties_map):
    features = []

    for geometry_id, geometry in geometries.items():
        additional_props = properties_map.get(geometry_id, {})

        feature = {
            "type": "Feature",
            "properties": {
                "geometry_id": geometry_id,
                "zone_code": additional_props.get('zone_code'),
                "municipality": additional_props.get('municipality')
            },
            "geometry": geometry
        }
        features.append(feature)

    geojson = {
        "type": "FeatureCollection",
        "features": features
    }

    return geojson


def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Fetch geometry data for card IDs and save as GeoJSON')
    parser.add_argument('--input', type=str, default='data.json',
                        help='Input JSON file with geometry_ids (default: data.json)')
    parser.add_argument('--output', type=str, default='geometries.geojson',
                        help='Output GeoJSON file for geometries (default: geometries.geojson)')
    parser.add_argument('--concurrent', type=int, default=10,
                        help='Maximum number of concurrent requests (default: 10)')

    args = parser.parse_args()

    if args.concurrent < 1:
        print("Error: concurrent must be at least 1")
        return

    print(f"Starting geometry fetcher with configuration:")
    print(f"  Input file: {args.input}")
    print(f"  Output file: {args.output}")
    print(f"  Concurrent requests: {args.concurrent}")

    start_time = time.time()

    asyncio.run(fetch_all_geometries(args.input, args.output, args.concurrent))

    end_time = time.time()
    duration = end_time - start_time

    print(f"Total execution time: {duration:.2f} seconds")


if __name__ == "__main__":
    main()
