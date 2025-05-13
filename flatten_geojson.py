#!/usr/bin/env python3
import json
import sys

def flatten_geojson(input_file, output_file):
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    if data.get('type') != 'FeatureCollection':
        print(f"Error: Input file is not a FeatureCollection, found {data.get('type')}")
        return
    
    flattened_data = {
        "type": "FeatureCollection",
        "features": []
    }
    
    for feature in data.get('features', []):
        if feature.get('geometry', {}).get('type') == 'FeatureCollection':
            nested_features = feature.get('geometry', {}).get('features', [])
            properties = feature.get('properties', {})
            
            for nested_feature in nested_features:
                new_feature = nested_feature.copy()

                new_properties = properties.copy()
                if 'properties' in nested_feature and nested_feature['properties']:
                    new_properties.update(nested_feature.get('properties', {}))
                
                new_feature['properties'] = new_properties
                flattened_data['features'].append(new_feature)
        else:
            # If it's a regular feature, add it directly
            flattened_data['features'].append(feature)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(flattened_data, f, ensure_ascii=False, indent=2)
    
    print(f"Successfully flattened GeoJSON file. Original features: {len(data['features'])}, "
          f"Flattened features: {len(flattened_data['features'])}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python flatten_geojson.py input_file.geojson output_file.geojson")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    flatten_geojson(input_file, output_file)
