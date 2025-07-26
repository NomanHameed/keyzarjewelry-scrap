import os
import json
import csv

DOWNLOADS_DIR = 'response'
OUTPUT_CSV = 'combined_products.csv'

# Fields to extract
CSV_FIELDS = [
    'id', 'title', 'vendor', 'productType', 'description',
    'media_alt1', 'media_url1', 'media_alt2', 'media_url2',
    'variant_center_stone_shape', 'variant_material',
    'variant_price', 'variant_compare_at_price',
    'shankWidth', 'sideStonesOrigin', 'sideStonesShape',
    'sideStonesAverageColor', 'sideStonesAverageClarity',
    'sideStonesAverageCaratWeig', 'style', 'styleComment'
]

def extract_field(dct, path, default=None):
    """Safely extract a nested field from a dict using a list path."""
    for key in path:
        if isinstance(dct, dict):
            dct = dct.get(key)
        elif isinstance(dct, list) and isinstance(key, int) and len(dct) > key:
            dct = dct[key]
        else:
            return default
        if dct is None:
            return default
    return dct

def extract_product_fields(product):
    row = {}
    row['id'] = product.get('id', '')
    row['title'] = product.get('title', '')
    row['vendor'] = product.get('vendor', '')
    row['productType'] = product.get('productType', '')
    row['description'] = product.get('description', '')

    # Media fields (first and second image/alt if available)
    media_nodes = extract_field(product, ['media', 'nodes'], [])
    row['media_alt1'] = extract_field(media_nodes, [0, 'alt'], '')
    row['media_url1'] = extract_field(media_nodes, [0, 'image', 'url'], '')
    row['media_alt2'] = extract_field(media_nodes, [1, 'alt'], '')
    row['media_url2'] = extract_field(media_nodes, [1, 'image', 'url'], '')

    # Variants (first variant)
    variant_nodes = extract_field(product, ['variants', 'nodes'], [])
    if variant_nodes:
        selected_options = variant_nodes[0].get('selectedOptions', [])
        # Find values by option name
        row['variant_center_stone_shape'] = next((opt.get('value', '') for opt in selected_options if opt.get('name') == 'Center Stone Shape'), '')
        row['variant_material'] = next((opt.get('value', '') for opt in selected_options if opt.get('name') == 'Material'), '')
        row['variant_price'] = extract_field(variant_nodes[0], ['price', 'amount'], '')
        row['variant_compare_at_price'] = extract_field(variant_nodes[0], ['compareAtPrice', 'amount'], '')
    else:
        row['variant_center_stone_shape'] = ''
        row['variant_material'] = ''
        row['variant_price'] = ''
        row['variant_compare_at_price'] = ''

    # Metafields
    row['shankWidth'] = extract_field(product, ['shankWidth', 'value'], '')
    row['sideStonesOrigin'] = extract_field(product, ['sideStonesOrigin', 'value'], '')
    row['sideStonesShape'] = extract_field(product, ['sideStonesShape', 'value'], '')
    row['sideStonesAverageColor'] = extract_field(product, ['sideStonesAverageColor', 'value'], '')
    row['sideStonesAverageClarity'] = extract_field(product, ['sideStonesAverageClarity', 'value'], '')
    row['sideStonesAverageCaratWeig'] = extract_field(product, ['sideStonesAverageCaratWeig', 'value'], '')
    row['style'] = extract_field(product, ['style', 'value'], '')
    row['styleComment'] = extract_field(product, ['styleComment', 'value'], '')
    return row

def main():
    all_rows = []
    for filename in sorted(os.listdir(DOWNLOADS_DIR)):
        if filename.endswith('.json'):
            filepath = os.path.join(DOWNLOADS_DIR, filename)
            with open(filepath, 'r', encoding='utf-8') as f:
                try:
                    data = json.load(f)
                except Exception as e:
                    print(f"Error reading {filename}: {e}")
                    continue
                # Support both list of products and dict with 'nodes'
                if isinstance(data, list):
                    products = data
                elif isinstance(data, dict) and 'nodes' in data:
                    products = data['nodes']
                else:
                    products = data
                for product in products:
                    if not isinstance(product, dict):
                        continue
                    row = extract_product_fields(product)
                    all_rows.append(row)
    # Write to CSV
    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for row in all_rows:
            writer.writerow(row)
    print(f"Combined CSV written to {OUTPUT_CSV} with {len(all_rows)} products.")

if __name__ == "__main__":
    main() 