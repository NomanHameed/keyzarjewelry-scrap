import json
import csv
import os
import glob
import logging
import time

# --- Configuration ---
JSON_BATCHES_DIR = "downloads"  # Directory where JSON batch files are saved
OUTPUT_CSV_FILE = "jewelry_products.csv"  # Name of the final merged CSV file

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def merge_json_batches_to_csv():
    """
    Merges all JSON files from a specified directory into a single CSV file.
    """
    logging.info(f"Starting merge process for JSON files in '{JSON_BATCHES_DIR}'...")
    start_time = time.perf_counter()

    all_products = []
    json_files_found = 0

    # Find all JSON files in the specified directory
    # Using glob for pattern matching (e.g., products_batch_*.json)
    json_file_paths = sorted(glob.glob(os.path.join(JSON_BATCHES_DIR, "*.json")))

    if not json_file_paths:
        logging.warning(f"No JSON files found in '{JSON_BATCHES_DIR}'. Nothing to merge.")
        return

    # 1. Load all data from JSON files
    for file_path in json_file_paths:
        json_files_found += 1
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, list):
                    all_products.extend(data)
                    logging.info(f"Loaded {len(data)} products from '{os.path.basename(file_path)}'.")
                else:
                    logging.warning(
                        f"File '{os.path.basename(file_path)}' does not contain a list of products. Skipping.")
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON from '{os.path.basename(file_path)}': {e}. Skipping this file.")
        except IOError as e:
            print(f"Error reading file '{os.path.basename(file_path)}': {e}. Skipping this file.")
        except Exception as e:
            print(
                f"An unexpected error occurred while processing '{os.path.basename(file_path)}': {e}. Skipping this file.")

    if not all_products:
        logging.warning("No valid product data found across all JSON files. CSV will not be created.")
        return

    logging.info(
        f"Successfully loaded data from {json_files_found} JSON files. Total products to write: {len(all_products)}")

    # 2. Determine all unique fieldnames (CSV headers)
    fieldnames = set()
    for product in all_products:
        fieldnames.update(product.keys())

    # Sort fieldnames to ensure consistent column order in the CSV
    fieldnames = sorted(list(fieldnames))
    logging.info(f"CSV headers identified: {fieldnames}")

    # 3. Write data to a single CSV file
    try:
        with open(JSON_BATCHES_DIR +'/'+ OUTPUT_CSV_FILE, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()  # Write the header row

            # Write each product dictionary as a row
            # DictWriter automatically handles missing keys by leaving cells blank
            writer.writerows(all_products)

        logging.info(f"All products successfully merged and saved to '{OUTPUT_CSV_FILE}'.")

    except IOError as e:
        print(f"Critical error: Could not write to CSV file '{OUTPUT_CSV_FILE}': {e}")
    except Exception as e:
        print(f"An unexpected error occurred during CSV writing: {e}")

    end_time = time.perf_counter()
    total_duration = end_time - start_time
    minutes = int(total_duration // 60)
    seconds = int(total_duration % 60)
    logging.info(f"Merge process completed in {minutes:02d}m {seconds:02d}s ({total_duration:.2f} seconds).")


if __name__ == "__main__":
    merge_json_batches_to_csv()
