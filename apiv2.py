import json
import requests
import time
import os
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Configuration ---
API_URL = 'https://keyzarjewelry.com/collections/center-stones?_data=routes/($locale).collections.center-stones'
OUTPUT_DIR = "downloads"
PRODUCTS_PER_REQUEST = 14
MAX_CURSOR = 1818  # Based on your observation (25441 / 14 = ~1817.2)

PARALLEL_REQUESTS = 10  # Number of requests to send in parallel
# Set PRODUCTS_PER_FILE to match products per parallel batch (10 requests * 14 products/request = 140 products)
PRODUCTS_PER_FILE = PRODUCTS_PER_REQUEST * PARALLEL_REQUESTS  # Save a new JSON file after every batch

TOTAL_PRODUCTS_EXPECTED = 25441  # Total products to aim for

BATCH_DELAY_SECONDS = 1.1  # Delay after each batch of PARALLEL_REQUESTS completes
REQUEST_TIMEOUT_SECONDS = 20  # Timeout for individual requests

MAX_RETRIES = 3  # Number of retries for failed requests
RETRY_DELAY_SECONDS = 5  # Delay before retrying a failed request

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
error_logger = logging.getLogger('scraper_errors')
error_handler = logging.FileHandler('scraper_errors.log')
error_handler.setLevel(logging.ERROR)
error_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
error_handler.setFormatter(error_formatter)
error_logger.addHandler(error_handler)


def save_batch_to_json(data_batch, batch_number):
    """Saves a list of product dictionaries to a JSON file."""
    if not data_batch:
        logging.info(f"Batch {batch_number}: No data to save.")
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)  # Ensure output directory exists
    file_path = os.path.join(OUTPUT_DIR, f"batch_{batch_number:03d}.json")

    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data_batch, f, indent=4, ensure_ascii=False)
        logging.info(f"Batch {batch_number}: Saved {len(data_batch)} products to {file_path}")
    except IOError as e:
        error_logger.error(f"Batch {batch_number}: Error saving to {file_path}: {e}")
    except Exception as e:
        error_logger.error(f"Batch {batch_number}: Unexpected error saving JSON: {e}")


def fetch_and_parse_single_cursor(cursor_value, base_json_payload):
    """
    Fetches data for a single cursor value and extracts relevant product info.
    This function will be run in parallel by the ThreadPoolExecutor.
    Returns a list of parsed product dictionaries or None on failure.
    """
    current_json_payload = base_json_payload.copy()
    current_json_payload["cursor"] = cursor_value

    retries = 0
    while retries < MAX_RETRIES:
        try:
            form_data = {"body": json.dumps(current_json_payload)}
            response = requests.post(API_URL, data=form_data, timeout=REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status()

            res_data = response.json()
            products_raw = res_data.get('products', [])

            products_parsed = []
            for item in products_raw:
                item_to_save = {}  # Create a NEW dictionary for each product

                item_to_save["title"] = item.get("title", "")
                item_to_save["price_min"] = item.get("price_min", "")

                # Safely access nested variant price and weight
                if item.get('variants') and len(item['variants']) > 0:
                    item_to_save["price"] = item['variants'][0].get('price', "")
                    item_to_save["weight"] = item['variants'][0].get('weight', "")
                else:
                    item_to_save["price"] = ""
                    item_to_save["weight"] = ""

                # Safely access nested image URLs and alt text
                if item.get('media') and len(item['media']) > 0 and item['media'][0].get('image'):
                    item_to_save["originalSrc"] = item['media'][0]['image'].get('originalSrc', "")
                    item_to_save["alt"] = item['media'][0].get("alt", "")
                else:
                    item_to_save["originalSrc"] = ""
                    item_to_save["alt"] = ""

                if item.get('images_info') and len(item['images_info']) > 0:
                    item_to_save["image"] = item['images_info'][0].get("src", "")
                else:
                    item_to_save["image"] = ""

                meta_fields = [
                    "carat", "color", "shape", "clarity", "polish",
                    "lab", "fluorescence", "length", "width",
                    "symmetry", "length_width_ratio"
                ]
                # Extract metafields
                if item.get("metafields"):
                    for key_val in item["metafields"]:
                        key_name = key_val.get("key")
                        if key_name in meta_fields:
                            item_to_save[key_name] = key_val.get("value", "")

                products_parsed.append(item_to_save)

            return products_parsed  # Return the list of parsed products

        except requests.exceptions.RequestException as e:
            retries += 1
            error_logger.error(f"Cursor {cursor_value}: Request failed (Attempt {retries}/{MAX_RETRIES}): {e}")
            if hasattr(e, 'response') and e.response is not None:
                error_logger.error(f"Response Content (if available): {e.response.text}")
            if retries < MAX_RETRIES:
                logging.info(f"Retrying cursor {cursor_value} in {RETRY_DELAY_SECONDS} seconds...")
                time.sleep(RETRY_DELAY_SECONDS)
            else:
                error_logger.critical(f"Cursor {cursor_value}: Max retries reached. Skipping this cursor value.")
                return None  # Indicate failure
        except json.JSONDecodeError as e:
            retries += 1
            error_logger.error(f"Cursor {cursor_value}: JSON decode error (Attempt {retries}/{MAX_RETRIES}): {e}")
            if retries < MAX_RETRIES:
                logging.info(f"Retrying cursor {cursor_value} in {RETRY_DELAY_SECONDS} seconds...")
                time.sleep(RETRY_DELAY_SECONDS)
            else:
                error_logger.critical(f"Cursor {cursor_value}: Max retries reached for JSON decode error. Skipping.")
                return None  # Indicate failure
        except Exception as e:
            error_logger.critical(f"Cursor {cursor_value}: An unexpected error occurred during parsing: {e}")
            return None  # Indicate failure
    return None  # Should only be reached if all retries fail


def scrape_keyzar_api_parallel():
    """
    Scrapes product data from Keyzar Jewelry API using parallel requests in batches,
    saves data in batches to JSON files, and logs progress and errors.
    """
    logging.info("Starting Keyzar Jewelry API scraping process with parallel requests...")
    overall_start_time = time.perf_counter()

    all_products_in_current_batch_file = []
    current_file_batch_number = 1
    products_count_overall = 0
    products_count_in_current_file = 0

    # Define the base JSON payload
    base_json_payload = {
        "filtersState": {
            "diamond": {
                "type": "natural_LooseDiamond",
                "cutRange": [0, 3], "colorRange": [0, 7], "caratRange": [0.5, 11],
                "clarityRange": [0, 7], "priceRange": [230, 1103370],
                "polishRange": [0, 3], "symmetryRange": [0, 3],
            },
            "labDiamond": {
                "type": "lab_LooseDiamond",
                "cutRange": [0, 3], "colorRange": [4, 7], "caratRange": [2, 11],
                "clarityRange": [3, 7], "priceRange": [230, 1103370],
                "polishRange": [0, 3], "symmetryRange": [0, 3],
            }
        },
        "stoneTypeState": "labDiamond",
        "sortState": "price-ascending",
        "currencyCode": "USD",
        "currencyRate": "1.0"
    }

    # Use ThreadPoolExecutor for parallel requests
    with ThreadPoolExecutor(max_workers=PARALLEL_REQUESTS) as executor:
        # Iterate through cursor values in chunks (batches)
        for i in range(1, MAX_CURSOR + 1, PARALLEL_REQUESTS):
            batch_start_time = time.perf_counter()

            # Create a dictionary to map futures to their cursor values
            future_to_cursor = {}
            for cursor_value in range(i, min(i + PARALLEL_REQUESTS, MAX_CURSOR + 1)):
                future = executor.submit(fetch_and_parse_single_cursor, cursor_value, base_json_payload)
                future_to_cursor[future] = cursor_value

            # Process results as they complete
            for future in as_completed(future_to_cursor):
                cursor_value_processed = future_to_cursor[future]  # Retrieve cursor value reliably
                try:
                    products_from_response = future.result()  # Get the list of parsed products
                    if products_from_response is not None:
                        if not products_from_response:
                            logging.warning(
                                f"Cursor {cursor_value_processed}: Fetched 0 products. This might indicate end of data or an issue.")

                        all_products_in_current_batch_file.extend(products_from_response)
                        products_count_overall += len(products_from_response)
                        products_count_in_current_file += len(products_from_response)

                        logging.info(
                            f"Cursor {cursor_value_processed}: Processed {len(products_from_response)} products. Total fetched so far: {products_count_overall}")

                except Exception as exc:
                    error_logger.critical(f"Error processing future for cursor {cursor_value_processed}: {exc}")

            # Check if it's time to save a batch file
            # This condition is now met after every PARALLEL_REQUESTS batch
            if products_count_in_current_file >= PRODUCTS_PER_FILE:
                save_batch_to_json(all_products_in_current_batch_file, current_file_batch_number)
                all_products_in_current_batch_file = []  # Clear the batch
                products_count_in_current_file = 0  # Reset counter
                current_file_batch_number += 1  # Increment batch number

            # Apply delay after each batch of parallel requests
            batch_end_time = time.perf_counter()
            elapsed_for_batch = batch_end_time - batch_start_time

            # Calculate elapsed time for overall progress update
            overall_elapsed_time = batch_end_time - overall_start_time
            minutes = int(overall_elapsed_time // 60)
            seconds = int(overall_elapsed_time % 60)

            logging.info(
                f"Batch completed (Cursors {i} to {min(i + PARALLEL_REQUESTS - 1, MAX_CURSOR)}). "
                f"Time elapsed for batch: {elapsed_for_batch:.2f}s. "
                f"Overall elapsed: {minutes:02d}m {seconds:02d}s."
            )

            # Only sleep if there are more batches to process
            if i + PARALLEL_REQUESTS <= MAX_CURSOR:
                time.sleep(BATCH_DELAY_SECONDS)

    # --- Final Save ---
    # Save any remaining products in the last batch file
    if all_products_in_current_batch_file:
        save_batch_to_json(all_products_in_current_batch_file, current_file_batch_number)

    overall_end_time = time.perf_counter()
    total_duration = overall_end_time - overall_start_time
    minutes = int(total_duration // 60)
    seconds = int(total_duration % 60)

    logging.info(f"Scraping process finished.")
    logging.info(f"Total products collected: {products_count_overall}")
    logging.info(f"Total execution time: {minutes:02d}m {seconds:02d}s ({total_duration:.2f} seconds)")


if __name__ == "__main__":
    scrape_keyzar_api_parallel()
