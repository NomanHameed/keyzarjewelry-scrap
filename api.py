import json
import requests
import time
import os
import logging

# --- Configuration ---
API_URL = 'https://keyzarjewelry.com/collections/center-stones?_data=routes/($locale).collections.center-stones'
OUTPUT_DIR = "downloads"
PRODUCTS_PER_REQUEST = 14
MAX_CURSOR = 1818  # Based on your observation (25441 / 14 = ~1817.2)
PRODUCTS_PER_FILE = 1120  # Save a new JSON file after every 1400 products
TOTAL_PRODUCTS_EXPECTED = 25441  # Total products to aim for

REQUEST_DELAY_SECONDS = 0.5  # Delay between requests to be polite to the server (0.5 seconds)
MAX_RETRIES = 3  # Number of retries for failed requests
RETRY_DELAY_SECONDS = 5  # Delay before retrying a failed request

# Setup logging
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s')
error_logger = logging.getLogger('scraper_errors')
error_handler = logging.FileHandler('scraper_errors.log')
error_handler.setLevel(logging.ERROR)
error_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
error_handler.setFormatter(error_formatter)
error_logger.addHandler(error_handler)


def save_batch_to_json(data_batch, batch_number):
    """Saves a list of product dictionaries to a JSON file."""
    if not data_batch:
        print(f"Batch {batch_number}: No data to save.")
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)  # Ensure output directory exists
    file_path = os.path.join(OUTPUT_DIR, f"batch_{batch_number:03d}.json")

    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data_batch, f, indent=4, ensure_ascii=False)
        print(f"Batch {batch_number}: Saved {len(data_batch)} products to {file_path}")
    except IOError as e:
        error_logger.error(f"Batch {batch_number}: Error saving to {file_path}: {e}")
    except Exception as e:
        error_logger.error(f"Batch {batch_number}: Unexpected error saving JSON: {e}")


def scrape_keyzar_api():
    """
    Scrapes product data from Keyzar Jewelry API by iterating through cursor values,
    saves data in batches to JSON files, and logs progress and errors.
    """
    print("Starting Keyzar Jewelry API scraping process...")
    overall_start_time = time.perf_counter()

    all_products_in_current_batch = []
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

    # Iterate through cursor values
    # The cursor likely represents page number or offset for batches of 14 products.
    # We iterate up to MAX_CURSOR to cover the TOTAL_PRODUCTS_EXPECTED.
    for cursor_value in range(1, MAX_CURSOR + 1):
        request_start_time = time.perf_counter()

        # Update cursor for the current request
        current_json_payload = base_json_payload.copy()
        current_json_payload["cursor"] = cursor_value

        retries = 0
        while retries < MAX_RETRIES:
            try:
                form_data = {"body": json.dumps(current_json_payload)}
                response = requests.post(API_URL, data=form_data, timeout=20)  # Add a timeout for requests
                response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)

                res_data = response.json()

                products_data = res_data.get('products', [])
                item_to_save = {}
                products_from_response = []
                for item in products_data:

                    item_to_save["title"] = item["title"]
                    item_to_save["price_min"] = item["price_min"]

                    item_to_save["price"] = item['variants'][0]['price']
                    item_to_save["weight"] = item['variants'][0]['weight']

                    item_to_save["originalSrc"] = item['media'][0]['image']['originalSrc']
                    item_to_save["alt"] = item['media'][0]["alt"]

                    item_to_save["image"] = item["images_info"][0]["src"]

                    meta_fields = [
                        "carat", "color", "shape", "clarity", "polish",
                        "lab",  "fluorescence", "length", "width",
                        "symmetry", "length_width_ratio"
                    ]
                    for key_val in item["metafields"]:
                        key_name = key_val["key"]
                        if key_name in meta_fields:
                            item_to_save[key_name] = key_val["value"]

                    products_from_response.append(item_to_save)

                if not products_from_response:
                    logging.warning(
                        f"Cursor {cursor_value}: No products found in response. This might indicate end of data or an issue.")
                    if products_count_overall > 0:
                        break

                all_products_in_current_batch.extend(products_from_response)
                products_count_overall += len(products_from_response)
                products_count_in_current_file += len(products_from_response)

                print(
                    f"Cursor {cursor_value}: Fetched {len(products_from_response)} products. Total fetched: {products_count_overall}")

                # Check if it's time to save a batch
                if products_count_in_current_file >= PRODUCTS_PER_FILE:
                    save_batch_to_json(all_products_in_current_batch, current_file_batch_number)
                    all_products_in_current_batch = []  # Clear the batch
                    products_count_in_current_file = 0  # Reset counter
                    current_file_batch_number += 1  # Increment batch number

                break  # Break retry loop on success

            except requests.exceptions.RequestException as e:
                retries += 1
                error_logger.error(f"Cursor {cursor_value}: Request failed (Attempt {retries}/{MAX_RETRIES}): {e}")
                if hasattr(e, 'response') and e.response is not None:
                    error_logger.error(f"Response Content (if available): {e.response.text}")
                if retries < MAX_RETRIES:
                    print(f"Retrying in {RETRY_DELAY_SECONDS} seconds...")
                    time.sleep(RETRY_DELAY_SECONDS)
                else:
                    error_logger.critical(f"Cursor {cursor_value}: Max retries reached. Skipping this cursor value.")
            except json.JSONDecodeError as e:
                retries += 1
                error_logger.error(f"Cursor {cursor_value}: JSON decode error (Attempt {retries}/{MAX_RETRIES}): {e}")
                if retries < MAX_RETRIES:
                    print(f"Retrying in {RETRY_DELAY_SECONDS} seconds...")
                    time.sleep(RETRY_DELAY_SECONDS)
                else:
                    error_logger.critical(
                        f"Cursor {cursor_value}: Max retries reached for JSON decode error. Skipping.")
            except Exception as e:
                error_logger.critical(f"Cursor {cursor_value}: An unexpected error occurred: {e}")
                break  # Stop processing this cursor on unexpected error

        request_end_time = time.perf_counter()

        # Calculate elapsed time for progress update
        elapsed_time = request_end_time - overall_start_time
        minutes = int(elapsed_time // 60)
        seconds = int(elapsed_time % 60)

        print(
            f"Progress: Request {cursor_value}/{MAX_CURSOR} completed. "
            f"Time elapsed: {minutes:02d}m {seconds:02d}s."
        )

        # Delay between requests to avoid burdening the server
        if cursor_value < MAX_CURSOR:
            time.sleep(REQUEST_DELAY_SECONDS)

    # --- Final Save ---
    if all_products_in_current_batch:
        save_batch_to_json(all_products_in_current_batch, current_file_batch_number)

    overall_end_time = time.perf_counter()
    total_duration = overall_end_time - overall_start_time
    minutes = int(total_duration // 60)
    seconds = int(total_duration % 60)

    print(f"Scraping process finished.")
    print(f"Total products collected: {products_count_overall}")
    print(f"Total execution time: {minutes:02d}m {seconds:02d}s ({total_duration:.2f} seconds)")


if __name__ == "__main__":
    scrape_keyzar_api()
