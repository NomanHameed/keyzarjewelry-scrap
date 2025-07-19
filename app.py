from flask import Flask, jsonify, send_file
import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
import time
from webdriver_manager.chrome import ChromeDriverManager
import logging

app = Flask(__name__)

# PRODUCT_URL = "https://keyzarjewelry.com/products/natural_loosediamond_elongatedcushion_00-50_vvs2_i_55f2636ebdf6"
PRODUCT_URL = "https://keyzarjewelry.com/products/lab_loosediamond_round_02-02_if_g_7cd1af0f2501"
IMAGES_DIR = "images"
CSV_FILE = "product_data.csv"

CHECKPOINT_FILE = "center_stones_selenium_checkpoint.txt"
ERROR_LOG_FILE = "center_stones_selenium_errors.log"

# Setup error logging
logging.basicConfig(filename=ERROR_LOG_FILE, level=logging.ERROR)

def scrape_product(url):
    response = requests.get(url)
    if response.status_code != 200:
        return None
    soup = BeautifulSoup(response.text, 'html.parser')

    # 1. Collect product image URLs (no download)
    image_urls = []
    for img in soup.find_all('img'):
        src = img.get('src')
        if src and ('product' in src or 'diamond' in src):
            if src.startswith('//'):
                src = 'https:' + src
            elif src.startswith('/'):
                src = 'https://keyzarjewelry.com' + src
            image_urls.append(src)

    # 2. Extract 'Your Diamond Info' section (using class selectors and data-current-block)
    diamond_info_fields = {
        'carat': 'Carat',
        'color': 'Color',
        'clarity': 'Clarity',
        'cut': 'Cut',
        'dimensions (mm)': 'Dimensions (mm)',
        'certification': 'Certification',
    }
    diamond_info = {v: '' for v in diamond_info_fields.values()}
    info_section = soup.find(class_='StoneProductInfo')
    if info_section:
        container = info_section.find('div', class_='cpcst-info-container')
        if container:
            for block in container.find_all('div', class_='StoneDetailBlock'):
                data_block = block.get('data-current-block', '').strip().lower()
                value_tag = block.find('p', class_='StoneDetailBlock__content-value')
                label_tag = block.find(class_='cpcst-detail-title')
                if data_block == 'dimensions (mm)' and value_tag:
                    diamond_info['Dimensions (mm)'] = value_tag.get_text(strip=True)
                elif data_block == 'certification' and value_tag:
                    diamond_info['Certification'] = value_tag.get_text(strip=True)
                elif label_tag and value_tag:
                    label = label_tag.get_text(strip=True).lower()
                    value = value_tag.get_text(strip=True)
                    for field_key, field_name in diamond_info_fields.items():
                        if field_key in label:
                            diamond_info[field_name] = value

    # 3. Extract 'Diamond Details' section (using class selectors and index mapping)
    diamond_details_fields = ['Carat', 'Shape', 'Color', 'Clarity', 'L/W (mm)', 'Ratio', 'Cut']
    diamond_details = {field: '' for field in diamond_details_fields}
    tabs_wrapper = soup.find('div', class_='cpst-tabs-wrapper')
    if tabs_wrapper:
        titles = tabs_wrapper.find_all('div', class_='cpcst-details-title')
        values = tabs_wrapper.find_all('div', class_='cpcst-details-value')
        for i, title_tag in enumerate(titles):
            title = title_tag.get_text(strip=True)
            if i < len(values):
                value = values[i].get_text(strip=True)
                if title in diamond_details:
                    diamond_details[title] = value

    svg_tag = ''
    heading = ''
    price = ''
    title_container = soup.find('div', class_='cpst-title-container')
    if title_container:
        title_text_container = title_container.find('div', class_='cpst-title-text-container')
        if title_text_container:
            h1_tag = title_text_container.find('h1', class_='cpst-title')
            if h1_tag:
                heading = h1_tag.get_text(strip=True)
            # Extract price from the same container
            price_tag = title_text_container.find('div', class_='tangiblee-price')
            if price_tag:
                price = price_tag.get_text(strip=True)

    data = {
        'Product Title': str(heading),
        'Price': price,
        **diamond_info,
        **diamond_details,
        'images': ';'.join(image_urls),
        'url': url
    }
    return data
    
@app.route('/scrape', methods=['GET'])
def scrape():
    data = scrape_product(PRODUCT_URL)
    if not data:
        return jsonify({'error': 'Failed to fetch product page'}), 500
    df = pd.DataFrame([data])
    df.to_csv(CSV_FILE, index=False)
    return jsonify({'message': 'Scraping complete', 'csv': CSV_FILE, 'images_saved': len(data['images'].split(';'))})

@app.route('/scrape_collection', methods=['GET'])
def scrape_collection():
    base_url = 'https://keyzarjewelry.com'
    collection_url = base_url + '/collections/center-stones'
    response = requests.get(collection_url)
    if response.status_code != 200:
        return jsonify({'error': 'Failed to fetch collection page'}), 500
    soup = BeautifulSoup(response.text, 'html.parser')
    product_links = set()
    for a in soup.find_all('a', href=True):
        href = a['href']
        if href.startswith('/products/'):
            product_links.add(base_url + href)
        if len(product_links) >= 14:
            break
    if not product_links:
        return jsonify({'error': 'No product links found'}), 404
    all_data = []
    for url in product_links:
        data = scrape_product(url)
        if data:
            all_data.append(data)
    if not all_data:
        return jsonify({'error': 'No product data scraped'}), 500
    df = pd.DataFrame(all_data)
    df.to_csv(CSV_FILE, index=False)
    return jsonify({'message': 'Scraping complete', 'csv': CSV_FILE, 'products_scraped': len(all_data)})

@app.route('/scrape_all_pages', methods=['GET'])
def scrape_all_pages():
    service = Service(ChromeDriverManager().install())
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(service=service, options=options)

    url = "https://keyzarjewelry.com/collections/center-stones"
    driver.get(url)
    time.sleep(5)

    base_url = "https://keyzarjewelry.com"
    all_product_links = set()
    all_data = []
    header_written = os.path.exists(CSV_FILE)

    # Resume support: load last completed page
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f:
            page_num = int(f.read().strip())
        print(f"Resuming from page {page_num}")
        # Click next page_num-1 times to reach the last checkpoint
        for _ in range(1, page_num):
            try:
                next_button = driver.find_element(
                    By.XPATH,
                    "//div[contains(@class, 'leading-tighter')]/following-sibling::button[1]"
                )
                if next_button.get_attribute("disabled"):
                    print("Next button is disabled. No more pages.")
                    break
                driver.execute_script("arguments[0].click();", next_button)
                print(f"Jumped to page {_+1}")
                time.sleep(5)
            except Exception as e:
                print(f"Could not jump to page {_+1}: {e}")
                break
    else:
        page_num = 1
        print("Starting from page 1")

    while True:
        print(f"Scraping page {page_num} ...")
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        product_links = []
        for a in soup.find_all('a', href=True):
            href = a['href']
            if href.startswith('/products/') and (base_url + href) not in all_product_links:
                product_links.append(base_url + href)
                all_product_links.add(base_url + href)
        print(f"Found {len(product_links)} new products on page {page_num}")
        page_data = []
        for url in product_links:
            print(f"  Scraping product: {url}")
            # Retry logic for product requests
            for attempt in range(3):
                try:
                    response = requests.get(url, timeout=10)
                    if response.status_code == 200:
                        break
                except Exception as e:
                    print(f"    Attempt {attempt+1} failed for {url}: {e}")
                    if attempt == 2:
                        logging.error(f"Failed to fetch {url} after 3 attempts: {e}")
                        response = None
            else:
                response = None
            if not response or response.status_code != 200:
                print(f"    Failed to fetch {url} after 3 attempts. Skipping.")
                continue
            try:
                soup_prod = BeautifulSoup(response.text, 'html.parser')
                image_urls = []
                for img in soup_prod.find_all('img'):
                    src = img.get('src')
                    if src and ('product' in src or 'diamond' in src):
                        if src.startswith('//'):
                            src = 'https:' + src
                        elif src.startswith('/'):
                            src = 'https://keyzarjewelry.com' + src
                        image_urls.append(src)
                diamond_info_fields = {
                    'carat': 'Carat',
                    'color': 'Color',
                    'clarity': 'Clarity',
                    'cut': 'Cut',
                    'dimensions (mm)': 'Dimensions (mm)',
                    'certification': 'Certification',
                }
                diamond_info = {v: '' for v in diamond_info_fields.values()}
                info_section = soup_prod.find(class_='StoneProductInfo')
                if info_section:
                    container = info_section.find('div', class_='cpcst-info-container')
                    if container:
                        for block in container.find_all('div', class_='StoneDetailBlock'):
                            data_block = block.get('data-current-block', '').strip().lower()
                            value_tag = block.find('p', class_='StoneDetailBlock__content-value')
                            label_tag = block.find(class_='cpcst-detail-title')
                            if data_block == 'dimensions (mm)' and value_tag:
                                diamond_info['Dimensions (mm)'] = value_tag.get_text(strip=True)
                            elif data_block == 'certification' and value_tag:
                                diamond_info['Certification'] = value_tag.get_text(strip=True)
                            elif label_tag and value_tag:
                                label = label_tag.get_text(strip=True).lower()
                                value = value_tag.get_text(strip=True)
                                for field_key, field_name in diamond_info_fields.items():
                                    if field_key in label:
                                        diamond_info[field_name] = value
                diamond_details_fields = ['Carat', 'Shape', 'Color', 'Clarity', 'L/W (mm)', 'Ratio', 'Cut']
                diamond_details = {field: '' for field in diamond_details_fields}
                tabs_wrapper = soup_prod.find('div', class_='cpst-tabs-wrapper')
                if tabs_wrapper:
                    titles = tabs_wrapper.find_all('div', class_='cpcst-details-title')
                    values = tabs_wrapper.find_all('div', class_='cpcst-details-value')
                    for i, title_tag in enumerate(titles):
                        title = title_tag.get_text(strip=True)
                        if i < len(values):
                            value = values[i].get_text(strip=True)
                            if title in diamond_details:
                                diamond_details[title] = value
                heading = ''
                price = ''
                title_container = soup_prod.find('div', class_='cpst-title-container')
                if title_container:
                    title_text_container = title_container.find('div', class_='cpst-title-text-container')
                    if title_text_container:
                        h1_tag = title_text_container.find('h1', class_='cpst-title')
                        if h1_tag:
                            heading = h1_tag.get_text(strip=True)
                        price_tag = title_text_container.find('div', class_='tangiblee-price')
                        if price_tag:
                            price = price_tag.get_text(strip=True)
                data = {
                    'Product Title': str(heading),
                    'Price': price,
                    **diamond_info,
                    **diamond_details,
                    'images': ';'.join(image_urls),
                    'url': url
                }
                page_data.append(data)
            except Exception as e:
                print(f"    Error parsing product {url}: {e}")
                logging.error(f"Error parsing product {url}: {e}")
                continue
        # Batch save after each page
        if page_data:
            df = pd.DataFrame(page_data)
            if not header_written:
                df.to_csv(CSV_FILE, mode='w', header=True, index=False)
                header_written = True
            else:
                df.to_csv(CSV_FILE, mode='a', header=False, index=False)
            print(f"Saved {len(page_data)} products from page {page_num} to {CSV_FILE}")
        # Save checkpoint
        with open(CHECKPOINT_FILE, 'w') as f:
            f.write(str(page_num))
        try:
            next_button = driver.find_element(
                By.XPATH,
                "//div[contains(@class, 'leading-tighter')]/following-sibling::button[1]"
            )
            if next_button.get_attribute("disabled"):
                print("Next button is disabled. No more pages.")
                break
            driver.execute_script("arguments[0].click();", next_button)
            print("Clicked next button.")
            page_num += 1
            time.sleep(5)
        except Exception as e:
            print(f"Could not find or click next button on page {page_num}: {e}")
            break
    driver.quit()
    print(f"Scraping finished. Data saved to {CSV_FILE}. Checkpoint at page {page_num}.")
    return jsonify({'message': f'Scraped up to page {page_num}. Data saved to {CSV_FILE}.', 'csv': CSV_FILE, 'checkpoint': page_num})

@app.route('/download_csv')
def download_csv():
    if not os.path.exists(CSV_FILE):
        return "<h3>CSV file not found. Please run the scraping process first.</h3>", 404
    return send_file(CSV_FILE, as_attachment=True)

@app.route('/')
def index():
    return '''
    <html>
      <head>
        <title>Download CSV</title>
      </head>
      <body>
        <h2>Download Scraped CSV</h2>
        <a href="/download_csv">
          <button style="font-size:1.2em;padding:10px 20px;">Download CSV</button>
        </a>
      </body>
    </html>
    '''

if __name__ == '__main__':
    app.run(debug=True) 