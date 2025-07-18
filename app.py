from flask import Flask, jsonify
import os
import requests
from bs4 import BeautifulSoup
import pandas as pd

app = Flask(__name__)

# PRODUCT_URL = "https://keyzarjewelry.com/products/natural_loosediamond_elongatedcushion_00-50_vvs2_i_55f2636ebdf6"
PRODUCT_URL = "https://keyzarjewelry.com/products/lab_loosediamond_round_02-02_if_g_7cd1af0f2501"
IMAGES_DIR = "images"
CSV_FILE = "product_data.csv"

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

if __name__ == '__main__':
    app.run(debug=True) 