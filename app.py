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

@app.route('/scrape', methods=['GET'])
def scrape():
    # Fetch the product page
    response = requests.get(PRODUCT_URL)
    if response.status_code != 200:
        return jsonify({'error': 'Failed to fetch product page'}), 500
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
                # Prefer data-current-block for Dimensions (mm), else use label
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
    # Extract product title and SVG icon from cpst-title-container
    title_container = soup.find('div', class_='cpst-title-container')
    if title_container:
        # Product Title
        title_text_container = title_container.find('div', class_='cpst-title-text-container')
        if title_text_container:
            h1_tag = title_text_container.find('h1', class_='cpst-title')
            if h1_tag:
                heading = h1_tag.get_text(strip=True)
        # # Product SVG
        # icon_container = title_container.find('div', class_='cpst-title-icon-container')
        # if icon_container:
        #     svg_tag = icon_container.find('svg')


# 4. Save to CSV
    data = {
        'Product Title' : str(heading),
        **diamond_info,
        **diamond_details,
        'images': ';'.join(image_urls),
    }
    
    df = pd.DataFrame([data])
    df.to_csv(CSV_FILE, index=False)


    return jsonify({'message': 'Scraping complete', 'csv': CSV_FILE, 'images_saved': len(image_urls)})

if __name__ == '__main__':
    app.run(debug=True) 