from flask import Flask, jsonify, request, render_template_string
import os
import requests
from bs4 import BeautifulSoup
import pandas as pd

app = Flask(__name__)

IMAGES_DIR = "images"
CSV_FILE = "product_data.csv"

HTML_FORM = '''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Shopify Product Scraper</title>
</head>
<body>
    <h2>Enter Shopify Product URL</h2>
    <form method="post" action="/scrape">
        <input type="text" name="url" style="width:400px" placeholder="Paste product URL here" required>
        <button type="submit">Scrape</button>
    </form>
</body>
</html>
'''

@app.route('/', methods=['GET'])
def index():
    return render_template_string(HTML_FORM)

@app.route('/scrape', methods=['GET', 'POST'])
def scrape():
    # Get URL from client
    if request.method == 'POST':
        product_url = request.form.get('url') or request.json.get('url')
    else:
        product_url = request.args.get('url')
    if not product_url:
        return jsonify({'error': 'No product URL provided'}), 400

    response = requests.get(product_url)
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
                src = product_url.split('/products/')[0] + src
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

    # 4. Extract values from DiamondInfo__container area
    diamond_info_container = soup.find('div', class_='DiamondInfo__container')
    if diamond_info_container:
        for block in diamond_info_container.find_all('div', class_='DiamondInfo__block'):
            value_tag = block.find('p', class_='DiamondInfo__block-value')
            title_tag = block.find('p', class_='DiamondInfo__block-title')
            if value_tag and title_tag:
                title = title_tag.get_text(strip=True)
                value = value_tag.get_text(strip=True)
                diamond_info[title] = value

    # 5. Extract product title and SVG icon from cpst-title-container
    product_title = ''
    product_svg = ''
    title_container = soup.find('div', class_='cpst-title-container')
    if title_container:
        title_text_container = title_container.find('div', class_='cpst-title-text-container')
        if title_text_container:
            h1_tag = title_text_container.find('h1', class_='cpst-title')
            if h1_tag:
                product_title = h1_tag.get_text(strip=True)
        icon_container = title_container.find('div', class_='cpst-title-icon-container')
        if icon_container:
            svg_tag = icon_container.find('svg')
            if svg_tag:
                product_svg = str(svg_tag)

    # 6. Save to CSV
    data = {
        'Product Title': product_title,
        'Product SVG': product_svg,
        **diamond_info,
        **diamond_details,
        'images': ';'.join(image_urls)
    }
    df = pd.DataFrame([data])
    df.to_csv(CSV_FILE, index=False)

    return jsonify({'message': 'Scraping complete', 'csv': CSV_FILE, 'images_saved': len(image_urls), 'data': data})

if __name__ == '__main__':
    app.run(debug=True) 