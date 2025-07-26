import json
import os
from playwright.sync_api import sync_playwright

def run():
    os.makedirs("response", exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()

        base_url_part = "_data=routes%2F%28%24locale%29.collections.engagement-ring-settings"
        file_count = 1
        collected_responses = []

        def handle_response(response):
            nonlocal file_count
            if base_url_part in response.url:
                if response.status == 200 and response.request.method in ["GET", "POST"]:
                    try:
                        json_data = response.json()
                        file_path = f"response/file{file_count}.json"
                        with open(file_path, "w", encoding="utf-8") as f:
                            json.dump(json_data, f, indent=2)
                        print(f"[✓] Saved {response.request.method} response to {file_path}")
                        file_count += 1
                    except Exception as e:
                        print(f"[!] Failed to save response: {e}")

        # Attach response listener
        page.on("response", handle_response)

        print("[*] Navigating to page")
        page.goto("https://keyzarjewelry.com/collections/engagement-ring-settings", timeout=60000)
        page.wait_for_timeout(5000)

        while True:
            try:
                load_more_btn = page.locator("button.tangiblee-load-more")
                if not load_more_btn.is_visible():
                    print("[✓] No more 'Load More' button found. Exiting loop.")
                    break

                print("[*] Clicking 'Load More'")
                load_more_btn.click()
                page.wait_for_timeout(5000)

            except Exception as e:
                print(f"[!] Error during Load More: {e}")
                break

        browser.close()

if __name__ == "__main__":
    run()
