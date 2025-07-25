import json
from playwright.sync_api import sync_playwright

def run():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)  # Set headless=True if you don't want to see the browser
        context = browser.new_context()
        page = context.new_page()

        matched_response = None

        def handle_response(response):
            nonlocal matched_response
            if (
                response.request.method == "POST"
                and "_data=routes%2F%28%24locale%29.collections.engagement-ring-settings" in response.url
            ):
                print("[✓] Found target POST response")
                matched_response = response

        # Listen to responses
        page.on("response", handle_response)

        print("[*] Navigating to page")
        page.goto("https://keyzarjewelry.com/collections/engagement-ring-settings", timeout=60000)
        page.wait_for_timeout(5000)

        print("[*] Clicking Load More")
        page.click("button.tangiblee-load-more")
        page.wait_for_timeout(5000)

        # If found, save the response
        if matched_response:
            try:
                data = matched_response.json()
                with open("remix_response.json", "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2)
                print("[✓] Response saved to remix_response.json")
            except Exception as e:
                print("[!] Error parsing or saving JSON:", e)
        else:
            print("[!] No matching POST request found.")

        browser.close()

if __name__ == "__main__":
    run()
