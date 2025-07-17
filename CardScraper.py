import asyncio  # For running asynchronous operations
import nest_asyncio  # To allow nested event loops (useful in interactive environments like Jupyter)
import re  # Regular expressions, though unused in this snippet
import json  # For JSON parsing/serialization if needed
from playwright.async_api import async_playwright  # Playwright's async API for browser automation
from DetailsScraper import DetailsScraping  # Custom module to scrape card details
from datetime import datetime, timedelta  # For handling date operations
from dateutil.relativedelta import relativedelta  # Unused here, but useful for month/year date deltas

# Define the scraper class for cards
class CardScraper:
    def __init__(self, url):
        self.url = url  # The main URL to scrape brands from
        self.data = []  # Container to store all scraped data

    # Asynchronous method to scrape all brands and their respective types/cards
    async def scrape_brands_and_types(self):
        async with async_playwright() as p:
            # Launch a Chromium browser in headless mode (no GUI)
            browser = await p.chromium.launch(headless=True)
            # Open a new browser tab/page
            page = await browser.new_page()
            # Navigate to the initial URL
            await page.goto(self.url)

            # Query all elements that match the brand links using their class selector
            brand_elements = await page.query_selector_all('.styles_itemWrapper__MTzPB a')

            # If no brands were found, print a message and return the empty data list
            if not brand_elements:
                print(f"No brand elements found on {self.url}")
                return self.data

            # Loop through each brand element found
            for element in brand_elements:
                # Extract the title and href (relative or absolute link) of the brand
                title = await element.get_attribute('title')
                brand_link = await element.get_attribute('href')

                if brand_link:
                    # Construct the base URL (protocol + domain)
                    base_url = self.url.split('/', 3)[0] + '//' + self.url.split('/', 3)[2]
                    # Create the full URL if the brand_link is relative
                    full_brand_link = base_url + brand_link if brand_link.startswith('/') else brand_link

                    # Print the constructed brand link for debugging/logging
                    print(f"Full brand link: {full_brand_link}")

                    # Open a new tab to scrape card details for this specific brand
                    new_page = await browser.new_page()
                    await new_page.goto(full_brand_link)

                    # Use the DetailsScraping class to fetch card-specific data
                    details_scraper = DetailsScraping(full_brand_link)
                    card_details = await details_scraper.get_card_details()
                    await new_page.close()  # Close the tab after scraping is done

                    # Append structured data for this brand to the overall data list
                    self.data.append({
                        'brand_title': title,  # Name/title of the brand
                        'brand_link': full_brand_link.rsplit('/', 1)[0] + '/{}',  # Prepare pagination template
                        'available_cards': card_details,  # List of card details retrieved
                    })

                    # Print brand info for tracking
                    print(f"Found brand: {title}, Link: {full_brand_link}")

            # Close the browser after finishing all operations
            await browser.close()
        
        # Return the collected data
        return self.data
