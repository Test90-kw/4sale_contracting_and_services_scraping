# Required libraries for async operations, scraping, data handling, and Google Drive API.
import asyncio
import pandas as pd
import os
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from playwright.async_api import async_playwright
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import socket
import ssl
from DetailsScraper import DetailsScraping  # Custom scraper module


class MedicalServices:
    def __init__(self, credentials_dict, url, num_pages=1, specific_brands=None, specific_pages=None):
        # Google Drive credentials and setup
        self.credentials_dict = credentials_dict
        self.scopes = ['https://www.googleapis.com/auth/drive']
        self.service = None
        self.parent_folder_id = '1dwoFxJ4F56HIfaUrRk3QufXDE1QlotzA'  # Parent folder on Google Drive

        # Scraping setup
        self.url = url
        self.num_pages = num_pages  # Default pages to scrape per brand
        self.specific_brands = specific_brands or []  # Brands requiring special treatment
        self.specific_pages = specific_pages if specific_pages else num_pages
        self.data = []  # Will hold scraped data

        # General scraper settings
        self.chunk_size = 2
        self.max_concurrent_links = 2
        self.logger = logging.getLogger(__name__)
        self.setup_logging()  # Setup logging to file and console
        self.temp_dir = Path("temp_files")
        self.temp_dir.mkdir(exist_ok=True)
        self.upload_retries = 3
        self.upload_retry_delay = 15
        self.page_delay = 3
        self.chunk_delay = 10

    def setup_logging(self):
        # Configure logging to show output in console and save to file
        stream_handler = logging.StreamHandler()
        file_handler = logging.FileHandler("scraper.log")
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[stream_handler, file_handler]
        )
        self.logger.setLevel(logging.INFO)

    def authenticate(self):
        # Authenticate to Google Drive using the service account
        try:
            creds = Credentials.from_service_account_info(self.credentials_dict, scopes=self.scopes)
            self.service = build('drive', 'v3', credentials=creds)
        except Exception as e:
            self.logger.error(f"Authentication error: {e}")
            raise

    # Retry up to 3 times for network-related issues
    @retry(stop=stop_after_attempt(3), 
           wait=wait_exponential(multiplier=1, min=4, max=10),
           retry=retry_if_exception_type((socket.error, ssl.SSLError, ConnectionError)))
    def get_folder_id(self, folder_name):
        # Search for a folder by name under the parent folder
        try:
            query = (f"name='{folder_name}' and "
                    f"'{self.parent_folder_id}' in parents and "
                    f"mimeType='application/vnd.google-apps.folder' and "
                    f"trashed=false")
            results = self.service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
            files = results.get('files', [])
            if files:
                self.logger.info(f"Found folder: {folder_name}")
                return files[0]['id']
            return None
        except Exception as e:
            self.logger.error(f"Error getting folder ID: {e}")
            raise

    # Retry folder creation for network errors
    @retry(stop=stop_after_attempt(3),
           wait=wait_exponential(multiplier=1, min=4, max=10),
           retry=retry_if_exception_type((socket.error, ssl.SSLError, ConnectionError)))
    def create_folder(self, folder_name):
        # Create a new folder on Google Drive inside the parent folder
        try:
            file_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [self.parent_folder_id]
            }
            folder = self.service.files().create(body=file_metadata, fields='id, name').execute()
            self.logger.info(f"Created folder: {folder_name}")
            return folder.get('id')
        except Exception as e:
            self.logger.error(f"Error creating folder: {e}")
            raise

    async def scrape_brands_and_types(self):
        # Launch Playwright browser and navigate to the main URL
        # Scrape all brand links and titles
        # For each brand:
        #     - Build paginated URLs
        #     - Scrape data using DetailsScraping class
        #     - Collect only cards if present
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            await page.goto(self.url)
            
            brand_elements = await page.query_selector_all('.styles_itemWrapper__MTzPB a')
            if not brand_elements:
                self.logger.info(f"No brand elements found on {self.url}")
                return self.data
                
            for element in brand_elements:
                title = await element.get_attribute('title')
                brand_link = await element.get_attribute('href')
                
                if brand_link:
                    base_url = self.url.split('/', 3)[0] + '//' + self.url.split('/', 3)[2]
                    full_brand_link = base_url + brand_link if brand_link.startswith('/') else brand_link
                    pages_to_scrape = self.specific_pages if title in self.specific_brands else self.num_pages
                    
                    brand_data = []
                    for page_num in range(1, pages_to_scrape + 1):
                        paginated_link = f"{full_brand_link}/{page_num}"
                        try:
                            details_scraper = DetailsScraping(paginated_link)
                            card_details = await details_scraper.get_card_details()
                            if card_details:
                                brand_data.extend(card_details)
                            else:
                                break
                        except Exception as e:
                            self.logger.error(f"Error scraping {paginated_link}: {e}")
                            break

                    self.data.append({
                        'brand_title': title,
                        'brand_link': full_brand_link.rsplit('/', 1)[0] + '/{}',
                        'available_cars': brand_data
                    })
                    
            await browser.close()
            return self.data

    async def save_to_excel(self, category_name: str, brand_data: list) -> str:
        # Create an Excel workbook where each brand has its own sheet
        # Only include cards that were published "yesterday"
        # Return the path to the saved Excel file
        if not brand_data:
            self.logger.info(f"No data to save for {category_name}")
            return None

        excel_file = Path(f"{category_name}.xlsx")
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        
        try:
            with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
                sheets_created = False
                
                for brand in brand_data:
                    brand_title = brand['brand_title']
                    cars = brand['available_cars']
                    
                    yesterday_cars = [
                        car for car in cars 
                        if car.get('date_published') and car['date_published'].split()[0] == yesterday
                    ]
                    
                    if yesterday_cars:
                        df = pd.DataFrame(yesterday_cars)
                        sheet_name = "".join(x for x in brand_title if x.isalnum())[:31]
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                        sheets_created = True
                        self.logger.info(f"Created sheet for {brand_title} with {len(yesterday_cars)} entries")
                
                if not sheets_created:
                    self.logger.info("No data from yesterday found for any brand")
                    return None
                
            self.logger.info(f"Successfully saved data for {category_name}")
            return str(excel_file)
        except Exception as e:
            self.logger.error(f"Error saving Excel file {excel_file}: {e}")
            return None

    # Upload file to Google Drive with retry logic
    @retry(stop=stop_after_attempt(3),
           wait=wait_exponential(multiplier=1, min=4, max=10),
           retry=retry_if_exception_type((socket.error, ssl.SSLError, ConnectionError)))
    def upload_file(self, file_name: str, folder_id: str) -> str:
        # Upload a local file to the specified Drive folder
        try:
            if not os.path.exists(file_name):
                raise FileNotFoundError(f"Local file not found: {file_name}")

            file_metadata = {
                'name': os.path.basename(file_name),
                'parents': [folder_id]
            }
            media = MediaFileUpload(file_name, resumable=True)
            file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()
            return file.get('id')
        except Exception as e:
            self.logger.error(f"Error uploading file {file_name}: {e}")
            raise

    async def process_medical_services(self):
        # Main function to handle scraping and uploading
        # 1. Authenticate
        # 2. Create/Get folder for yesterday
        # 3. Scrape brand/type data
        # 4. Save Excel if there are results from yesterday
        # 5. Upload file to Drive
        # 6. Clean up local file
        self.temp_dir.mkdir(exist_ok=True)
        try:
            self.authenticate()
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            
            folder_id = self.get_folder_id(yesterday)
            if not folder_id:
                folder_id = self.create_folder(yesterday)
                self.logger.info(f"Created new folder '{yesterday}'")

            brand_data = await self.scrape_brands_and_types()
            if brand_data:
                excel_file = await self.save_to_excel("خدمات طبية", brand_data)
                if excel_file:
                    file_id = self.upload_file(excel_file, folder_id)
                    self.logger.info(f"Successfully uploaded file with ID: {file_id}")
                    os.remove(excel_file)
                    self.logger.info(f"Cleaned up local file: {excel_file}")

        except Exception as e:
            self.logger.error(f"Error in process_medical_services: {e}")
            raise

if __name__ == "__main__":
    # Load credentials from environment variable
    credentials_json = os.environ.get("SERVICES_GCLOUD_KEY_JSON")
    if not credentials_json:
        raise EnvironmentError("SERVICES_GCLOUD_KEY_JSON environment variable not found")

    # Parse credentials and initialize MedicalServices
    credentials_dict = json.loads(credentials_json)
    medical_services = MedicalServices(
        credentials_dict=credentials_dict,
        url="https://www.q84sale.com/ar/services/medical-services",
        num_pages=1,
        specific_brands=["تمريض"],
        specific_pages=2
    )

    # Run the complete process
    asyncio.run(medical_services.process_medical_services())
