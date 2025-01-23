import asyncio
import pandas as pd
import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
from pathlib import Path
from DetailsScraper import DetailsScraping
from SavingOnDriveContracting import SavingOnDriveContracting


class ContractingMainScraper:
    def __init__(self, contractingANDservices_data: Dict[str, List[Tuple[str, int]]]):
        self.contractingANDservices_data = contractingANDservices_data
        self.chunk_size = 2  # Number of categories processed per chunk
        self.max_concurrent_links = 2  # Max links processed simultaneously
        self.logger = logging.getLogger(__name__)
        self.setup_logging()
        self.temp_dir = Path("temp_files")
        self.temp_dir.mkdir(exist_ok=True)
        self.upload_retries = 3
        self.upload_retry_delay = 15  # Retry delay in seconds
        self.page_delay = 3  # Delay between page requests
        self.chunk_delay = 10  # Delay between chunks

    # def setup_logging(self):
    #     """Initialize logging configuration."""
    #     logging.basicConfig(
    #         level=logging.INFO,
    #         format="%(asctime)s - %(levelname)s - %(message)s",
    #         handlers=[
    #             logging.StreamHandler(),
    #             logging.FileHandler("scraper.log"),
    #         ],
    #     )
    #     self.logger.setLevel(logging.INFO)

    def setup_logging(self):
        """Initialize logging configuration."""
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.INFO)
        stream_handler.flush = True

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                stream_handler,
                logging.FileHandler("scraper.log"),
            ],
        )
        self.logger.setLevel(logging.INFO)
        print("Logging setup complete.")

    
    async def scrape_contractingANDservice(self, contractingANDservice_name: str, urls: List[Tuple[str, int]], semaphore: asyncio.Semaphore) -> List[Dict]:
        """Scrape data for a single category."""
        self.logger.info(f"Starting to scrape {contractingANDservice_name}")
        card_data = []
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        async with semaphore:
            for url_template, page_count in urls:
                for page in range(1, page_count + 1):
                    url = url_template.format(page)
                    scraper = DetailsScraping(url)
                    try:
                        cards = await scraper.get_card_details()
                        for card in cards:
                            if card.get("date_published") and card.get("date_published", "").split()[0] == yesterday:
                                card_data.append(card)

                        await asyncio.sleep(self.page_delay)
                    except Exception as e:
                        self.logger.error(f"Error scraping {url}: {e}")
                        continue

        return card_data

    async def save_to_excel(self, contractingANDservice_name: str, card_data: List[Dict]) -> str:
        """Save data to an Excel file."""
        if not card_data:
            self.logger.info(f"No data to save for {contractingANDservice_name}, skipping Excel file creation.")
            return None

        excel_file = Path(f"{contractingANDservice_name}.xlsx")

        try:
            df = pd.DataFrame(card_data)
            df.to_excel(excel_file, index=False)
            self.logger.info(f"Successfully saved data for {contractingANDservice_name}")
            return str(excel_file)
        except Exception as e:
            self.logger.error(f"Error saving Excel file {excel_file}: {e}")
            return None

    async def upload_files_with_retry(self, drive_saver, files: List[str]) -> List[str]:
        """Upload files to Google Drive with retry mechanism."""
        uploaded_files = []
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        try:
            folder_id = drive_saver.get_folder_id(yesterday)
            if not folder_id:
                folder_id = drive_saver.create_folder(yesterday)
                self.logger.info(f"Created new folder '{yesterday}'")

            for file in files:
                for attempt in range(self.upload_retries):
                    try:
                        if os.path.exists(file):
                            drive_saver.upload_file(file, folder_id)
                            uploaded_files.append(file)
                            self.logger.info(f"Successfully uploaded {file} to Google Drive folder '{yesterday}'")
                            break
                    except Exception as e:
                        self.logger.error(f"Upload attempt {attempt + 1} failed for {file}: {e}")
                        if attempt < self.upload_retries - 1:
                            await asyncio.sleep(self.upload_retry_delay)
                            drive_saver.authenticate()
                        else:
                            self.logger.error(f"Failed to upload {file} after {self.upload_retries} attempts")

        except Exception as e:
            self.logger.error(f"Error managing Google Drive folder for {yesterday}: {e}")

        return uploaded_files

    async def scrape_all_contractingANDservices(self):
        """Scrape all categories in chunks."""
        self.temp_dir.mkdir(exist_ok=True)

        # Setup Google Drive
        try:
            credentials_json = os.environ.get("CONTRACTING_GCLOUD_KEY_JSON")
            if not credentials_json:
                raise EnvironmentError("CONTRACTING_GCLOUD_KEY_JSON environment variable not found")
            else:
                print("Environment variable CONTRACTING_GCLOUD_KEY_JSON is set.")

            credentials_dict = json.loads(credentials_json)
            drive_saver = SavingOnDriveContracting(credentials_dict)
            drive_saver.authenticate()
            drive_saver.list_files()
        except Exception as e:
            self.logger.error(f"Failed to setup Google Drive: {e}")
            return

        contractingANDservices_chunks = [
            list(self.contractingANDservices_data.items())[i : i + self.chunk_size]
            for i in range(0, len(self.contractingANDservices_data), self.chunk_size)
        ]

        semaphore = asyncio.Semaphore(self.max_concurrent_links)

        for chunk_index, chunk in enumerate(contractingANDservices_chunks, 1):
            self.logger.info(f"Processing chunk {chunk_index}/{len(contractingANDservices_chunks)}")

            tasks = []
            for contractingANDservice_name, urls in chunk:
                task = asyncio.create_task(self.scrape_contractingANDservice(contractingANDservice_name, urls, semaphore))
                tasks.append((contractingANDservice_name, task))
                await asyncio.sleep(2)

            pending_uploads = []
            for contractingANDservice_name, task in tasks:
                try:
                    card_data = await task
                    if card_data:
                        excel_file = await self.save_to_excel(contractingANDservice_name, card_data)
                        if excel_file:
                            pending_uploads.append(excel_file)
                except Exception as e:
                    self.logger.error(f"Error processing {contractingANDservice_name}: {e}")

            if pending_uploads:
                await self.upload_files_with_retry(drive_saver, pending_uploads)

                for file in pending_uploads:
                    try:
                        os.remove(file)
                        self.logger.info(f"Cleaned up local file: {file}")
                    except Exception as e:
                        self.logger.error(f"Error cleaning up {file}: {e}")

            if chunk_index < len(contractingANDservices_chunks):
                self.logger.info(f"Waiting {self.chunk_delay} seconds before next chunk...")
                await asyncio.sleep(self.chunk_delay)


if __name__ == "__main__":
    contractingANDservices_data = {
        "مكافحة الحشرات": [("https://www.q84sale.com/ar/contracting/bugs-exterminator/{}", 1)],
        "مقاول صحى": [("https://www.q84sale.com/ar/contracting/plumber/{}", 4)],
        "الأقفال": [("https://www.q84sale.com/ar/contracting/locksmith/{}", 2)],
        "تسليك مجارى": [("https://www.q84sale.com/ar/contracting/duct-cleaning/{}", 2)],
        "التكييف": [("https://www.q84sale.com/ar/contracting/ac-services/{}", 1)],
        "أصباغ": [("https://www.q84sale.com/ar/contracting/painter/{}", 3)],
        "أعمال الديكور": [("https://www.q84sale.com/ar/contracting/decoration/{}", 4)],
        "مشاتل و حدائق": [("https://www.q84sale.com/ar/contracting/gardener/{}", 2)],
        "صيانة أجهزة منزلية": [("https://www.q84sale.com/ar/contracting/home-appliances-maintenance/{}", 3)],
        "مقاول كهرباء": [("https://www.q84sale.com/ar/contracting/electrician/{}", 3)],
        "نجار": [("https://www.q84sale.com/ar/contracting/carpenter/{}", 3)],
        "حدادة": [("https://www.q84sale.com/ar/contracting/metalwork/{}", 4)],
        "كاشي و سيراميك": [("https://www.q84sale.com/ar/contracting/ceramic/{}", 3)],
        "عازل": [("https://www.q84sale.com/ar/contracting/insulated-roof/{}", 1)],
        "ألمنيوم": [("https://www.q84sale.com/ar/contracting/aluminum-2667/{}", 4)],
        "مقاولات بناء": [("https://www.q84sale.com/ar/contracting/builders/{}", 3)],
        "فنى زجاج": [("https://www.q84sale.com/ar/contracting/glass/{}", 1)],
        "الأبواب": [("https://www.q84sale.com/ar/contracting/doors/{}", 1)],
        "مصاعد": [("https://www.q84sale.com/ar/contracting/elevators/{}", 1)],
        "أعمال التهوية": [("https://www.q84sale.com/ar/contracting/ventilation-works/{}", 1)],
        "خزانات مياه": [("https://www.q84sale.com/ar/contracting/water-tanks/{}", 1)],
        "منتجات زراعية": [("https://www.q84sale.com/ar/contracting/agricultural-products/{}", 1)],
        "مواد بناء": [("https://www.q84sale.com/ar/contracting/building-materials/{}", 1)],
    }

    async def main():
        scraper = ContractingMainScraper(contractingANDservices_data)
        await scraper.scrape_all_contractingANDservices()

    # Run everything in the async event loop
    asyncio.run(main())
