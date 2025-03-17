import os
from typing import List
import requests
import concurrent.futures
from models.PDClient import PDClient
from models.Report import PDFReport
from utils.logger import logger
from tqdm import tqdm

class Downloader():
    def __init__(self, num_workers=5):
        self.num_workers = num_workers
        self.download_folder = "downloads"
        self.pdclient = PDClient()

        if not os.path.exists(self.download_folder):
            os.makedirs(self.download_folder)

    def set_reports(self, pdf_reports: List[PDFReport]):
        self.pdf_reports = pdf_reports

        logger.info(f"Set {len(pdf_reports)} reports to download")

    def download_report(self, report):
        urls_to_try = [report.pdf_url, report.backup_url] if report.backup_url else [
            report.pdf_url]
        for url in urls_to_try:
            try:
                with requests.get(url, timeout=10, stream=True) as response:
                    response.raise_for_status()
                    filename = os.path.join(
                        self.download_folder, f"{report.brnum}.pdf")
                    with open(filename, "wb") as f:
                        for chunk in response.iter_content(chunk_size=1):
                            f.write(chunk)
                            break
                            
                # If download is successful, update the status in the excel file
                if os.path.exists(filename) and os.path.getsize(filename) > 0:
                    self.pdclient.update_downloaded_status(report.brnum, True)
                    return
                
            except Exception as e:
                self.pdclient.update_downloaded_status(report.brnum, False)
                return

    def download(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            futures = [executor.submit(self.download_report, report) for report in self.pdf_reports]

            # Wrap the futures list with tqdm for consistent progress updates
            for future in tqdm(futures, total=len(futures)):
                try:
                    # Wait for the future to complete
                    future.result()
                except Exception as e:
                    logger.error(f"Failed to download report because {e}")
