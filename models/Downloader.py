import csv
import os
from typing import List
import requests
import concurrent.futures
from models.PDClient import PDClient
from models.Report import PDFReport
from utils.logger import logger
from tqdm import tqdm
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging

from utils.utils import append_to_csv

urllib3_logger = logging.getLogger("urllib3")
urllib3_logger.setLevel(logging.ERROR)

class Downloader:
    """
    This class is responsible for downloading PDF reports from a list of URLs.

    Features:
        - Handles downloading of PDF reports using concurrency.
        - Supports retry logic for transient HTTP errors (e.g., 500, 502, 503, 504).
        - Logs HTTP status codes, exceptions, and the type of URL (primary or backup) to a CSV file.
        - Validates the content type of the downloaded files to ensure they are PDFs.
        - Suppresses retry warnings from `urllib3` to keep logs clean.

    Arguments:
        num_workers (int): The number of threads to use for downloading. Default is 5.
        log_file (str): The name of the CSV file to log status codes and exceptions. Default is "status_codes_count.csv".

    Attributes:
        num_workers (int): The number of threads for concurrent downloads.
        pdclient (PDClient): An instance of PDClient to update the status of the downloaded reports.
        log_file (str): The name of the CSV file to log status codes and exceptions.
        download_folder (str): The folder where downloaded reports are saved.
        output_folder (str): The folder where output files (e.g., logs) are saved.
        results (dict): A dictionary to track the count of HTTP status codes and exceptions.
        session (requests.Session): A session with retry logic for HTTP requests.
    """

    def __init__(self, num_workers=5, log_file="status_codes_count.csv"):
        self.num_workers = num_workers
        self.pdclient = PDClient()
        self.log_file = log_file
        self.download_folder = "downloads"
        self.output_folder = "output"
        self.results = {}

        # Create a session with retry logic
        self.session = requests.Session()

        # Retry on 500, 502, 503, 504 status codes

        retries = Retry(
            total=3,  # Total number of retries
            backoff_factor=1,  # Wait 1s, 2s, 4s, etc. between retries
            # Retry on these HTTP status codes
            status_forcelist=[500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def log_status_count(self, result: str | int) -> None:
        """
        Logs the HTTP status code and updates the status code count in a CSV file.

        Arguments:
            status_code (int or str): The HTTP status code to log. If "N/A", it indicates an unknown error.
        """
        if not isinstance(result, str):
            result = int(result)

        if result not in self.results:
            self.results[result] = 1

        else:
            self.results[result] += 1

        data = [
            {
                "Result": key,
                "Count": value,
            }
            for key, value in self.results.items()
        ]

        append_to_csv(f"{self.output_folder}/{self.log_file}", data, 'w')

    def set_reports(self, pdf_reports: List[PDFReport]):
        self.pdf_reports = pdf_reports

        logger.info(f"Set {len(pdf_reports)} reports to download")

    def download_report(self, report: PDFReport) -> None:
        """
        Downloads a single report.
        It tries the primary URL first, then the backup URL if available.
        If the download is successful, it updates the status in the CSV file.

        Arguments:
            report (PDFReport): The PDFReport object containing the report's URLs and metadata.

        Raises:
            requests.HTTPError: If the HTTP request fails.
            ValueError: If the response content type is not "application/pdf".
        """
        urls_to_try = [report.pdf_url, report.backup_url] if report.backup_url else [
            report.pdf_url]

        for url in urls_to_try:
            try:
                response = None
                # Use the session with retry logic
                with self.session.get(url, timeout=10, stream=True) as response:
                    response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)

                    # Check if the response is a PDF
                    if response.headers.get("Content-Type") != "application/pdf":
                        raise ValueError(
                            f"Unexpected Content-Type for {url}: {response.headers.get('Content-Type')}")

                    # Save the file
                    filename = os.path.join(
                        self.download_folder, f"{report.brnum}.pdf")
                    with open(filename, "wb") as f:
                        for chunk in response.iter_content(chunk_size=1024):
                            f.write(chunk)
                            break

                    if os.path.exists(filename) and os.path.getsize(filename) > 0:
                        self.pdclient.update_status(
                            report.brnum, True, response.status_code)
                        return

            except Exception as e:
                if not response:
                    exception_type = type(e).__name__
                    self.log_status_count(exception_type)
                
                else:
                    self.log_status_count(response.status_code)
                    self.pdclient.update_status(report.brnum, False, "N/A")

    def download(self):
        """
        Downloads all the reports using a thread pool for concurrency.

        This method uses the `concurrent.futures.ThreadPoolExecutor` to download multiple reports
        simultaneously. Progress is displayed using the `tqdm` progress bar.
        """
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            futures = [executor.submit(self.download_report, report)
                       for report in self.pdf_reports]

            # Wrap the futures list with tqdm for consistent progress updates
            for future in tqdm(futures, total=len(futures)):
                try:
                    # Wait for the future to complete
                    future.result()
                except Exception as e:
                    logger.error(f"Failed to download report because {e}")
