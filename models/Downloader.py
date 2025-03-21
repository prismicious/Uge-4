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
from requests.exceptions import SSLError
import urllib3
import sys

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
        self.successful_downloads = 0
        self.failed_downloads = 0

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

        # Suppress InsecureRequestWarning warnings
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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

    def set_reports(self, pdf_reports: List[PDFReport]) -> None:
        self.pdf_reports = pdf_reports

        logger.info(f"Set {len(pdf_reports)} reports to download")

    def handle_download_exception(self, report: PDFReport, exception: Exception, result: str) -> None:
        """
        Handles exceptions during the download process by logging and updating the report status.

        Arguments:
            report (PDFReport): The report being downloaded.
            exception (Exception): The exception that occurred.
            result (str): The result to log (e.g., status code or exception type).
        """
        self.log_status_count(result)
        self.pdclient.update_status(report.brnum, False, result)
        self.failed_downloads += 1

    def download_report(self, report: PDFReport, verify_ssl: bool = True, retries_left: int = 3) -> None:
        """
        Downloads a single report with retry logic and exception handling.

        Arguments:
            report (PDFReport): The PDFReport object containing the report's URLs and metadata.
            verify_ssl (bool): Whether to verify SSL certificates. Default is True.
            retries_left (int): Number of retries left for the download. Default is 3.
        """
        urls_to_try = [report.pdf_url, report.backup_url] if report.backup_url else [
            report.pdf_url]

        for url in urls_to_try:
            response = None  # Initialize response to None
            try:
                # Use the session with retry logic
                with self.session.get(url, timeout=10, stream=True, verify=verify_ssl) as response:
                    response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)

                    # Check if the response is a PDF
                    if response.headers.get("Content-Type") != "application/pdf":
                        raise ValueError(
                            f"Unexpected Content-Type for {url}: {response.headers.get('Content-Type')}")

                    # Save the file
                    filename = os.path.join(
                        self.download_folder, f"{report.brnum}.pdf")
                    with open(filename, "wb") as f:
                        for chunk in response.iter_content(chunk_size=8124):
                            f.write(chunk)

                    if os.path.exists(filename) and os.path.getsize(filename) > 0:
                        self.pdclient.update_status(
                            report.brnum, True, response.status_code)
                        self.successful_downloads += 1
                        return

            except SSLError as e:
                # Retry with SSL verification turned off
                if verify_ssl:
                    self.download_report(
                        report, verify_ssl=False, retries_left=retries_left)
                    return
                else:
                    self.handle_download_exception(report, e, "SSL Error")

            except requests.HTTPError as e:
                # Check if response exists and handle accordingly
                if response is None:
                    status_code = "N/A"
                else:
                    status_code = response.status_code

                self.handle_download_exception(report, e, status_code)

            except ValueError as e:
                self.handle_download_exception(
                    report, e, "Invalid Content-Type")

            except Exception as e:
                exception_type = type(e).__name__
                self.handle_download_exception(report, e, exception_type)

        # Retry logic for transient errors
        if retries_left > 0:
            self.download_report(report, verify_ssl=verify_ssl,
                                 retries_left=retries_left - 1)

    def download(self) -> None:
        """
        Downloads all the reports using a thread pool for concurrency.

        This method uses the `concurrent.futures.ThreadPoolExecutor` to download multiple reports
        simultaneously. Progress is displayed using the `tqdm` progress bar.
        """
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            futures = [executor.submit(self.download_report, report)
                       for report in self.pdf_reports]

            # Create a single tqdm progress bar with a custom description
            with tqdm(total=len(futures), desc="Downloading PDFs", unit="pdf", file=sys.stderr) as progress_bar:
                for future in concurrent.futures.as_completed(futures):
                    try:
                        # Wait for the future to complete
                        future.result()
                    
                    except Exception as e:
                        logger.error(f"Failed to download report because {e}")
                    finally:
                        # Update the progress bar after each task completes
                        progress_bar.update(1)

        logger.info(f"Sucessful downloads: {self.successful_downloads} | Failed downloads: {self.failed_downloads}")