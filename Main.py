from models.Downloader import Downloader
from models.PDClient import PDClient
from utils.logger import logger
from utils.utils import create_folder_if_not_exists

class Main:
    """
    This program downloads PDF reports from a list of URLs provided in an Excel file.

    Features:
        - Automatically creates necessary folders ("downloads" and "output") if they don't exist.
        - Parses an Excel file to extract report URLs and associated metadata.
        - Attempts to download reports using the primary URL first; falls back to a backup URL if the primary fails.
        - Utilizes concurrency to download multiple reports simultaneously for improved efficiency.
        - Logs the progress, success, and failure of each download operation.

    Attributes:
        parser (PDClient): Responsible for parsing the Excel file and extracting report data.
        downloader (Downloader): Manages the downloading of reports with support for concurrency.

    Methods:
        run(): Orchestrates the program by creating folders, parsing the Excel file, and initiating downloads.
    """
    def __init__(self):
        self.parser = PDClient("GRI_2017_2020")
        self.downloader = Downloader(num_workers=100)
        
        
    def run(self):
        for folder in ["downloads", "output"]:
            create_folder_if_not_exists(folder)
        
        reports = self.parser.parse_excel_to_reports()
        self.downloader.set_reports(reports)
        self.downloader.download()
        
if __name__ == "__main__":
    logger.info("Starting PDF Report Downloader")
    main = Main()
    main.run()