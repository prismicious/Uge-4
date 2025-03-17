from models.Downloader import Downloader
from models.PDClient import PDClient
from utils.logger import logger

class Main():
    def __init__(self):
        self.parser = PDClient("GRI_2017_2020")
        self.downloader = Downloader(1000)
        
        
    def run(self):
        reports = self.parser.parse_excel_to_reports()
        self.downloader.set_reports(reports)
        self.downloader.download()
        
if __name__ == "__main__":
    logger.info("Starting PDF Report Downloader")
    main = Main()
    main.run()