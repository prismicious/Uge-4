from urllib.parse import urlparse


class PDFReport:
    def __init__(self, brnum: str, pdf_url: str, backup_url: str):
        self.brnum = brnum  # The BRnum of the report
        self.pdf_url = pdf_url  # The URL of the PDF report
        # The backup URL of the PDF report
        self.backup_url = None if "nan" in str(backup_url) else backup_url
        self.validate()
        self.downloaded = False  # Is the report downloaded?

    def validate(self):
        if not self.is_valid_url(self.pdf_url):
            if self.backup_url and self.is_valid_url(self.backup_url):
                self.pdf_url = self.backup_url

    def is_valid_url(self, url):
        # Check if url is missing http
        if "http" not in url:
            self.pdf_url = f"http://{self.url}"

        parsed = urlparse(self.pdf_url)
        return bool(parsed.netloc)
