from urllib.parse import urlparse


class PDFReport:
    """
    Represents a PDF report with a primary and backup URL. 
    Provides functionality to validate and manage the report's URLs.
    """

    def __init__(self, brnum: str, pdf_url: str, backup_url: str):
        """
        Initializes a PDFReport instance.

        Args:
            brnum (str): The BRnum of the report.
            pdf_url (str): The primary URL of the PDF report.
            backup_url (str): The backup URL of the PDF report. If "nan" is present, it is set to None.
        """
        self.brnum = brnum  # The BRnum of the report
        self.pdf_url = pdf_url  # The URL of the PDF report
        # The backup URL of the PDF report
        self.backup_url = None if "nan" in str(backup_url) else backup_url
        self.validate()
        self.downloaded = False  # Is the report downloaded?

    def validate(self):
        """
        Validates the primary URL of the PDF report. 
        If the primary URL is invalid and a valid backup URL exists, 
        the primary URL is replaced with the backup URL.
        """
        if not self.is_valid_url(self.pdf_url):
            if self.backup_url and self.is_valid_url(self.backup_url):
                self.pdf_url = self.backup_url

    def is_valid_url(self, url):
        """
        Checks if a given URL is valid. If the URL is missing the "http" scheme, 
        it attempts to prepend "http://".

        Args:
            url (str): The URL to validate.

        Returns:
            bool: True if the URL is valid, False otherwise.
        """
        # Check if url is missing http
        if "http" not in url:
            self.pdf_url = f"http://{self.url}"

        parsed = urlparse(self.pdf_url)
        return bool(parsed.netloc)
