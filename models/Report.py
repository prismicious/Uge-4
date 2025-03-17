class PDFReport:
    def __init__(self, brnum: str, pdf_url: str, backup_url: str):
        self.downloaded = False
        self.brnum = brnum
        self.pdf_url = pdf_url
        self.backup_url = backup_url

