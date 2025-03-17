import json
import os
import pandas as pd
from utils.logger import logger
from models.Report import PDFReport


class PDClient():
    def __init__(self, file_name="GRI_2017_2020", folder_path="data"):
        self.file_name = file_name
        self.folder_path = folder_path
        self.id = "BRnum"
        self.df = pd.read_excel(
            f"{self.folder_path}/{self.file_name}.xlsx", index_col=self.id)

    def parse_excel_to_reports(self):
        reports = []
        df = self.df

        for i in df.index:
            report = PDFReport(
                i, df.at[i, "Pdf_URL"], df.at[i, "Report Html Address"])
            reports.append(report)

        logger.info(
            f"Succesfully parsed all reports from {self.file_name}.xlsx")
        return reports

    def update_downloaded_status(self, brnum, downloaded):

        if downloaded:
            status = "Yes"
        else:
            status = "No"

        self.append_to_csv({
            "brnum": brnum,
            "downloaded": status
        })

    def append_to_csv(self, data):
        with open("downloaded_reports.csv", 'a') as f:
            f.write(f"{data['brnum']},{data['downloaded']}\n")
