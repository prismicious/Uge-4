import json
import os
from typing import List
import pandas as pd
from utils.logger import logger
from models.Report import PDFReport

from utils.utils import append_to_csv


class PDClient:
    """
    PDClient is responsible for parsing an Excel file to extract report data and managing the status of downloads.

    Features:
        - Parses an Excel file to create `PDFReport` objects containing report metadata.
        - Updates the status of reports (e.g., whether they were successfully downloaded).
        - Appends status updates to a CSV file for tracking.

    Arguments:
        file_name (str): The name of the Excel file to parse. Default is "GRI_2017_2020".
        folder_path (str): The folder path where the Excel file is located. Default is "data".

    Attributes:
        file_name (str): The name of the Excel file.
        folder_path (str): The folder path of the Excel file.
        csv_file (str): The name of the CSV file for logging download statuses.
        output_folder (str): The folder where output files (e.g., logs) are saved.
        id (str): The column name in the Excel file used as the unique identifier (default is "BRnum").
        df (pd.DataFrame): The DataFrame representation of the Excel file, indexed by `id`.

    Methods:
        parse_excel_to_reports(): Parses the Excel file and returns a list of `PDFReport` objects.
        update_status(brnum, downloaded, status_code): Updates the download status of a report and logs it to a CSV file.
    """
    def __init__(self, file_name="GRI_2017_2020", folder_path="data"):
        self.file_name = file_name
        self.folder_path = folder_path
        self.csv_file = "downloaded_reports.csv"
        self.output_folder = "output"
        self.id = "BRnum"
        self.df = pd.read_excel(
            f"{self.folder_path}/{self.file_name}.xlsx", index_col=self.id)

    def parse_excel_to_reports(self) -> List[PDFReport]:
        reports = []
        df = self.df

        for i in df.index:
            try:
                report = PDFReport(
                    i, df.at[i, "Pdf_URL"], df.at[i, "Report Html Address"])
                reports.append(report)
            
            except Exception as e:
                continue

        logger.info(
            f"Succesfully parsed all reports from {self.file_name}.xlsx")
        return reports

    def update_status(self, brnum, downloaded, status_code) -> None:
        # Convert the boolean `downloaded` to "Yes" or "No"
        status = "Yes" if downloaded else "No"

        # Append the ReportStatus object to the CSV
        
        dict = {
            "BRnum": brnum,
            "Downloaded": status,
            "Status Code": status_code
        }
        append_to_csv(f"{self.output_folder}/{self.csv_file}", dict)
