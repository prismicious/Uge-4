import os
import csv
from typing import Dict, List, Union

def append_to_csv(file_path: str, data: Union[Dict, List[Dict]], mode='a') -> None:
    """
    Appends data to a CSV file. If the file is empty or in 'w' mode, it writes the headers.

    Args:
        file_path (str): Path to the CSV file.
        data (Union[Dict, List[Dict]]): Data to append. Can be a single dictionary or a list of dictionaries.
        mode (str): File mode ('a' for append, 'w' for overwrite). Default is 'a'.
    """
    # Determine if headers need to be written
    write_headers = False
    if mode == 'w':
        # In 'w' mode, always write headers because the file is being overwritten
        write_headers = True
    else:
        # In 'a' mode, check if the file is empty
        try:
            with open(file_path, 'r') as f:
                write_headers = f.readline().strip() == ""
        except FileNotFoundError:
            # If the file doesn't exist, treat it as empty
            write_headers = True

    # Open the file in the specified mode
    with open(file_path, mode, newline='') as f:
        writer = csv.writer(f)

        # Write the headers if needed
        if write_headers:
            if isinstance(data, list) and len(data) > 0:
                writer.writerow(data[0].keys())  # Use keys from the first dictionary as headers
            elif isinstance(data, dict):
                writer.writerow(data.keys())  # Use dictionary keys as headers

        # Write the data rows
        if isinstance(data, list):
            for row in data:
                writer.writerow(row.values())  # Write each dictionary's values as a row
        elif isinstance(data, dict):
            writer.writerow(data.values())  # Write the single dictionary's values as a row
        else:
            raise ValueError("Data must be a dictionary or a list of dictionaries.")
        
def create_folder_if_not_exists(folder_name: str) -> None:
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)