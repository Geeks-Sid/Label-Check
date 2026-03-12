import argparse
import os

import pandas as pd
from bs4 import BeautifulSoup


def parse_eslide_table(file_path):
    """
    Parses an eSlideManager HTML file to extract the main data table.

    Args:
        file_path (str): The path to the HTML file.

    Returns:
        pandas.DataFrame: A DataFrame containing the extracted data,
                          or None if the table is not found.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            html_content = f.read()
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
        return None

    soup = BeautifulSoup(html_content, "lxml")

    # Find the main data table by its unique ID
    table = soup.find("table", id="SlideListTable")
    if not table:
        print("Error: Could not find the data table with id='SlideListTable'.")
        return None

    # --- Extract Headers ---
    # The actual header row is the last <tr> inside the <thead>
    header_row = table.find("thead").find_all("tr")[-1]

    # Extract text from each header cell (<th>), skipping the first one (checkbox)
    # Also, clean up the text by removing the sorting arrow '↓'
    headers = [
        th.get_text(strip=True).replace("↓", "").strip()
        for th in header_row.find_all("th")[1:]
    ]

    # --- Extract Data Rows ---
    all_rows_data = []
    # Find all table rows (<tr>) in the <tbody> with the class 'DataRow'
    # This correctly ignores the 'summary-row' elements
    data_rows = table.find("tbody").find_all("tr", class_="DataRow")

    for row in data_rows:
        # Extract text from each data cell (<td>), skipping the first one (controls)
        cells = row.find_all("td")[1:]
        row_data = [cell.get_text(strip=True) for cell in cells]

        # Ensure the row has the correct number of columns before adding
        if len(row_data) == len(headers):
            all_rows_data.append(row_data)

    # Create a pandas DataFrame for beautiful and structured output
    df = pd.DataFrame(all_rows_data, columns=headers)

    return df


# --- Main execution block ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse eSlideManager HTML table.")
    parser.add_argument(
        "--folder",
        type=str,
        help="Path to the folder containing the eSlideManager HTML file/s.",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="eslides.csv",
        help="Path to save the extracted CSV file.",
    )

    args = parser.parse_args()

    folder_path = args.folder
    output_path = args.output

    files = os.listdir(folder_path)
    html_files = [f for f in files if f.endswith(".html") or f.endswith(".htm")]

    for file_name in html_files:
        # Parse the file and get the DataFrame
        eslide_df = parse_eslide_table(os.path.join(folder_path, file_name))

        # You can now easily save this data to other formats, for example:
        eslide_df.to_csv(output_path, index=False)
        print(f"\n\nData successfully saved to {output_path}")
