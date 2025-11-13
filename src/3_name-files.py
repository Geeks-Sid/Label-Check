"""
This script is the third and final step in a data processing pipeline for whole-slide images (WSIs).
It takes a CSV file enriched with OCR text (from the second script) and performs the final
data extraction and normalization.

The primary goals of this script are:
1.  **Parse OCR Text:** Use regular expressions (regex) to find and extract critical information,
    specifically the Accession ID and the Stain type, from the combined OCR text of the label
    and macro images.
2.  **Normalize Data:** Standardize the extracted data. Accession IDs are formatted consistently
    (e.g., 'NP 22-950' becomes 'NP22-950'), and various OCR misreadings of stain names
    (e.g., "H and E", "H+E") are mapped to a single canonical name ("H&E").
3.  **Enrich the CSV:** Append the parsed and normalized data as new columns to the CSV.
    It also adds flags indicating whether the parsing was successful and a column for
    manual quality control in a subsequent review tool.
4.  **Process Efficiently:** The script uses a thread pool for parallel processing, making it
    fast even with a large number of rows.

The final output is a clean, structured CSV file ready for use in a database or a manual
review application.
"""

# -----------------------------------------------------------------------------
# 1. IMPORTS
# -----------------------------------------------------------------------------
import argparse
import csv
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Tuple

from tqdm import tqdm  # For displaying a progress bar

# -----------------------------------------------------------------------------
# 2. Logging Configuration
# -----------------------------------------------------------------------------
# Set up a logger for informative console output with timestamps.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# 3. Configuration & Constants
# -----------------------------------------------------------------------------
# Define the names for the new columns that will be added to the output CSV.
COL_ACCESSION_ID = "AccessionID"
COL_STAIN = "Stain"
COL_EXTRACTION_SUCCESSFUL = "ExtractionSuccessful"
# This column is added for compatibility with the manual review tool. It starts empty.
COL_QC_PASSED = "ParsingQCPassed"

# A comprehensive dictionary to correct common OCR errors and variations for stain names.
# The key is the "canonical" (standard) name, and the value is a list of all known
# variations that should be mapped to it. This can be easily extended or moved to an
# external config file (like JSON) for easier management.
STAIN_NAME_CORRECTIONS = {
    "H&E": ["H and E", "H+E", "H-E", "HBE", "H8E", "#&E", "HnE", "H8E", "HnBE", "H&E"],
    "TPREP": ["T-PREP", "TPREP", "T PREP", "TP-REP", "TPREP."],
    "IDH": ["IDH1", "IDH-1", "IDHl", "lDH", "IDH.", "IDH"],
    "ATRX": ["ATR-X", "ATRX", "ATR X", "ATRX.", "AT-RX"],
    "1P19Q": ["1P/19Q", "1P-19Q", "1P 19Q", "1P19-Q", "1P19Q.", "1P19Q"],
    "P53": ["P-53", "P 53", "P53.", "P5-3", "P-5-3", "P53"],
    "KI67": ["KI-67", "KI 67", "KI67.", "K167", "KI-6-7", "KI67"],
    "OLIG2": ["OLIG-2", "OLIG 2", "OLIG2.", "OL1G2", "OLIG-2.", "OLIG2"],
    "EGFR": ["E-GFR", "EGFR", "EGFR.", "E GFR", "EG-FR"],
    "MGMT": ["M-GMT", "MGMT", "MGMT.", "MGM-T", "M-G-MT"],
    "H3K27M": ["H3-K27M", "H3 K27M", "H3K27M.", "H3K-27M", "H3-K-27M", "H3K27M"],
    "GFAP": ["G-FAP", "GFAP", "GFAP.", "GFA-P", "G-F-AP"],
    "CD34": ["CD-34", "CD 34", "CD34.", "CD3-4", "C-D34", "CD34"],
    "CD68": ["CD-68", "CD 68", "CD68.", "C-D68", "C-D-68", "CD68"],
    "CD3": ["CD-3", "CD 3", "CD3.", "C-D3", "C-D-3", "CD3"],
    "CD20": ["CD-20", "CD 20", "CD20.", "C-D20", "C-D-20", "CD20"],
    "BRCA1": ["BRCA-1", "BRCA 1", "BRCA1.", "B-RCA1", "B-RCA-1", "BRCA1"],
    "HER2": ["HER-2", "HER 2", "HER2.", "H-ER2", "H-ER-2", "HER2"],
    "PTEN": ["P-TEN", "PTEN", "PTEN.", "P-T-EN", "P-T-EN."],
    "FS1": ["FS-1", "FS 1", "FS1.", "F-S1", "F-S-1", "FS1"],
    "TP53": ["TP-53", "TP 53", "TP53.", "T-P53", "T-P-53", "TP53"],
    "CD45": ["CD-45", "CD 45", "CD45.", "C-D45", "C-D-45", "CD45"],
    "CD8": ["CD-8", "CD 8", "CD8.", "C-D8", "C-D-8", "CD8"],
    "CD4": ["CD-4", "CD 4", "CD4.", "C-D4", "C-D-4", "CD4"],
    "CD56": ["CD-56", "CD 56", "CD56.", "C-D56", "C-D-56", "CD56"],
    "KRAS": ["K-RAS", "KRAS", "KRAS.", "K-RAS.", "K-RAS"],
    "NRAS": ["N-RAS", "NRAS", "NRAS.", "N-RAS.", "N-RAS"],
    "BRAF": ["B-RAF", "BRAF", "BRAF.", "B-RAF.", "B-RAF"],
    "CTNNB1": ["CTNNB-1", "CTNNB 1", "CTNNB1.", "C-TNNB1", "C-TNNB-1", "CTNNB1"],
    "ALK": ["A-LK", "ALK", "ALK.", "A-LK."],
}


# -----------------------------------------------------------------------------
# 4. Core Functions
# -----------------------------------------------------------------------------
def build_stain_normalizer(
    corrections: Dict[str, List[str]],
) -> Tuple[re.Pattern, Dict[str, str]]:
    """
    Builds a regex pattern and a lookup map for efficient stain name normalization.

    This function preprocesses the `STAIN_NAME_CORRECTIONS` dictionary to create two
    optimized data structures:
    1. A compiled regex pattern that can find any of the known stain variations in text.
    2. A lookup dictionary that maps any lowercase variation directly to its canonical form.

    Args:
        corrections (Dict[str, List[str]]): The dictionary of stain name corrections.

    Returns:
        Tuple[re.Pattern, Dict[str, str]]: A tuple containing the compiled regex
                                           pattern and the variation-to-canonical lookup map.
    """
    variation_lookup = {}
    all_variations = set()

    # Create a reverse mapping from any variation to its canonical name.
    for canonical, variations in corrections.items():
        # Use a set for efficient addition and to handle duplicates.
        current_variations = {v.lower() for v in variations}
        current_variations.add(canonical.lower())  # Add the canonical name itself as a variation.
        all_variations.update(current_variations)
        for var in current_variations:
            variation_lookup[var] = canonical

    # Sort variations by length in descending order. This is a crucial optimization for the
    # regex. It ensures that longer matches (e.g., "H3 K27M") are found before shorter
    # substrings (e.g., "H3"), preventing incorrect partial matches.
    sorted_variations = sorted(list(all_variations), key=len, reverse=True)

    # Join all variations into a single regex "OR" pattern (e.g., 'h-e|h\+e|h&e').
    # re.escape() is used to handle special characters like '+' correctly.
    pattern_str = "|".join(re.escape(var) for var in sorted_variations)
    pattern = re.compile(pattern_str, re.IGNORECASE)

    return pattern, variation_lookup


def process_csv_row(
    row: Dict[str, str],
    accession_pattern: re.Pattern,
    stain_pattern: re.Pattern,
    stain_lookup: Dict[str, str],
) -> Dict[str, any]:
    """

    Processes a single CSV row to find, extract, and normalize the accession ID and stain.

    This is the core worker function that is executed in parallel for each row of the CSV.

    Args:
        row (Dict[str, str]): A dictionary representing a single row from the input CSV.
        accession_pattern (re.Pattern): The compiled regex for finding accession IDs.
        stain_pattern (re.Pattern): The compiled regex for finding stain names.
        stain_lookup (Dict[str, str]): The map to convert a found stain variation to its canonical name.

    Returns:
        Dict[str, any]: The original row dictionary, updated with the new parsed columns.
    """
    updated_row = row.copy()
    accession_id = None
    canonical_stain = None

    # Combine the OCR text from both label and macro images into a single string for searching.
    search_text = f"{row.get('label_text', '')} {row.get('macro_text', '')}"

    # --- Step 1: Find and Normalize the Accession ID ---
    accession_match = accession_pattern.search(search_text)
    if accession_match:
        # If a match is found, normalize it to a standard format (uppercase, hyphens, no spaces).
        accession_id = accession_match.group(0).replace(" ", "-").upper()

    # --- Step 2: Find and Normalize the Stain Name ---
    stain_match = stain_pattern.search(search_text)
    if stain_match:
        found_variation = stain_match.group(0).lower()
        # Use the pre-built lookup map to get the canonical name.
        canonical_stain = stain_lookup.get(found_variation, found_variation)

    # --- Step 3: Add the new data to the row dictionary ---
    updated_row[COL_ACCESSION_ID] = accession_id
    updated_row[COL_STAIN] = canonical_stain
    # The extraction is considered successful only if BOTH an ID and a stain were found.
    updated_row[COL_EXTRACTION_SUCCESSFUL] = bool(accession_id and canonical_stain)
    # Initialize the QC_PASSED column as empty.
    updated_row[COL_QC_PASSED] = ""

    return updated_row


def enrich_csv_with_parsing(
    input_path: Path, output_path: Path, accession_pattern_str: str, num_workers: int
):
    """
    Orchestrates the entire CSV enrichment process: reads the input, processes rows
    in parallel, and saves the final enriched CSV.

    Args:
        input_path (Path): Path to the input CSV file containing OCR text.
        output_path (Path): Path where the final, enriched CSV will be saved.
        accession_pattern_str (str): The regex pattern string for finding accession IDs.
        num_workers (int): The number of concurrent threads to use for processing.
    """
    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        return

    logger.info("Building regex patterns for data extraction...")
    try:
        # Compile the user-provided regex for accession IDs.
        accession_pattern = re.compile(accession_pattern_str, re.IGNORECASE)
        logger.info(f"Using accession pattern: {accession_pattern_str}")
    except re.error as e:
        logger.error(f"Invalid regex pattern for accession ID: '{accession_pattern_str}'. Error: {e}")
        return

    # Build the optimized stain pattern and lookup map.
    stain_pattern, stain_lookup = build_stain_normalizer(STAIN_NAME_CORRECTIONS)

    try:
        # Read the entire input CSV into memory.
        with open(input_path, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            original_rows = list(reader)
            original_headers = reader.fieldnames
    except Exception as e:
        logger.error(f"Failed to read input CSV '{input_path}': {e}")
        return

    if not original_rows:
        logger.warning("Input CSV is empty. No data to process.")
        return

    logger.info(f"Starting CSV enrichment for {len(original_rows)} rows using {num_workers} workers.")

    # Process all rows in parallel using a thread pool.
    updated_rows = []
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        # Schedule the processing of each row.
        future_to_row = {
            executor.submit(
                process_csv_row, row, accession_pattern, stain_pattern, stain_lookup
            ): row
            for row in original_rows
        }

        # Create a progress bar that updates as each row's processing completes.
        progress = tqdm(as_completed(future_to_row), total=len(original_rows), desc="Parsing rows")
        for future in progress:
            try:
                # Collect the result (the updated row) from the completed future.
                updated_rows.append(future.result())
            except Exception as e:
                logger.error(f"A row failed to process due to an unexpected error: {e}")

    # Calculate summary statistics for logging.
    successful_extractions = sum(1 for row in updated_rows if row[COL_EXTRACTION_SUCCESSFUL])

    # Define the headers for the output CSV, ensuring new columns are added.
    # `dict.fromkeys` is a trick to get unique headers while preserving order.
    new_headers = (original_headers or []) + [
        COL_ACCESSION_ID,
        COL_STAIN,
        COL_EXTRACTION_SUCCESSFUL,
        COL_QC_PASSED,
    ]
    final_headers = list(dict.fromkeys(new_headers))

    logger.info(f"Writing enriched data to '{output_path}'")
    try:
        # Write the final list of updated rows to the output CSV file.
        with open(output_path, mode="w", newline="", encoding="utf-8") as outfile:
            writer = csv.DictWriter(outfile, fieldnames=final_headers)
            writer.writeheader()
            writer.writerows(updated_rows)

        # Log a final summary of the operation.
        logger.info("CSV enrichment complete.")
        logger.info(f"Successfully processed {len(updated_rows)} data rows.")
        logger.info(f"Successfully extracted both ID and Stain in {successful_extractions} rows ({successful_extractions / len(updated_rows):.2%}).")

    except Exception as e:
        logger.exception(f"An unexpected error occurred while writing the output file: {e}")


# -----------------------------------------------------------------------------
# 5. Script Execution
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    # Set up the command-line interface for the script.
    parser = argparse.ArgumentParser(
        description="Extracts Accession ID and Stain from OCR text in a CSV, normalizes them, "
        "and appends the results as new columns."
    )
    parser.add_argument(
        "--input-csv",
        type=Path,
        required=True,
        help="Path to the input CSV file enriched with OCR text.",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        required=True,
        help="Path for the final, enriched output CSV file.",
    )
    parser.add_argument(
        "--accession-pattern",
        type=str,
        # A robust default regex that matches formats like 'NP 22-950' or 'NP22-123'.
        # \b ensures we match whole words only.
        # \s* allows for zero or more spaces.
        default=r"\b(NP\s*\d{2}\s*-\s*\d+)\b",
        help="Regex pattern to extract the Accession ID.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of worker threads for parallel processing.",
    )

    args = parser.parse_args()

    # Ensure the output directory exists before writing the file.
    args.output_csv.parent.mkdir(parents=True, exist_ok=True)

    # Run the main processing function with the provided arguments.
    enrich_csv_with_parsing(
        args.input_csv, args.output_csv, args.accession_pattern, args.workers
    )
