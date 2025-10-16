import argparse
import csv
import logging
import re
from pathlib import Path
from typing import Dict, List, Tuple

from tqdm import tqdm

# -----------------------------------------------------------------------------
# Logging Configuration
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Configuration & Constants
# -----------------------------------------------------------------------------
# Names for the new columns to be added to the CSV
COL_ACCESSION_ID = "AccessionID"
COL_STAIN = "Stain"
COL_EXTRACTION_SUCCESSFUL = "ExtractionSuccessful"
COL_QC_PASSED = "QCPassed"

# Mapping for stain name corrections. This could also be loaded from a JSON/YAML file.
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
# Core Functions
# -----------------------------------------------------------------------------


def build_stain_normalizer(
    corrections: Dict[str, List[str]],
) -> Tuple[re.Pattern, Dict[str, str]]:
    """Builds a regex pattern and a lookup map for normalizing stain names."""
    variation_lookup = {}
    all_variations = set()

    for canonical, variations in corrections.items():
        # Ensure the canonical name is part of the variations set
        current_variations = set(v.lower() for v in variations)
        current_variations.add(canonical.lower())

        all_variations.update(current_variations)
        for var in current_variations:
            variation_lookup[var] = canonical

    # Sort by length descending to match longer variations first (e.g., "H&E" before "H")
    sorted_variations = sorted(list(all_variations), key=len, reverse=True)
    pattern_str = "|".join(re.escape(var) for var in sorted_variations)
    pattern = re.compile(pattern_str, re.IGNORECASE)

    return pattern, variation_lookup


def process_row(
    row: List[str],
    accession_pattern: re.Pattern,
    stain_pattern: re.Pattern,
    stain_lookup: Dict[str, str],
) -> Dict[str, any]:
    """
    Processes a single CSV row to find and normalize accession ID and stain.

    Returns:
        A dictionary with the extracted and processed data.
    """
    accession_id = None
    canonical_stain = None

    # Join the row into a single string for a comprehensive search
    # This handles cases where an ID or stain is split across cells
    full_row_text = ";".join(str(field) for field in row if field)

    # 1. Find Accession ID
    accession_match = accession_pattern.search(full_row_text)
    if accession_match:
        accession_id = accession_match.group(0).replace(" ", "-").upper()

    # 2. Find Stain Name
    stain_match = stain_pattern.search(full_row_text)
    if stain_match:
        found_variation = stain_match.group(0).lower()
        canonical_stain = stain_lookup.get(found_variation, found_variation)

    return {
        COL_ACCESSION_ID: accession_id,
        COL_STAIN: canonical_stain,
        COL_EXTRACTION_SUCCESSFUL: bool(accession_id and canonical_stain),
    }


def enrich_csv_data(input_path: Path, output_path: Path, delimiter: str = ";"):
    """
    Reads a CSV, enriches it with extracted data, and saves it to a new file.
    """
    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        return

    logger.info("Building regex patterns for extraction...")
    # --- Pattern to extract accession ID ---
    reg_name = "S"
    accession_pattern = re.compile(rf"{reg_name}[- ]\d+", re.IGNORECASE)
    stain_pattern, stain_lookup = build_stain_normalizer(STAIN_NAME_CORRECTIONS)

    logger.info(f"Starting CSV enrichment from '{input_path}' to '{output_path}'")

    try:
        # First, count rows for the progress bar
        with open(input_path, mode="r", encoding="utf-8", errors="replace") as f:
            total_rows = sum(1 for row in f) - 1  # Subtract header

        with (
            open(
                input_path, mode="r", newline="", encoding="utf-8", errors="replace"
            ) as infile,
            open(output_path, mode="w", newline="", encoding="utf-8") as outfile,
        ):
            reader = csv.reader(infile, delimiter=delimiter)
            writer = csv.writer(outfile, delimiter=delimiter)

            # Read original header and create the new, enriched header
            header = next(reader)
            new_header = header + [
                COL_ACCESSION_ID,
                COL_STAIN,
                COL_EXTRACTION_SUCCESSFUL,
                COL_QC_PASSED,
            ]
            writer.writerow(new_header)

            successful_extractions = 0

            # Use tqdm for a progress bar
            for row in tqdm(reader, total=total_rows, desc="Processing rows"):
                extracted_data = process_row(
                    row, accession_pattern, stain_pattern, stain_lookup
                )

                if extracted_data[COL_EXTRACTION_SUCCESSFUL]:
                    successful_extractions += 1

                # Create the new row by appending extracted data
                output_row = row + [
                    extracted_data[COL_ACCESSION_ID] or "",
                    extracted_data[COL_STAIN] or "",
                    extracted_data[COL_EXTRACTION_SUCCESSFUL],
                    "",  # Leave QCPassed column empty for manual review
                ]
                writer.writerow(output_row)

        logger.info("CSV enrichment complete.")
        logger.info(f"Processed {total_rows} data rows.")
        logger.info(
            f"Successfully extracted both ID and Stain in {successful_extractions} rows."
        )

    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")


# -----------------------------------------------------------------------------
# Script Execution
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extracts Accession ID and Stain Name from a CSV, normalizes them, "
        "and appends the results as new columns to the original data."
    )
    parser.add_argument(
        "-i",
        "--input-file",
        type=Path,
        required=True,
        help="Path to the input CSV file.",
    )
    parser.add_argument(
        "-o",
        "--output-file",
        type=Path,
        required=True,
        help="Path for the enriched output CSV file.",
    )
    parser.add_argument(
        "-d",
        "--delimiter",
        type=str,
        default=";",
        help="Delimiter used in the CSV file (default: ';').",
    )

    args = parser.parse_args()

    # Ensure the output directory exists
    args.output_file.parent.mkdir(parents=True, exist_ok=True)

    enrich_csv_data(args.input_file, args.output_file, args.delimiter)
