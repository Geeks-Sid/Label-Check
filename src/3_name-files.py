import argparse
import csv
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
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
COL_QC_PASSED = "ParsingQCPassed"  # Renamed for clarity vs ocr_qc_needed

# Mapping for stain name corrections. This could also be loaded from a JSON/YAML file.

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
        current_variations = {v.lower() for v in variations}
        current_variations.add(canonical.lower())
        all_variations.update(current_variations)
        for var in current_variations:
            variation_lookup[var] = canonical

    sorted_variations = sorted(list(all_variations), key=len, reverse=True)
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
    Processes a single CSV row dictionary to find and normalize accession ID and stain.
    """
    updated_row = row.copy()
    accession_id = None
    canonical_stain = None

    search_text = f"{row.get('label_text', '')} {row.get('macro_text', '')}"

    # 1. Find Accession ID
    accession_match = accession_pattern.search(search_text)
    if accession_match:
        # Normalize the found ID: uppercase and use hyphens
        accession_id = accession_match.group(0).replace(" ", "-").upper()

    # 2. Find Stain Name
    stain_match = stain_pattern.search(search_text)
    if stain_match:
        found_variation = stain_match.group(0).lower()
        canonical_stain = stain_lookup.get(found_variation, found_variation)

    # 3. Add the new data to the row
    updated_row[COL_ACCESSION_ID] = accession_id
    updated_row[COL_STAIN] = canonical_stain
    updated_row[COL_EXTRACTION_SUCCESSFUL] = bool(accession_id and canonical_stain)
    updated_row[COL_QC_PASSED] = ""

    return updated_row


def enrich_csv_with_parsing(
    input_path: Path, output_path: Path, accession_pattern_str: str, num_workers: int
):
    """
    Reads a CSV, enriches it with extracted data in parallel, and saves it.
    """
    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        return

    logger.info("Building regex patterns for extraction...")
    try:
        # *** CHANGE: Compile the flexible pattern provided by the user ***
        accession_pattern = re.compile(accession_pattern_str, re.IGNORECASE)
        logger.info(f"Using accession pattern: {accession_pattern_str}")
    except re.error as e:
        logger.error(
            f"Invalid regex pattern for accession ID: '{accession_pattern_str}'. Error: {e}"
        )
        return

    stain_pattern, stain_lookup = build_stain_normalizer(STAIN_NAME_CORRECTIONS)

    try:
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

    logger.info(
        f"Starting CSV enrichment for {len(original_rows)} rows using {num_workers} workers."
    )

    updated_rows = []
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        future_to_row = {
            executor.submit(
                process_csv_row, row, accession_pattern, stain_pattern, stain_lookup
            ): row
            for row in original_rows
        }

        progress = tqdm(
            as_completed(future_to_row), total=len(original_rows), desc="Parsing rows"
        )
        for future in progress:
            try:
                updated_rows.append(future.result())
            except Exception as e:
                logger.error(f"A row failed to process: {e}")

    successful_extractions = sum(
        1 for row in updated_rows if row[COL_EXTRACTION_SUCCESSFUL]
    )

    new_headers = (original_headers or []) + [
        COL_ACCESSION_ID,
        COL_STAIN,
        COL_EXTRACTION_SUCCESSFUL,
        COL_QC_PASSED,
    ]
    final_headers = list(dict.fromkeys(new_headers))

    logger.info(f"Writing enriched data to '{output_path}'")
    try:
        with open(output_path, mode="w", newline="", encoding="utf-8") as outfile:
            writer = csv.DictWriter(outfile, fieldnames=final_headers)
            writer.writeheader()
            writer.writerows(updated_rows)

        logger.info("CSV enrichment complete.")
        logger.info(f"Processed {len(updated_rows)} data rows.")
        logger.info(
            f"Successfully extracted both ID and Stain in {successful_extractions} rows."
        )

    except Exception as e:
        logger.exception(
            f"An unexpected error occurred while writing the output file: {e}"
        )


# -----------------------------------------------------------------------------
# Script Execution
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extracts Accession ID and Stain from OCR text in a CSV, normalizes them, "
        "and appends the results as new columns."
    )
    parser.add_argument(
        "--input-csv",
        type=Path,
        required=True,
        help="Path to the input CSV file (e.g., 'mapping_with_ocr.csv').",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        required=True,
        help="Path for the final, enriched output CSV file.",
    )
    # *** CHANGE: New, more powerful argument with a better default ***
    parser.add_argument(
        "--accession-pattern",
        type=str,
        default=r"\b(NP\s*\d+\s*-\s*\d+)\b",
        help="Regex pattern to extract the Accession ID. "
        "Default matches formats like 'S-12345' and 'NP22-950'.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of worker threads for parallel processing.",
    )

    args = parser.parse_args()

    args.output_csv.parent.mkdir(parents=True, exist_ok=True)

    enrich_csv_with_parsing(
        args.input_csv, args.output_csv, args.accession_pattern, args.workers
    )
