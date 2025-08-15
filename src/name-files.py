import csv
import re
import logging

# -----------------------------------------------------------------------------
# Logging configuration
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Configuration & Regex patterns
# -----------------------------------------------------------------------------
# Mapping dictionary for canonical stain names to common spelling mistakes/variations
stain_name_corrections = {
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

# --- Create regex pattern and normalization map for stains ---
all_stain_variations = set()
variation_to_canonical_map = {}

for canonical_name, variations in stain_name_corrections.items():
    # Ensure the canonical name itself is included as a variation
    # Use a set to automatically handle duplicates if canonical name is also in list
    current_variations = set(variations)
    current_variations.add(canonical_name)

    all_stain_variations.update(current_variations)
    for variation in current_variations:
        # Map the lowercase version of the variation to the canonical name
        variation_to_canonical_map[variation.lower()] = canonical_name

# Escape variations for regex and join with OR '|'
stain_pattern_str = "|".join(re.escape(name) for name in all_stain_variations)
stain_pattern = re.compile(stain_pattern_str, re.IGNORECASE)

# --- Pattern to extract accession ID ---
reg_name = "S"
accession_pattern = re.compile(rf"{reg_name}[- ]\d+", re.IGNORECASE)

# -----------------------------------------------------------------------------
# File paths (adjust as necessary)
# -----------------------------------------------------------------------------
# Use raw strings (r"...") or forward slashes for Windows paths
input_filename = "Data/combined_data_ocr.csv"
output_filename = "Data/combined_data_ocr_processed.csv"

# -----------------------------------------------------------------------------
# Main processing function
# -----------------------------------------------------------------------------
def process_csv(input_file: str, output_file: str):
    """
    Reads a CSV file, extracts Accession ID and Stain Name from each row,
    normalizes the stain name, and writes the results to an output CSV file.
    """
    logger.info("Starting CSV processing from '%s' to '%s'", input_file, output_file)
    processed_lines = 0
    found_both_count = 0

    try:
        with open(input_file, mode='r', newline='', encoding="utf-8", errors='replace') as infile, \
            open(output_file, mode='w', newline='', encoding="utf-8") as outfile:

            reader = csv.reader(infile, delimiter=';')
            writer = csv.writer(outfile, delimiter=';')

            # Write header row for the output CSV
            writer.writerow(["AccessionID", "Stain", "Complete", "OriginalLine"])

            for i, row in enumerate(reader):
                # Reassemble the original CSV line (useful for debugging or later reference)
                # Handle potential None values in row if reader yields them (unlikely but safe)
                original_line = ";".join(str(field) if field is not None else "" for field in row)
                accession_id = None
                found_stain_variation = None
                canonical_stain = None

                # Look through every field to gather target substring data.
                for field in row:
                    # Ensure field is a string before searching
                    if not isinstance(field, str):
                        continue

                    # Search for Accession ID if not already found
                    if accession_id is None:
                        match_accession = accession_pattern.search(field)
                        if match_accession:
                            accession_id = match_accession.group(0).replace(" ", "-").upper() # Normalize spacing and case
                            # Optional: remove leading/trailing spaces if needed: accession_id = accession_id.strip()


                    # Search for Stain Name if not already found
                    if found_stain_variation is None:
                        match_stain = stain_pattern.search(field)
                        if match_stain:
                            found_stain_variation = match_stain.group(0)
                            # Normalize the found stain variation to its canonical form
                            canonical_stain = variation_to_canonical_map.get(found_stain_variation.lower(), found_stain_variation) # Fallback to original if somehow not in map

                    # Optimization: Stop searching fields in this row if both are found
                    if accession_id and canonical_stain:
                        break

                # Determine completeness
                complete = bool(accession_id and canonical_stain)
                if complete:
                    found_both_count += 1

                # Log the details of processing at INFO level
                # Use the canonical_stain for logging
                logger.info("Processed line %d - AccessionID: %s, Stain: %s (Found as: %s), Complete: %s",
                            i + 1, accession_id, canonical_stain, found_stain_variation, complete)

                # Write results to output CSV
                # Use the canonical_stain in the output
                writer.writerow([accession_id or "", canonical_stain or "", complete, original_line])
                processed_lines += 1

        logger.info("Finished CSV processing. Processed %d lines.", processed_lines)
        logger.info("Found both Accession ID and Stain in %d lines.", found_both_count)

    except FileNotFoundError:
        logger.error("Error: Input file not found at '%s'", input_file)
    except Exception as e:
        logger.exception("An unexpected error occurred during CSV processing: %s", e)

# -----------------------------------------------------------------------------
# Script execution
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    process_csv(input_filename, output_filename)