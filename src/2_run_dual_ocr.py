import argparse
import csv
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import cv2
import easyocr
import numpy as np
import PIL
from tqdm import tqdm

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def clean_and_resolve_path(path_str):
    """Cleans a Windows-style path string and returns a resolved Path object."""
    if not path_str:
        return None

    # 1. Replace Windows backslashes with forward slashes (Unix/Linux standard)
    cleaned_str = path_str.replace("\\", "/")

    # 2. Strip any leading './' or '.\\' which can mess up absolute resolution
    #    when joined to a current working directory.
    if cleaned_str.startswith("./"):
        cleaned_str = cleaned_str[2:]

    # 3. Create the Path object and resolve it
    return Path(cleaned_str).resolve()


def preprocess_image_for_ocr(image_np: np.ndarray) -> np.ndarray:
    """
    Applies grayscale and Otsu's binarization to improve OCR accuracy.
    """
    # OpenCV expects BGR, but PIL opens as RGB. Convert RGB -> Grayscale.
    gray_image = cv2.cvtColor(image_np, cv2.COLOR_RGB2GRAY)

    # Apply binarization using Otsu's thresholding for automatic threshold detection.
    _, binary_image = cv2.threshold(
        gray_image, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU
    )

    # Convert single-channel binary image back to a 3-channel image for EasyOCR.
    three_channel_image = cv2.cvtColor(binary_image, cv2.COLOR_GRAY2RGB)
    return three_channel_image


def perform_ocr_on_row(row: dict, csv_dir: Path, reader: easyocr.Reader) -> dict:
    """
    Worker function to perform OCR on images specified in a single CSV row.

    Args:
        row (dict): A dictionary representing one row from the input CSV.
        csv_dir (Path): The parent directory of the input CSV, used to resolve relative paths.
        reader (easyocr.Reader): The initialized EasyOCR reader instance.

    Returns:
        dict: The original row dictionary, updated with OCR results and a QC flag.
    """
    updated_row = row.copy()
    label_text = ""
    macro_text = ""
    ocr_qc_needed = False  # Default to False. Set to True only on successful OCR.

    # --- Path Resolution and Validation ---
    # The paths in the CSV are relative to the CSV's location.
    label_path_str = row.get("label_path")
    macro_path_str = row.get("macro_path")

    # A dictionary to hold paths for processing
    paths_to_process = {}
    if label_path_str:
        label_path = clean_and_resolve_path(label_path_str)
        if label_path.exists():
            paths_to_process["label"] = label_path
        else:
            logger.warning(f"Label image not found at {label_path} for row: {row}")

    if macro_path_str:
        macro_path = clean_and_resolve_path(macro_path_str)
        if macro_path.exists():
            paths_to_process["macro"] = macro_path
        else:
            logger.warning(f"Macro image not found at {macro_path} for row: {row}")

    # If we have at least one valid image path, proceed with OCR.
    if paths_to_process:
        try:
            # --- Process Label Image ---
            if "label" in paths_to_process:
                image_label_pil = PIL.Image.open(paths_to_process["label"])
                image_label_np = np.array(image_label_pil)
                processed_label = preprocess_image_for_ocr(image_label_np)
                ocr_results = reader.readtext(processed_label)
                label_text = " ".join([text for _, text, _ in ocr_results])

            # --- Process Macro Image ---
            if "macro" in paths_to_process:
                image_macro_pil = PIL.Image.open(paths_to_process["macro"])
                # Apply specific transformations for macro image
                img_macro_pil = image_macro_pil.rotate(-90, expand=True)
                width, height = img_macro_pil.size
                crop_box = (
                    (0, 0, width / 2, height)
                    if width > height
                    else (0, 0, width, height / 2)
                )
                img_macro_pil = img_macro_pil.crop(crop_box)

                image_macro_np = np.array(img_macro_pil)
                processed_macro = preprocess_image_for_ocr(image_macro_np)
                ocr_results = reader.readtext(
                    processed_macro, rotation_info=[0, 90, 180, 270]
                )
                macro_text = " ".join([text for _, text, _ in ocr_results])

            # If we successfully ran OCR on at least one image, mark for QC.
            ocr_qc_needed = True

        except Exception as e:
            logger.error(f"Failed OCR on row {row.get('original_slide_path')}: {e}")
            # Even on failure, we keep existing text (if any) and don't request QC.
            ocr_qc_needed = False

    # Add new/updated fields to the row
    updated_row["label_text"] = label_text
    updated_row["macro_text"] = macro_text
    updated_row["ocr_qc_needed"] = ocr_qc_needed

    return updated_row


def add_ocr_to_mapping(
    mapping_csv: Path, output_csv: Path, use_cpu: bool, num_workers: int
):
    """
    Reads a mapping CSV, performs OCR on the specified images, and writes an enriched CSV.
    """
    if not mapping_csv.exists():
        logger.error(f"Input mapping CSV not found: {mapping_csv}")
        return

    logger.info("Initializing EasyOCR reader...")
    reader = easyocr.Reader(["en"], gpu=not use_cpu)

    # Read all rows from the source CSV into memory
    with open(mapping_csv, "r", encoding="utf-8") as f:
        reader_csv = csv.DictReader(f)
        rows = list(reader_csv)

    if not rows:
        logger.warning("Input CSV is empty. Nothing to process.")
        return

    updated_rows = []
    csv_dir = mapping_csv.parent  # Base directory for resolving relative paths

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        future_to_row = {
            executor.submit(perform_ocr_on_row, row, csv_dir, reader): row
            for row in rows
        }

        progress_bar = tqdm(
            as_completed(future_to_row), total=len(rows), desc="Running OCR"
        )
        for future in progress_bar:
            updated_rows.append(future.result())

    if not updated_rows:
        logger.error("Processing failed. No rows were updated.")
        return

    # Write the updated data to the new CSV file
    # Dynamically determine headers from the first updated row
    headers = list(updated_rows[0].keys())

    logger.info(f"Writing enriched data to {output_csv}...")
    try:
        with open(output_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(updated_rows)
        logger.info("Successfully created enriched CSV.")
    except Exception as e:
        logger.error(f"Failed to write output CSV: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Enrich a slide mapping CSV with OCR text from label and macro images."
    )
    parser.add_argument(
        "--mapping-csv",
        type=Path,
        required=True,
        help="Path to the input CSV file generated by the first script (e.g., slide_mapping.csv).",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        required=True,
        help="Path to save the new CSV file with added OCR columns.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of worker threads for parallel processing.",
    )
    parser.add_argument(
        "--use-cpu",
        action="store_true",
        help="Force EasyOCR to use CPU instead of GPU.",
    )

    args = parser.parse_args()

    # Ensure the parent directory for the output CSV exists
    args.output_csv.parent.mkdir(parents=True, exist_ok=True)

    add_ocr_to_mapping(args.mapping_csv, args.output_csv, args.use_cpu, args.workers)
