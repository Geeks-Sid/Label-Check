import argparse
import csv
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import openslide
from tqdm import tqdm

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Constants for standard associated image names
ASSOCIATED_IMAGE_TYPES = ["macro", "label", "thumbnail"]
DEFAULT_THUMBNAIL_SIZE = (300, 300)


def extract_associated_images(
    svs_path: Path,
    input_dir: Path,
    output_dir: Path,
    maintain_structure: bool = True,
    thumbnail_size: tuple = DEFAULT_THUMBNAIL_SIZE,
):
    """
    Extracts associated images (macro, label, thumbnail) from a whole-slide image file.

    Args:
        svs_path (Path): Path to the SVS file.
        input_dir (Path): The root directory where scanning for SVS files began.
        output_dir (Path): The root directory to save extracted images.
        maintain_structure (bool): If True, replicate the input sub-directory structure.
        thumbnail_size (tuple): The size (width, height) for generated thumbnails if one isn't found.

    Returns:
        A dictionary containing paths to the extracted images and the original slide,
        or None if an error occurs.
    """
    try:
        slide = openslide.OpenSlide(str(svs_path))
        base_name = svs_path.stem
        output_paths = {"original": svs_path}

        # Determine the correct subdirectory for output
        if maintain_structure:
            relative_sub_dir = svs_path.parent.relative_to(input_dir)
        else:
            relative_sub_dir = Path()  # Empty path

        # Loop through macro, label, and thumbnail to avoid repeating code
        for image_type in ASSOCIATED_IMAGE_TYPES:
            # Create the specific output directory (e.g., output/macro/...)
            image_output_dir = output_dir / image_type / relative_sub_dir
            image_output_dir.mkdir(parents=True, exist_ok=True)

            output_filename = f"{base_name}_{image_type}.png"
            image_path = image_output_dir / output_filename

            if image_type in slide.associated_images:
                image = slide.associated_images[image_type]
                image.save(image_path)
                logger.debug(f"Extracted {image_type} from {svs_path}")
            elif image_type == "thumbnail":
                # If no thumbnail exists, generate one from the base slide
                logger.warning(f"No thumbnail in {svs_path}, generating one.")
                image = slide.get_thumbnail(thumbnail_size)
                image.save(image_path)
            else:
                image_path = None  # Mark as not found

            output_paths[image_type] = image_path

        slide.close()
        return output_paths

    except Exception as e:
        logger.error(f"Error processing {svs_path}: {e}")
        return None


def process_slide_files(
    input_dir: Path,
    output_dir: Path,
    csv_path: Path,
    extensions: list,
    num_workers: int,
    thumbnail_size: tuple,
):
    """
    Finds and processes all slide files in a directory using a thread pool.
    """
    logger.info(f"Scanning for files with extensions {extensions} in {input_dir}...")

    # Use glob to find all matching files recursively
    slide_files = []
    for ext in extensions:
        slide_files.extend(list(input_dir.rglob(f"*.{ext}")))

    if not slide_files:
        logger.warning("No slide files found to process.")
        return

    logger.info(f"Found {len(slide_files)} slide files to process.")
    output_dir.mkdir(parents=True, exist_ok=True)

    results = []
    # Use ThreadPoolExecutor for concurrent processing
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        # Create a dictionary of futures
        future_to_svs = {
            executor.submit(
                extract_associated_images,
                svs_path,
                input_dir,
                output_dir,
                thumbnail_size=thumbnail_size,
            ): svs_path
            for svs_path in slide_files
        }

        # Use tqdm for a progress bar as futures complete
        progress_bar = tqdm(
            as_completed(future_to_svs),
            total=len(slide_files),
            desc="Processing slides",
        )
        for future in progress_bar:
            result = future.result()
            if result:
                results.append(result)

    if not results:
        logger.warning(
            "Processing complete, but no images were successfully extracted."
        )
        return

    # Write the results to a CSV file with relative paths
    logger.info(f"Writing mapping to {csv_path}...")
    csv_parent_dir = csv_path.parent
    try:
        with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
            header = [f"{img_type}_path" for img_type in ASSOCIATED_IMAGE_TYPES] + [
                "original_slide_path"
            ]
            writer = csv.DictWriter(csvfile, fieldnames=header)
            writer.writeheader()

            for result in results:
                relative_paths = {}
                # Calculate paths relative to the CSV file's location
                for img_type in ASSOCIATED_IMAGE_TYPES:
                    path_key = f"{img_type}_path"
                    if result.get(img_type):
                        relative_paths[path_key] = (
                            result[img_type].relative_to(csv_parent_dir).as_posix()
                        )
                    else:
                        relative_paths[path_key] = ""  # Empty string for not found

                relative_paths["original_slide_path"] = (
                    result["original"].relative_to(csv_parent_dir).as_posix()
                )
                writer.writerow(relative_paths)

        logger.info(
            f"Successfully processed {len(results)} out of {len(slide_files)} files."
        )
        logger.info(f"CSV mapping with relative paths saved to {csv_path}")

    except Exception as e:
        logger.error(f"Failed to write CSV file: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extract associated images (macro, label, thumbnail) from whole-slide image files."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        required=True,
        help="Input directory containing slide files.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Output directory for extracted images.",
    )
    parser.add_argument(
        "--csv-path",
        type=Path,
        default=Path("slide_mapping.csv"),
        help="Path to save the CSV mapping file.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of worker threads for parallel processing.",
    )
    parser.add_argument(
        "--extensions",
        nargs="+",
        default=["svs"],
        help="List of file extensions to process (e.g., svs tif ndpi).",
    )
    parser.add_argument(
        "--thumbnail-size",
        type=int,
        nargs=2,
        default=DEFAULT_THUMBNAIL_SIZE,
        metavar=("WIDTH", "HEIGHT"),
        help=f"Size of thumbnails to generate if not present. Default: {DEFAULT_THUMBNAIL_SIZE[0]} {DEFAULT_THUMBNAIL_SIZE[1]}",
    )

    args = parser.parse_args()

    # Ensure the parent directory for the CSV exists
    args.csv_path.parent.mkdir(parents=True, exist_ok=True)

    process_slide_files(
        args.input_dir,
        args.output_dir,
        args.csv_path,
        args.extensions,
        args.workers,
        tuple(args.thumbnail_size),
    )
