"""
This script extracts associated images (macro, label, thumbnail) from whole-slide image (WSI)
files, such as those in SVS format. It processes files in a specified input directory
recursively, saves the extracted images to an output directory while maintaining the original
folder structure, and generates a CSV file mapping the original slide paths to the new
image paths.

The script is designed for efficiency, using a thread pool to process multiple
files concurrently. It is configurable via command-line arguments for input/output
directories, file extensions, and the number of parallel workers.

Key libraries used:
- argparse: For parsing command-line arguments.
- csv: For writing the output mapping file.
- logging: For providing informative output about the script's execution.
- concurrent.futures: For parallel processing of slide files to speed up execution.
- pathlib: For modern, object-oriented handling of filesystem paths.
- openslide: A Python library for reading WSI files.
- tqdm: For displaying a progress bar during file processing.
"""

import argparse
import csv
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# Third-party libraries
import openslide
from tqdm import tqdm

# --- Configuration ---

# Set up a logger for informative console output.
# The format includes timestamp, log level, and the message.
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Define constants for the names of associated images typically found in WSI files.
# This makes the code cleaner and avoids magic strings.
ASSOCIATED_IMAGE_TYPES = ["macro", "label", "thumbnail"]
# Set a default size for thumbnails if the slide file doesn't contain an embedded one.
DEFAULT_THUMBNAIL_SIZE = (300, 300)


def extract_associated_images(
    svs_path: Path,
    input_dir: Path,
    output_dir: Path,
    maintain_structure: bool = True,
    thumbnail_size: tuple = DEFAULT_THUMBNAIL_SIZE,
):
    """
    Extracts associated images (macro, label, thumbnail) from a single whole-slide image file.

    If a thumbnail is not found in the slide's associated images, one is generated
    from the slide's base layer at the specified size.

    Args:
        svs_path (Path): The absolute path to the SVS or other WSI file.
        input_dir (Path): The root directory where the recursive search for SVS files began.
                          This is used to determine the relative path for maintaining structure.
        output_dir (Path): The root directory where the extracted images will be saved.
        maintain_structure (bool): If True, replicates the sub-directory structure from
                                   the input_dir in the output_dir.
        thumbnail_size (tuple): A (width, height) tuple for generating a thumbnail if one
                                is not already present in the slide file.

    Returns:
        A dictionary mapping image types ('original', 'macro', 'label', 'thumbnail')
        to their file paths (as Path objects). Returns None if the slide file cannot be
        opened or an error occurs.
    """
    try:
        # Open the whole-slide image file.
        slide = openslide.OpenSlide(str(svs_path))

        # Get the base filename without the extension (e.g., "slide01" from "slide01.svs").
        base_name = svs_path.stem
        # Initialize a dictionary to store the paths of the original slide and extracted images.
        output_paths = {"original": svs_path}

        # --- Determine the output subdirectory ---
        # This logic ensures that if the input is `data/slides/case1/slide.svs`,
        # the output will be `output/macro/slides/case1/`.
        if maintain_structure:
            # Calculate the relative path from the input root to the slide's parent directory.
            relative_sub_dir = svs_path.parent.relative_to(input_dir)
        else:
            # If not maintaining structure, all images go directly into the top-level output folder.
            relative_sub_dir = Path()  # An empty path.

        # --- Loop through each associated image type to extract it ---
        for image_type in ASSOCIATED_IMAGE_TYPES:
            # Construct the full output directory for this specific image type (e.g., output/macro/...).
            image_output_dir = output_dir / image_type / relative_sub_dir
            # Create the directory if it doesn't exist. `parents=True` creates parent dirs as needed.
            image_output_dir.mkdir(parents=True, exist_ok=True)

            # Define the full path for the output image file.
            output_filename = f"{base_name}_{image_type}.png"
            image_path = image_output_dir / output_filename

            # Check if the associated image exists in the slide file.
            if image_type in slide.associated_images:
                # Get the image object.
                image = slide.associated_images[image_type]
                # Save the image to the specified path.
                image.save(image_path)
                logger.debug(f"Extracted {image_type} from {svs_path}")
            # Special case: if a thumbnail is missing, generate one.
            elif image_type == "thumbnail":
                logger.warning(f"No thumbnail in {svs_path}, generating one.")
                # Generate a thumbnail from the slide's primary image data.
                image = slide.get_thumbnail(thumbnail_size)
                image.save(image_path)
            else:
                # If the image type (e.g., macro) is not found, mark its path as None.
                image_path = None

            # Store the path (or None) in the results dictionary.
            output_paths[image_type] = image_path

        # Close the slide file to release resources.
        slide.close()
        return output_paths

    except Exception as e:
        # Log any errors that occur during processing of a single slide.
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
    Finds and processes all slide files in a directory and its subdirectories
    using a thread pool for concurrent execution. It then writes the results
    to a CSV file.

    Args:
        input_dir (Path): The directory to search for slide files.
        output_dir (Path): The root directory to save extracted images.
        csv_path (Path): The path where the output CSV mapping file will be saved.
        extensions (list): A list of file extensions (e.g., ['svs', 'tif']) to look for.
        num_workers (int): The number of worker threads to use for parallel processing.
        thumbnail_size (tuple): The (width, height) to use for generated thumbnails.
    """
    logger.info(f"Scanning for files with extensions {extensions} in {input_dir}...")

    # --- Find all slide files recursively ---
    slide_files = []
    for ext in extensions:
        # `rglob` finds all files matching the pattern in the directory and subdirectories.
        slide_files.extend(list(input_dir.rglob(f"*.{ext}")))

    if not slide_files:
        logger.warning("No slide files found to process.")
        return

    logger.info(f"Found {len(slide_files)} slide files to process.")
    # Ensure the main output directory exists.
    output_dir.mkdir(parents=True, exist_ok=True)

    results = []
    # --- Use a ThreadPoolExecutor for concurrent processing ---
    # This creates a pool of `num_workers` threads to execute tasks in parallel.
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        # `executor.submit` schedules a function to be executed and returns a 'future' object.
        # A future represents a pending result. We store it in a dictionary to link it
        # back to the original file path.
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

        # `as_completed` is an iterator that yields futures as they complete.
        # This allows us to process results as they become available.
        # `tqdm` wraps this iterator to create a live progress bar.
        progress_bar = tqdm(
            as_completed(future_to_svs),
            total=len(slide_files),
            desc="Processing slides",
        )
        for future in progress_bar:
            # `future.result()` retrieves the return value from the `extract_associated_images` function.
            result = future.result()
            if result:
                results.append(result)

    if not results:
        logger.warning(
            "Processing complete, but no images were successfully extracted."
        )
        return

    # --- Write the results to a CSV file ---
    logger.info(f"Writing mapping to {csv_path}...")
    # Get the parent directory of the CSV file to calculate relative paths from it.
    csv_parent_dir = csv_path.parent
    try:
        # Open the CSV file for writing. `newline=''` prevents extra blank rows.
        with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
            # Define the header for the CSV file.
            header = [f"{img_type}_path" for img_type in ASSOCIATED_IMAGE_TYPES] + [
                "original_slide_path"
            ]
            writer = csv.DictWriter(csvfile, fieldnames=header)
            writer.writeheader()

            for result in results:
                relative_paths = {}
                # Calculate paths relative to the CSV file's location for portability.
                for img_type in ASSOCIATED_IMAGE_TYPES:
                    path_key = f"{img_type}_path"
                    # Check if the image was successfully extracted (path is not None).
                    if result.get(img_type):
                        # `relative_to` calculates the relative path.
                        # `.as_posix()` ensures forward slashes are used, which is common for paths in text files.
                        relative_paths[path_key] = csv_parent_dir / result[img_type]
                    else:
                        relative_paths[path_key] = ""  # Use an empty string if not found.

                relative_paths["original_slide_path"] = result["original"]

                writer.writerow(relative_paths)

        logger.info(
            f"Successfully processed {len(results)} out of {len(slide_files)} files."
        )
        logger.info(f"CSV mapping with relative paths saved to {csv_path}")

    except Exception as e:
        logger.error(f"Failed to write CSV file: {e}")


# --- Main execution block ---
# This code runs only when the script is executed directly from the command line.
if __name__ == "__main__":
    # Set up the command-line argument parser.
    parser = argparse.ArgumentParser(
        description="Extract associated images (macro, label, thumbnail) from whole-slide image files."
    )
    # Define the command-line arguments.
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
        nargs="+",  # `+` means one or more arguments.
        default=["svs"],
        help="List of file extensions to process (e.g., svs tif ndpi).",
    )
    parser.add_argument(
        "--thumbnail-size",
        type=int,
        nargs=2,  # Expect exactly two integer values.
        default=DEFAULT_THUMBNAIL_SIZE,
        metavar=("WIDTH", "HEIGHT"),
        help=f"Size of thumbnails to generate if not present. Default: {DEFAULT_THUMBNAIL_SIZE[0]} {DEFAULT_THUMBNAIL_SIZE[1]}",
    )

    # Parse the arguments provided by the user.
    args = parser.parse_args()

    # Before starting, ensure the parent directory for the output CSV file exists.
    output_dir = Path(args.output_dir)
    csv_path = output_dir / args.csv_path
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    # Call the main processing function with the parsed arguments.
    process_slide_files(
        args.input_dir,
        output_dir,
        csv_path,
        args.extensions,
        args.workers,
        tuple(args.thumbnail_size),  # Convert the list [w, h] to a tuple (w, h).
    )
