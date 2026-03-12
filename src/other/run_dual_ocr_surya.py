import os
from PIL import Image, UnidentifiedImageError
import torch
from concurrent.futures import ThreadPoolExecutor
import time
import traceback # Import traceback module

from surya.recognition import RecognitionPredictor
from surya.detection import DetectionPredictor

# --- Initialize Predictors (Keep as before) ---
device = "cuda" if torch.cuda.is_available() else "cpu"
print(f"Torch detected device: {device}. Surya predictors will likely use this.")
print("Initializing Surya DetectionPredictor...")
detection_predictor = DetectionPredictor()
print("Initializing Surya RecognitionPredictor...")
recognition_predictor = RecognitionPredictor()
ocr_langs = None # Auto-detect


# --- Modified processing function with INNER Try/Except and Traceback ---
def process_single_image(patient, label_subdir, macro_subdir, csv_path,
                         det_predictor, rec_predictor, langs):
    """Process a single patient's image, isolating predictor errors."""
    label_path = os.path.join(label_subdir, f"{patient}_label.png")
    macro_path = os.path.join(macro_subdir, f"{patient}_macro.png")

    label_text = "INIT"
    macro_text = "INIT"

    # --- Process Label Image ---
    if not os.path.exists(label_path):
        print(f"Label image not found for {patient}.")
        label_text = "IMAGE_NOT_FOUND"
    else:
        try:
            # Load Image
            img_label = Image.open(label_path).convert("RGB")
            print(f"DEBUG: Processing Label for {patient} - Image loaded.")
            lang_list_for_call = [langs] if langs else None
            predictions_label = None # Initialize before specific try

            # --- Isolate the Predictor Call ---
            try:
                predictions_label = rec_predictor([img_label], lang_list_for_call, det_predictor)
                # If successful, log the type (will be a list if it worked)
                print(f"DEBUG: Label predictor call successful for {patient}. Returned type: {type(predictions_label)}")

            except TypeError as te:
                # **** CATCH THE SPECIFIC TypeError ****
                print(f"ERROR: TypeError occurred *during* label prediction call for {patient}.")
                print("------- TRACEBACK START -------")
                print(traceback.format_exc()) # Print the full traceback
                print("------- TRACEBACK END -------")
                label_text = "ERROR_PREDICTION_CALL_TYPEERROR"
                # Ensure predictions_label remains None

            except Exception as pred_e:
                # Catch any OTHER errors during prediction call
                print(f"ERROR: Exception occurred *during* label prediction call for {patient}: {type(pred_e).__name__}")
                print("------- TRACEBACK START -------")
                print(traceback.format_exc()) # Print traceback for other errors too
                print("------- TRACEBACK END -------")
                label_text = f"ERROR_PREDICTION_CALL_{type(pred_e).__name__}"
                # Ensure predictions_label remains None
            # --- End Predictor Call Isolation ---

            # --- Process Results (only if prediction call didn't set an error) ---
            if label_text == "INIT":
                # Now run the checks on predictions_label (which might be None if call succeeded but returned None)
                if predictions_label is None:
                    print(f"INFO: Label predictor returned None for {patient} (after successful call).")
                    label_text = "PREDICTOR_RETURNED_NONE"
                elif not isinstance(predictions_label, list) or len(predictions_label) == 0:
                     print(f"WARNING: Label predictor returned non-list or empty list for {patient}. Value: {predictions_label}")
                     label_text = "PREDICTOR_RETURNED_UNEXPECTED"
                elif predictions_label[0] is None:
                     print(f"WARNING: Label predictor result element [0] is None for {patient}.")
                     label_text = "PREDICTION_OBJECT_IS_NONE"
                elif not hasattr(predictions_label[0], 'text_lines'):
                     print(f"WARNING: Label prediction object for {patient} missing 'text_lines' attribute.")
                     label_text = "PREDICTION_MALFORMED_NO_TEXT_LINES"
                elif predictions_label[0].text_lines is None:
                     print(f"INFO: Label prediction for {patient} has text_lines=None (No text detected).")
                     label_text = "NO_TEXT_DETECTED"
                elif not predictions_label[0].text_lines: # Check if text_lines is an empty list
                     print(f"INFO: Label prediction for {patient} has empty text_lines list (No text detected).")
                     label_text = "NO_TEXT_DETECTED"
                else:
                     # Extract text if everything is valid
                     try:
                         label_lines = [line.text for line in predictions_label[0].text_lines if hasattr(line, 'text')]
                         label_text = " ".join(label_lines)
                         print(f"Processed Label for {patient} - Text found.")
                     except Exception as line_ex:
                          print(f"ERROR: Could not extract text from label lines for {patient}: {line_ex}")
                          label_text = "ERROR_EXTRACTING_TEXT_FROM_LINES"

        except UnidentifiedImageError:
             print(f"ERROR: Cannot identify/open label image file (corrupted?): {label_path}")
             label_text = "ERROR_CORRUPT_IMAGE"
        except Exception as e:
            # This outer catch handles image loading errors or errors in the result processing logic itself
            print(f"ERROR processing label image {label_path} (outside prediction call): {type(e).__name__} - {e}")
            if label_text == "INIT": # Only set if not already set by inner error
                label_text = f"ERROR_PROCESSING_LABEL_OUTER: {type(e).__name__}"

    # --- Process Macro Image (Apply the exact same INNER try/except logic) ---
    if not os.path.exists(macro_path):
        print(f"Macro image not found for {patient}.")
        macro_text = "IMAGE_NOT_FOUND"
    else:
        try:
            img_macro = Image.open(macro_path).convert("RGB")
            # Rotate the image by 90 degrees clockwise
            img_macro = img_macro.rotate(-90, expand=True)
            # Cut the image by half on the longer side
            width, height = img_macro.size
            if width > height:
                img_macro = img_macro.crop((0, 0, width / 2, height))
            else:
                img_macro = img_macro.crop((0, 0, width, height / 2))
            print(f"DEBUG: Processing Macro for {patient} - Image loaded.")
            lang_list_for_call = [langs] if langs else None
            predictions_macro = None # Initialize

            # --- Isolate the Predictor Call ---
            try:
                predictions_macro = rec_predictor([img_macro], lang_list_for_call, det_predictor)
                print(f"DEBUG: Macro predictor call successful for {patient}. Returned type: {type(predictions_macro)}")

            except TypeError as te:
                print(f"ERROR: TypeError occurred *during* macro prediction call for {patient}.")
                print("------- TRACEBACK START -------")
                print(traceback.format_exc())
                print("------- TRACEBACK END -------")
                macro_text = "ERROR_PREDICTION_CALL_TYPEERROR"

            except Exception as pred_e:
                print(f"ERROR: Exception occurred *during* macro prediction call for {patient}: {type(pred_e).__name__}")
                print("------- TRACEBACK START -------")
                print(traceback.format_exc())
                print("------- TRACEBACK END -------")
                macro_text = f"ERROR_PREDICTION_CALL_{type(pred_e).__name__}"
            # --- End Predictor Call Isolation ---

            # --- Process Results (only if prediction call didn't set an error) ---
            if macro_text == "INIT":
                # Run checks on predictions_macro... (mirror the checks from the Label section)
                if predictions_macro is None:
                    print(f"INFO: Macro predictor returned None for {patient} (after successful call).")
                    macro_text = "PREDICTOR_RETURNED_NONE"
                elif not isinstance(predictions_macro, list) or len(predictions_macro) == 0:
                     print(f"WARNING: Macro predictor returned non-list or empty list for {patient}. Value: {predictions_macro}")
                     macro_text = "PREDICTOR_RETURNED_UNEXPECTED"
                elif predictions_macro[0] is None:
                     print(f"WARNING: Macro predictor result element [0] is None for {patient}.")
                     macro_text = "PREDICTION_OBJECT_IS_NONE"
                elif not hasattr(predictions_macro[0], 'text_lines'):
                     print(f"WARNING: Macro prediction object for {patient} missing 'text_lines' attribute.")
                     macro_text = "PREDICTION_MALFORMED_NO_TEXT_LINES"
                elif predictions_macro[0].text_lines is None:
                     print(f"INFO: Macro prediction for {patient} has text_lines=None (No text detected).")
                     macro_text = "NO_TEXT_DETECTED"
                elif not predictions_macro[0].text_lines:
                     print(f"INFO: Macro prediction for {patient} has empty text_lines list (No text detected).")
                     macro_text = "NO_TEXT_DETECTED"
                else:
                     try:
                         macro_lines = [line.text for line in predictions_macro[0].text_lines if hasattr(line, 'text')]
                         macro_text = " ".join(macro_lines)
                         print(f"Processed Macro for {patient} - Text found.")
                     except Exception as line_ex:
                          print(f"ERROR: Could not extract text from macro lines for {patient}: {line_ex}")
                          macro_text = "ERROR_EXTRACTING_TEXT_FROM_LINES"

        except UnidentifiedImageError:
             print(f"ERROR: Cannot identify/open macro image file (corrupted?): {macro_path}")
             macro_text = "ERROR_CORRUPT_IMAGE"
        except Exception as e:
            print(f"ERROR processing macro image {macro_path} (outside prediction call): {type(e).__name__} - {e}")
            if macro_text == "INIT":
                macro_text = f"ERROR_PROCESSING_MACRO_OUTER: {type(e).__name__}"

    # --- Combine text and write to CSV ---
    combined_text = f"Label: {label_text};Macro: {macro_text}"
    try:
        patient_safe = str(patient).replace('"', '""')
        combined_text_safe = combined_text.replace('"', '""')
        with open(csv_path, "a", encoding='utf-8') as f:
            f.write(f'"{patient_safe}";"{combined_text_safe}"\n')
    except Exception as e:
        print(f"ERROR writing to CSV for {patient}: {e}")

# --- process_image_files and Main Execution (Keep as before) ---
def process_image_files(input_dir, csv_path, num_workers=4):
    """Process all label and macro images in parallel using the NEW Surya-OCR API."""
    label_subdir = os.path.join(input_dir, "label")
    macro_subdir = os.path.join(input_dir, "macro")

    if not os.path.isdir(label_subdir):
        print(f"Error: Label directory not found: {label_subdir}")
        return
    if not os.path.isdir(macro_subdir):
        print(f"Error: Macro directory not found: {macro_subdir}")
        return

    try:
        patient_slide_names = [
            os.path.basename(image).split("_label.png")[0]
            for image in os.listdir(label_subdir)
            if image.endswith("_label.png") and os.path.isfile(os.path.join(label_subdir, image))
        ]
        print(f"Found {len(patient_slide_names)} potential patients based on label images.")
        if not patient_slide_names:
            print("No label images found matching the pattern *_label.png")
            return
    except FileNotFoundError:
        print(f"Error: Cannot list files in label directory: {label_subdir}")
        return

    write_header = not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0
    try:
        with open(csv_path, "a", encoding='utf-8') as f:
             if write_header:
                 f.write("Patient;Combined Text\n")
    except Exception as e:
         print(f"Error opening or writing header to CSV file {csv_path}: {e}")
         return

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [
            executor.submit(
                process_single_image,
                patient, label_subdir, macro_subdir, csv_path,
                detection_predictor, recognition_predictor, ocr_langs
                )
            for patient in patient_slide_names
        ]

        completed_count = 0
        total_tasks = len(futures)
        print(f"Submitted {total_tasks} tasks to the executor.")
        for i, future in enumerate(futures):
            try:
                future.result()
                completed_count += 1
            except Exception as e:
                 # Errors inside process_single_image should be caught there,
                 # but this catches errors during future management itself.
                print(f"CRITICAL ERROR in worker thread management (task index {i}): {e}")

    print(f"\nProcessing complete. {completed_count}/{total_tasks} tasks processed (check logs for individual errors).")


# --- Main Execution ---
input_dir = r"Correct-Me\NP22-assoc"
output_path = r"Correct-Me\NP22-assoc\output-surya-ocr-newAPI-debug.csv" # New output
ocr_langs = ["en"] # Specify language for OCR, or None for auto-detect
workers = 1 # Keep workers low for debugging

print(f"Starting OCR processing with NEW Surya API (Debug Mode)...")
print(f"Input directory: {input_dir}")
print(f"Output CSV: {output_path}")
print(f"Number of workers: {workers}")
print(f"Languages: {'Auto-detect' if ocr_langs is None else ocr_langs}")

process_image_files(input_dir, output_path, workers)

print("Script finished.")

# (Argument parsing section remains commented out as in the original)
# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(
#         description="Extract text from label/macro images using Surya-OCR (New API)"
#     )
#     # ... (rest of the argparse setup) ...
#     # args = parser.parse_args()
#     # process_image_files(args.input_dir, args.output_path, args.workers)
