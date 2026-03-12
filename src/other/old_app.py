import csv
import os
import re
import shutil
import datetime
from flask import Flask, render_template, request, redirect, url_for, send_from_directory, flash, session, jsonify
from flask import get_flashed_messages

app = Flask(__name__, template_folder='../templates')
app.secret_key = 'even_more_secret_key_for_robust_session' # MUST be set for session

# --- Configuration ---
CSV_FILE_PATH = '/Users/siddheshthakur/Work/Projects/Label-Check/2025-07-20/output_processed_latest.csv'
IMAGE_BASE_DIR = '/Users/siddheshthakur/Work/Projects/Label-Check/2025-07-20'
BACKUP_DIR = 'csv_backups' # Directory to store backups
INTERMEDIATE_BACKUP_FILE = r'/Users/siddheshthakur/Work/Projects/Label-Check/2025-07-20/output-ocr-2025-07-20_bkup.csv'
INTERMEDIATE_BACKUP_FREQUENCY = 10 # Save recovery file every 10 updates
# --- End Configuration ---


logging.info(f"INFO: Application starting... : {CSV_FILE_PATH}")

# --- Global Data Store & State ---
data = [] # In-memory list of dictionaries representing CSV rows
headers = []
# Session stores:
# session['show_only_incomplete'] = True/False
# session['update_counter'] = integer
# session['last_loaded_csv_mod_time'] = float (timestamp)
# --- End Global Data Store ---

# --- Helper Functions ---

def parse_original_line(line_str):
    # (Same as before - extracts identifier, label_text, macro_text)
    identifier = None
    label_text = "N/A"
    macro_text = "N/A"
    id_match = re.match(r'^([0-9a-fA-F\-]+_\d+)', line_str)
    if id_match:
        identifier = id_match.group(1)
    parts = line_str.split(';')
    for part in parts:
        part = part.strip()
        if part.startswith("Label:"):
            label_text = part[len("Label:"):].strip()
        elif part.startswith("Macro:"):
            macro_text = part[len("Macro:"):].strip()
            if macro_text.endswith('"'):
                macro_text = macro_text[:-1]
    return identifier, label_text, macro_text

def _is_row_incomplete(row_dict):
    """Checks the boolean '_is_complete' flag in a row dictionary."""
    return not row_dict.get('_is_complete', False) # Default to False if missing

def get_current_display_list_indices():
    """Returns a list of original_indices based on the current filter."""
    filter_active = session.get('show_only_incomplete', False)
    if not data:
        return []
    if filter_active:
        return [i for i, row in enumerate(data) if _is_row_incomplete(row)]
    else:
        return list(range(len(data))) # All indices

def get_display_info_for_original_index(original_index):
     """Given an original_index, find its position in the current display list."""
     display_indices = get_current_display_list_indices()
     try:
         # Find where original_index appears in the filtered list
         display_index = display_indices.index(original_index)
         return {
             'display_index': display_index, # 0-based position in the filtered list
             'total_display_count': len(display_indices)
         }
     except ValueError:
         # The requested original_index is not part of the current filter view
         return None

def find_navigation_index(current_original_index, direction):
    """
    Finds the next/previous original_index based on direction and filter.
    direction: 'next', 'prev', 'next_incorrect'
    Returns the target original_index or None if not found.
    """
    if not data: return None

    display_indices = get_current_display_list_indices()
    if not display_indices: return None # Nothing to navigate within

    try:
        current_display_pos = display_indices.index(current_original_index)
    except ValueError:
        # Current index isn't in the display list (shouldn't happen if called correctly)
        # Default to the first item in the display list
        return display_indices[0] if display_indices else None


    if direction == 'next':
        next_display_pos = current_display_pos + 1
        if next_display_pos < len(display_indices):
            return display_indices[next_display_pos]
        else:
            return None # At the end
    elif direction == 'prev':
        prev_display_pos = current_display_pos - 1
        if prev_display_pos >= 0:
            return display_indices[prev_display_pos]
        else:
            return None # At the beginning
    elif direction == 'next_incorrect':
        # Find the first original_index *after* current_original_index that is incomplete
        start_search = current_original_index + 1
        for i in range(start_search, len(data)):
            if _is_row_incomplete(data[i]):
                # Check if this index is *also* in the current display list (relevant if filter is on)
                if i in display_indices:
                     return i
        # If not found after, don't wrap around for simplicity
        flash("No further incorrect rows found.", "info")
        return None # Stay on current index if none found

    return None # Should not be reached

def load_csv_data(file_path=CSV_FILE_PATH):
    """
    Loads data from CSV into global variables `data` and `headers`.
    Uses print for logging, suitable for startup and request context.
    Returns True on success, False on failure. DOES NOT touch session.
    """
    global data, headers
    print(f"INFO: Attempting to load CSV data from: {file_path}") # Indicate loading attempt

    _data = []
    _headers = []
    required_headers = ['AccessionID', 'Stain', 'Complete', 'OriginalLine']

    try:
        # Check file existence explicitly first
        if not os.path.exists(file_path):
            print(f"ERROR: CSV file not found at '{file_path}'")
            return False # Return False early

        # Check modification time (for potential use in before_request, not strictly needed here)
        # current_mod_time = os.path.getmtime(file_path) # We don't compare here anymore

        with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=';')
            if not reader.fieldnames:
                print(f"ERROR: CSV file '{os.path.basename(file_path)}' appears empty or has no headers.")
                return False # Return False early
            _headers = reader.fieldnames
            if not all(h in _headers for h in required_headers):
                print(f"ERROR: CSV '{os.path.basename(file_path)}' missing required headers: {required_headers}")
                return False # Return False early

            for i, row in enumerate(reader):
                # Pre-process row (same as before)
                row['_original_index'] = i
                identifier, label, macro = parse_original_line(row.get('OriginalLine', ''))
                row['_identifier'] = identifier
                row['_label_text'] = label
                row['_macro_text'] = macro
                complete_val = str(row.get('Complete', '')).strip().lower()
                row['_is_complete'] = complete_val == 'true'
                row['AccessionID'] = row.get('AccessionID', '')
                row['Stain'] = row.get('Stain', '')
                _data.append(row)

        # --- Success: Update global variables ---
        data = _data
        headers = _headers
        print(f"INFO: Successfully loaded {len(data)} rows from {os.path.basename(file_path)}")
        return True

    # Specific exception for file not found (redundant due to check above, but safe)
    except FileNotFoundError:
        print(f"ERROR: CSV file not found during open operation at '{file_path}'")
        data = []
        headers = []
        return False
    # Catch other potential exceptions during file reading/processing
    except Exception as e:
        print(f"ERROR: Exception reading CSV '{os.path.basename(file_path)}': {e}")
        # Optionally print traceback for more detail during debugging
        # import traceback
        # traceback.print_exc()
        data = []
        headers = []
        return False

def save_csv_data(target_path=CSV_FILE_PATH):
    """Saves in-memory data to the specified path. Returns True on success."""
    global data, headers
    if not headers:
        flash("Error: Cannot save, headers not loaded.", "error")
        return False
    # Note: `data` can be empty, saving an empty file might be valid

    write_headers = [h for h in headers if not h.startswith('_')] # Exclude internal fields
    if not write_headers:
        flash("Error: No valid headers found to write.", "error")
        return False

    try:
        # Write to temp file first, then replace for atomicity
        temp_path = target_path + ".tmp"
        with open(temp_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=write_headers, delimiter=';', quoting=csv.QUOTE_NONNUMERIC)
            writer.writeheader()
            for row in data:
                write_row = {h: row.get(h, '') for h in write_headers}
                write_row['Complete'] = str(row.get('_is_complete', False)).capitalize()
                writer.writerow(write_row)

        os.replace(temp_path, target_path) # Atomic replace on most OS
        session['last_loaded_csv_mod_time'] = os.path.getmtime(target_path) # Update mod time
        return True
    except Exception as e:
        flash(f"Error saving data to {target_path}: {e}", "error")
        if os.path.exists(temp_path):
             try: os.remove(temp_path) # Clean up temp file
             except OSError: pass
        return False

def _create_backup():
    """Creates timestamped backup. Returns True on success."""
    if not os.path.exists(CSV_FILE_PATH):
        flash("Warning: Original CSV not found, cannot create backup.", "warning")
        return False
    try:
        if not os.path.exists(BACKUP_DIR): os.makedirs(BACKUP_DIR)
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_filename = f"{os.path.basename(CSV_FILE_PATH)}_{timestamp}.bak"
        backup_path = os.path.join(BACKUP_DIR, backup_filename)
        shutil.copy2(CSV_FILE_PATH, backup_path)
        flash(f"Backup created: {backup_path}", "info")
        return True
    except Exception as e:
        flash(f"Error creating backup: {e}", "error")
        return False

def save_intermediate_backup():
    """Saves current state to recovery file. Called periodically."""
    global data
    if not data: return # Nothing to save

    print(f"Attempting intermediate backup to {INTERMEDIATE_BACKUP_FILE}...") # Log attempt
    if save_csv_data(target_path=INTERMEDIATE_BACKUP_FILE):
        print(f"Intermediate backup successful.")
        session['update_counter'] = 0 # Reset counter ONLY on successful save
    else:
        # Error flashed by save_csv_data
        print(f"Intermediate backup FAILED.")

# --- Flask Routes ---

@app.before_request
def ensure_data_loaded():
    """
    Ensures data is loaded before each request (except static/images).
    Checks modification time and reloads if necessary. Handles session setup.
    """
    # Skip for static files and image serving endpoints
    if request.endpoint in ['static', 'serve_image']:
        return

    # Initialize session variables if they don't exist
    if 'show_only_incomplete' not in session:
        session['show_only_incomplete'] = False
    if 'update_counter' not in session:
        session['update_counter'] = 0
    if 'last_loaded_csv_mod_time' not in session:
        session['last_loaded_csv_mod_time'] = 0 # Initialize if not present

    # --- Check if reload is needed ---
    reload_needed = False
    if not data: # Always load if data is empty
        reload_needed = True
        print("INFO [Request]: Data is empty, attempting initial load.")
    else:
        # Check modification time against session
        try:
            current_mod_time = os.path.getmtime(CSV_FILE_PATH)
            if current_mod_time != session.get('last_loaded_csv_mod_time', 0):
                reload_needed = True
                print(f"INFO [Request]: CSV file '{os.path.basename(CSV_FILE_PATH)}' modified, reloading.")
        except FileNotFoundError:
             flash(f"Error: Main CSV file '{CSV_FILE_PATH}' not found during request.", "error")
             # Handle this case - maybe redirect to an error page or clear data?
             # For now, let the route handler potentially deal with empty data
             return
        except Exception as e:
             flash(f"Warning: Could not check CSV modification time: {e}", "warning")
             # Decide whether to force reload or continue with possibly stale data

    if reload_needed:
        # Call the revised load_csv_data (which uses print, not flash)
        if load_csv_data(file_path=CSV_FILE_PATH):
             # --- Update session ONLY on successful load WITHIN request context ---
             try:
                session['last_loaded_csv_mod_time'] = os.path.getmtime(CSV_FILE_PATH)
                session['update_counter'] = 0 # Reset counter on reload
                flash(f"Data reloaded successfully from {os.path.basename(CSV_FILE_PATH)}.", "info")
             except Exception as e:
                 flash(f"Error updating session after reload: {e}", "error")
        else:
            # Loading failed, flash error to user
            flash(f"CRITICAL: Failed to load/reload CSV data from '{CSV_FILE_PATH}'. Check file.", "critical")
            # Data might be empty now, route handlers need to cope or show error state

@app.route('/', methods=['GET'])
def index():
    if not data:
        # Loading failed in before_request or initial load
        return render_template('index.html', error_message="Failed to load CSV data. Please check file and logs.", data_loaded=False)

    filter_param = request.args.get('filter')
    target_original_index_str = request.args.get('index') # This now refers to original_index

    if filter_param is not None:
        session['show_only_incomplete'] = (filter_param == 'incomplete')
        # Go to first item in the new filter view when filter changes
        display_indices = get_current_display_list_indices()
        current_original_index = display_indices[0] if display_indices else 0
    else:
        # Try to use provided index, default to 0
        try:
            current_original_index = int(target_original_index_str) if target_original_index_str is not None else 0
        except ValueError:
            current_original_index = 0

    # Validate the requested original_index against available data
    if not (0 <= current_original_index < len(data)):
         flash(f"Requested index {current_original_index} out of bounds.", "warning")
         current_original_index = 0 # Reset to first item

    # Now, ensure the index is valid within the current filter
    display_indices = get_current_display_list_indices()
    if not display_indices:
         # Filter results in no items
         return render_template('index.html',
                                filter_active=session.get('show_only_incomplete'),
                                no_rows_after_filter=True,
                                total_original_rows=len(data),
                                data_loaded=True)

    if current_original_index not in display_indices:
         # Requested index exists but is filtered out, go to the first item in filter
         current_original_index = display_indices[0]
         flash("Requested item is filtered out, showing first available item.", "info")


    # Get the row data
    current_row = data[current_original_index]

    # Get display info (position in filtered list)
    display_info = get_display_info_for_original_index(current_original_index)
    if not display_info:
         # Should not happen if logic above is correct, but handle defensively
         flash("Error determining display position.", "error")
         display_info = {'display_index': 0, 'total_display_count': len(display_indices)}

    # Image path logic (remains the same)
    label_img_path, macro_img_path = None, None
    label_img_exists, macro_img_exists = False, False
    if current_row.get('_identifier'):
        base_id = current_row['_identifier']
        label_filename = f"{base_id}_label.png"
        macro_filename = f"{base_id}_macro.png"
        potential_label_path = os.path.join(IMAGE_BASE_DIR, 'label', label_filename)
        potential_macro_path = os.path.join(IMAGE_BASE_DIR, 'macro', macro_filename)
        if os.path.exists(potential_label_path):
            label_img_path = url_for('serve_image', subdir='label', filename=label_filename)
            label_img_exists = True
        if os.path.exists(potential_macro_path):
            macro_img_path = url_for('serve_image', subdir='macro', filename=macro_filename)
            macro_img_exists = True

    return render_template(
        'index.html',
        row=current_row,
        original_index=current_original_index, # The key reference
        display_index=display_info['display_index'], # Position in current view (0-based)
        total_display_count=display_info['total_display_count'], # Count in current view
        total_original_rows=len(data), # Total count in CSV
        label_img_path=label_img_path,
        macro_img_path=macro_img_path,
        label_img_exists=label_img_exists,
        macro_img_exists=macro_img_exists,
        filter_active=session.get('show_only_incomplete'),
        messages=flash_messages(), # Pass flash messages helper result
        data_loaded=True
    )


@app.route('/update', methods=['POST'])
def update():
    global data
    if not data:
        flash("Error: Data not loaded, cannot update.", "error")
        return redirect(url_for('index'))

    try:
        # The single source of truth for the item being operated on
        original_index_to_update = int(request.form['original_index'])
        action = request.form.get('action') # e.g., 'prev', 'next', 'next_incorrect'

        # Get submitted data
        submitted_accession_id = request.form.get('accession_id', '').strip()
        submitted_stain = request.form.get('stain', '').strip()
        submitted_complete_checked = request.form.get('complete') == 'on'

        # --- Validation ---
        # Apply validation ONLY IF trying to move FORWARD (Next or Next Incorrect)
        validation_failed = False
        if action in ['next', 'next_incorrect']:
            if not submitted_accession_id:
                flash("Validation Error: Accession ID cannot be empty to move forward.", "error")
                validation_failed = True
            if not submitted_stain:
                flash("Validation Error: Stain cannot be empty to move forward.", "error")
                validation_failed = True

            if validation_failed:
                # Stay on the current index if validation fails for forward movement
                flash("Please correct errors before moving forward.", "warning")
                # Need to redirect back to the *same* original_index
                return redirect(url_for('index', index=original_index_to_update))

        # --- Update Logic ---
        if 0 <= original_index_to_update < len(data):
            data_changed = False
            row_to_update = data[original_index_to_update]

            # Update fields if they changed
            if row_to_update.get('AccessionID') != submitted_accession_id:
                row_to_update['AccessionID'] = submitted_accession_id
                data_changed = True
            if row_to_update.get('Stain') != submitted_stain:
                row_to_update['Stain'] = submitted_stain
                data_changed = True

            # Handle 'Complete' checkbox - Enforce rules based on submitted data
            is_complete_target_state = False
            can_be_marked_complete = submitted_accession_id and submitted_stain
            if submitted_complete_checked:
                if can_be_marked_complete:
                    is_complete_target_state = True
                else:
                    is_complete_target_state = False # Force False if deps not met
                    flash("Cannot mark as Complete: Accession ID and Stain must be filled.", "warning")
            else:
                is_complete_target_state = False # User explicitly unchecked

            if row_to_update.get('_is_complete') != is_complete_target_state:
                 row_to_update['_is_complete'] = is_complete_target_state
                 row_to_update['Complete'] = str(is_complete_target_state).capitalize()
                 data_changed = True

            if data_changed:
                display_info = get_display_info_for_original_index(original_index_to_update)
                display_num = display_info['display_index'] + 1 if display_info else '?'
                flash(f"Item {display_num} (Original Index {original_index_to_update + 1}) updated locally.", "info")

                # Intermediate Backup Trigger
                session['update_counter'] = session.get('update_counter', 0) + 1
                if session['update_counter'] >= INTERMEDIATE_BACKUP_FREQUENCY:
                    save_intermediate_backup() # Resets counter on success

        else:
            flash("Error: Invalid original index received for update.", "error")
            return redirect(url_for('index')) # Go to default view on error

        # --- Navigation Logic ---
        target_original_index = original_index_to_update # Default: stay put if no action

        if action in ['next', 'prev', 'next_incorrect']:
            nav_index = find_navigation_index(original_index_to_update, action)
            if nav_index is not None:
                target_original_index = nav_index
            # else: stay on current index (already handled by find_navigation_index flash msg)

        return redirect(url_for('index', index=target_original_index))

    except Exception as e:
        flash(f"Critical Error processing update: {e}", "error")
        # Attempt to redirect back to a known state (e.g., index 0)
        return redirect(url_for('index'))

@app.route('/jump', methods=['POST'])
def jump():
    if not data: return redirect(url_for('index'))

    try:
        # Jump target is 1-based index *within the current display list*
        jump_target_display_index_1based = int(request.form.get('jump_to_index', '1'))
        jump_target_display_index_0based = jump_target_display_index_1based - 1

        display_indices = get_current_display_list_indices()
        if not display_indices:
             flash("No items to jump to in current view.", "warning")
             return redirect(url_for('index')) # Go to default

        if 0 <= jump_target_display_index_0based < len(display_indices):
            target_original_index = display_indices[jump_target_display_index_0based]
            flash(f"Jumped to item {jump_target_display_index_1based}.", "info")
            return redirect(url_for('index', index=target_original_index))
        else:
            flash(f"Invalid jump target: {jump_target_display_index_1based}. Please enter a number between 1 and {len(display_indices)}.", "error")
            # Stay on the current index (passed hidden)
            current_index = request.form.get('original_index', 0)
            return redirect(url_for('index', index=current_index))

    except ValueError:
        flash("Invalid input for jump target. Please enter a number.", "error")
        current_index = request.form.get('original_index', 0)
        return redirect(url_for('index', index=current_index))
    except Exception as e:
         flash(f"Error during jump: {e}", "error")
         return redirect(url_for('index'))


@app.route('/search', methods=['POST'])
def search():
    if not data: return redirect(url_for('index'))

    search_term = request.form.get('search_term', '').strip().lower()
    if not search_term:
        flash("Please enter a search term (Accession ID or Filename ID).", "warning")
        current_index = request.form.get('original_index', 0)
        return redirect(url_for('index', index=current_index))

    found_original_index = -1

    # Search logic: check AccessionID (exact match) and _identifier (exact match)
    for i, row in enumerate(data):
        # Check AccessionID first (case-insensitive)
        if row.get('AccessionID', '').lower() == search_term:
            found_original_index = i
            break
        # Then check identifier (case-insensitive)
        if row.get('_identifier', '').lower() == search_term:
             found_original_index = i
             break
        # Optional: Add partial match if needed later (more complex)
        # if search_term in row.get('AccessionID','').lower(): ...

    if found_original_index != -1:
         # Found it! Go to this original index.
         # Decide if we should force 'Show All' filter when searching? Often useful.
         session['show_only_incomplete'] = False # Force show all
         flash(f"Found matching item for '{search_term}'. Filter set to 'Show All'.", "info")
         return redirect(url_for('index', index=found_original_index))
    else:
         flash(f"No item found matching Accession ID or Filename ID '{search_term}'.", "warning")
         current_index = request.form.get('original_index', 0)
         return redirect(url_for('index', index=current_index)) # Stay put

@app.route('/save', methods=['POST'])
def save():
    # 1. Create Primary Backup
    if not _create_backup():
        flash("Save cancelled because backup failed.", "error")
    else:
        # 2. Attempt to Save main file
        if save_csv_data(target_path=CSV_FILE_PATH):
            flash(f"Data successfully saved to {CSV_FILE_PATH}", "success")
            session['update_counter'] = 0 # Reset intermediate counter
        else:
            # save_csv_data flashes the error
            flash(f"Data saving FAILED. Check logs. Recovery data might be in {INTERMEDIATE_BACKUP_FILE}", "critical") # Use stronger category

    # Redirect back to the current view after attempting save
    current_index = request.form.get('original_index', 0)
    return redirect(url_for('index', index=current_index))


# --- Image Serving & Flash Helper (Unchanged) ---
@app.route('/images/<subdir>/<path:filename>')
def serve_image(subdir, filename):
    if subdir not in ['label', 'macro']: return "Invalid image category", 404
    image_dir = os.path.join(IMAGE_BASE_DIR, subdir)
    if not os.path.isdir(IMAGE_BASE_DIR): return "Image directory not found.", 404
    try:
        return send_from_directory(image_dir, filename)
    except FileNotFoundError:
        return "Image file not found.", 404

def flash_messages():
    messages = []
    flashed = get_flashed_messages(with_categories=True)
    if flashed:
        for category, message in flashed:
            # Add custom class for critical messages
            css_class = category
            if category == 'critical': css_class += ' flash-critical'
            messages.append({'category': category, 'message': message, 'css_class': css_class})
    return messages

# --- Startup Logic ---
# --- Startup Logic ---
if __name__ == '__main__':
    print("INFO: Application starting...") # Add startup message

    # Initial check for main CSV file
    if not os.path.exists(CSV_FILE_PATH):
        print(f"CRITICAL ERROR: Main CSV file '{CSV_FILE_PATH}' not found. Application cannot start.")
        exit(1)

    # Attempt initial load using the modified load_csv_data (which now uses print)
    if not load_csv_data(): # Force load on first start
        print("INFO: Main CSV load failed. Attempting to load from recovery file...")
        if os.path.exists(INTERMEDIATE_BACKUP_FILE):
            # Attempt to load recovery file
            if load_csv_data(file_path=INTERMEDIATE_BACKUP_FILE):
                 print(f"INFO: Successfully loaded data from recovery file: {INTERMEDIATE_BACKUP_FILE}")
                 # We cannot FLASH here, but the user will see data when they connect.
                 # A message could be added to the index template if a 'loaded_from_recovery' flag was set globally.
            else:
                 print(f"CRITICAL ERROR: Failed to load from main CSV and recovery file '{INTERMEDIATE_BACKUP_FILE}'. Exiting.")
                 exit(1)
        else:
            print("CRITICAL ERROR: Failed to load main CSV and no recovery file found. Exiting.")
            exit(1)
    else:
        # This message is now printed inside load_csv_data on success
        # print(f"INFO: Successfully loaded main CSV: {CSV_FILE_PATH}")
        pass

    # Check for image directory (just a warning)
    if not os.path.exists(IMAGE_BASE_DIR):
        print(f"WARNING: Image base directory '{IMAGE_BASE_DIR}' not found. Image loading will fail.")

    print("INFO: Starting Flask development server...")
    # Use host='0.0.0.0' to make accessible on local network if needed
    app.run(debug=True, host='0.0.0.0')