import csv
import os
import re
import shutil
import datetime
from flask import Flask, render_template, request, redirect, url_for, send_from_directory, flash, session, jsonify
from flask import get_flashed_messages
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from collections import defaultdict, Counter # Added Counter
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__, template_folder='../templates')
app.secret_key = 'even_more_secret_key_for_robust_session' # MUST be set for session

# --- Configuration ---
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///users.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# !!! IMPORTANT: VERIFY THESE PATHS ARE CORRECT FOR YOUR SYSTEM !!!
CSV_FILE_PATH = r'Data/combined_data_ocr_processed.csv'
IMAGE_BASE_DIR = r'Data'
BACKUP_DIR = 'csv_backups'
INTERMEDIATE_BACKUP_FILE = r'Data/Data_ocr_bkup.csv'
INTERMEDIATE_BACKUP_FREQUENCY = 10
# --- End Configuration ---

# --- Flask-Login Setup ---
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'
login_manager.login_message = "Please log in to access this page."
login_manager.login_message_category = "warning"

# --- User Model ---
class User(UserMixin, db.Model):
    id = db.Column(db.String(80), primary_key=True, unique=True, nullable=False)
    password_hash = db.Column(db.String(128), nullable=False)
    correction_count = db.Column(db.Integer, default=0, nullable=False)
    is_admin = db.Column(db.Boolean, default=False, nullable=False)

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def verify_password(self, password):
        return check_password_hash(self.password_hash, password)

    def __repr__(self):
        return f'<User {self.id}>'

@login_manager.user_loader
def load_user(user_id):
    return User.query.get(user_id)

# --- Global Data Store ---
data = []
headers = []
# --- End Global Data Store ---

# --- Helper Functions ---

def _recalculate_accession_counts():
    """
    NEW: Iterates through all data to recount all accession IDs.
    This is the authoritative source of the count and ensures data integrity.
    """
    global data
    if not data:
        return

    # Use collections.Counter to efficiently count non-empty AccessionIDs
    accession_ids = [row.get('AccessionID', '').strip() for row in data]
    id_counts = Counter(aid for aid in accession_ids if aid) # Filter out empty strings

    # Update each row with the correct count
    for row in data:
        current_id = row.get('AccessionID', '').strip()
        if current_id:
            row['_accession_id_count'] = id_counts[current_id]
        else:
            # No count for empty IDs
            row['_accession_id_count'] = 0

def parse_original_line(line_str):
    """
    Parses the 'OriginalLine' string to extract the patient identifier,
    and the text from 'Label:' and 'Macro:' fields.
    """
    identifier = None
    label_text = "N/A"
    macro_text = "N/A"

    parts = line_str.split(';')
    if parts:
        identifier = parts[0].strip().replace('"', '')

    label_match = re.search(r'Label:\s*(.*?)(?:;Macro:|$)', line_str)
    if label_match:
        label_text = label_match.group(1).strip()
        if label_text.endswith('"'):
            label_text = label_text[:-1]

    macro_match = re.search(r'Macro:\s*(.*?)(?:;|$)', line_str)
    if macro_match:
        macro_text = macro_match.group(1).strip()
        if macro_text.endswith('"'):
            macro_text = macro_text[:-1]

    return identifier, label_text, macro_text

def _is_row_incomplete(row_dict):
    return not row_dict.get('_is_complete', False)

def get_current_display_list_indices():
    filter_active = session.get('show_only_incomplete', False)
    if not data:
        return []
    if filter_active:
        return [i for i, row in enumerate(data) if _is_row_incomplete(row)]
    else:
        return list(range(len(data)))

def get_display_info_for_original_index(original_index):
     display_indices = get_current_display_list_indices()
     try:
         display_index = display_indices.index(original_index)
         return {
             'display_index': display_index,
             'total_display_count': len(display_indices)
         }
     except ValueError:
         return None

def find_navigation_index(current_original_index, direction):
    if not data: return None
    display_indices = get_current_display_list_indices()
    if not display_indices: return None

    try:
        current_display_pos = display_indices.index(current_original_index)
    except ValueError:
        return display_indices[0] if display_indices else None

    if direction == 'next':
        next_display_pos = current_display_pos + 1
        return display_indices[next_display_pos] if next_display_pos < len(display_indices) else None
    elif direction == 'prev':
        prev_display_pos = current_display_pos - 1
        return display_indices[prev_display_pos] if prev_display_pos >= 0 else None
    elif direction == 'next_incorrect':
        for i in range(current_display_pos + 1, len(display_indices)):
            next_original_index_in_view = display_indices[i]
            if _is_row_incomplete(data[next_original_index_in_view]):
                return next_original_index_in_view
        for i in range(current_display_pos + 1):
             next_original_index_in_view = display_indices[i]
             if _is_row_incomplete(data[next_original_index_in_view]):
                 flash("No further incomplete rows. Restarting search.", "info")
                 return next_original_index_in_view
        flash("No incomplete rows found.", "info")
        return None
    return None

def load_csv_data(file_path=CSV_FILE_PATH):
    """
    Loads data from CSV. Includes logic for patient file counting and
    initial calculation of accession ID counts.
    """
    global data, headers
    print(f"INFO: Attempting to load CSV data from: {file_path}")

    _data = []
    _headers = []
    # UPDATED: Added BlockNumber. AccessionID_Count is calculated, but we read it for reference.
    required_headers = ['AccessionID', 'Stain', 'Complete', 'OriginalLine']

    try:
        if not os.path.exists(file_path):
            print(f"ERROR: CSV file not found at '{file_path}'")
            return False

        with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=';')
            _headers = reader.fieldnames
            if not _headers or not all(h in _headers for h in required_headers):
                print(f"ERROR: CSV missing required headers. Need at least: {required_headers}")
                return False

            for i, row in enumerate(reader):
                row['_original_index'] = i
                identifier, label, macro = parse_original_line(row.get('OriginalLine', ''))
                row['_identifier'] = identifier
                row['_label_text'] = label
                row['_macro_text'] = macro
                row['_is_complete'] = str(row.get('Complete', '')).strip().lower() == 'true'
                
                # UPDATED: Ensure new and existing fields are present, defaulting to empty.
                row['AccessionID'] = row.get('AccessionID', '')
                row['Stain'] = row.get('Stain', '')
                row['BlockNumber'] = row.get('BlockNumber', '') # NEW: Load BlockNumber
                _data.append(row)

        patient_slide_identifiers = defaultdict(list)
        for i, row in enumerate(_data):
            if row['_identifier']:
                patient_slide_identifiers[row['_identifier']].append(i)

        for identifier, original_indices in patient_slide_identifiers.items():
            total_for_patient = len(original_indices)
            original_indices.sort()
            for j, original_index in enumerate(original_indices):
                _data[original_index]['_total_patient_files'] = total_for_patient
                _data[original_index]['_patient_file_number'] = j + 1

        data = _data
        headers = _headers

        # NEW: After loading all data, run the authoritative count calculation.
        _recalculate_accession_counts()

        print(f"INFO: Successfully loaded {len(data)} rows from {os.path.basename(file_path)}")
        print("INFO: Initial accession ID counts calculated.")
        return True

    except Exception as e:
        print(f"ERROR: Exception reading CSV '{os.path.basename(file_path)}': {e}")
        data, headers = [], []
        return False

def save_csv_data(target_path=CSV_FILE_PATH):
    """Saves in-memory data to the specified path."""
    global data, headers
    if not headers:
        flash("Error: Cannot save, headers not loaded.", "error")
        return False

    # UPDATED: Added BlockNumber and AccessionID_Count to the core headers.
    core_headers = ['AccessionID', 'Stain', 'BlockNumber', 'Complete', 'AccessionID_Count', 'OriginalLine']
    
    other_original_headers = [h for h in headers if not h.startswith('_') and h not in core_headers]
    write_headers = core_headers + sorted(other_original_headers)

    try:
        temp_path = target_path + ".tmp"
        with open(temp_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=write_headers, delimiter=';', extrasaction='ignore', quoting=csv.QUOTE_NONNUMERIC)
            writer.writeheader()
            for row in data:
                write_row = row.copy() # Start with a copy
                # Ensure correct 'Complete' format
                write_row['Complete'] = str(row.get('_is_complete', False)).capitalize()
                # NEW: Ensure AccessionID_Count is taken from our internal calculated value
                write_row['AccessionID_Count'] = row.get('_accession_id_count', 0)
                writer.writerow(write_row)

        os.replace(temp_path, target_path)
        session['last_loaded_csv_mod_time'] = os.path.getmtime(target_path)
        return True
    except Exception as e:
        flash(f"Error saving data to {target_path}: {e}", "error")
        if os.path.exists(temp_path):
             try: os.remove(temp_path)
             except OSError: pass
        return False


def _create_backup():
    if not os.path.exists(CSV_FILE_PATH):
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
    global data
    if not data: return
    if save_csv_data(target_path=INTERMEDIATE_BACKUP_FILE):
        print(f"Intermediate backup successful to {INTERMEDIATE_BACKUP_FILE}.")
        session['update_counter'] = 0
    else:
        print(f"Intermediate backup FAILED.")

# --- Flask Routes ---

@app.before_request
def ensure_data_loaded():
    if request.endpoint in ['static', 'serve_image', 'login', 'logout', 'add_user', 'users_management']:
        return
    if 'show_only_incomplete' not in session: session['show_only_incomplete'] = False
    if 'update_counter' not in session: session['update_counter'] = 0
    if 'last_loaded_csv_mod_time' not in session: session['last_loaded_csv_mod_time'] = 0

    reload_needed = False
    if not data:
        reload_needed = True
    else:
        try:
            if os.path.getmtime(CSV_FILE_PATH) != session.get('last_loaded_csv_mod_time', 0):
                reload_needed = True
        except FileNotFoundError:
             flash(f"Error: Main CSV file '{CSV_FILE_PATH}' not found.", "error")
             return
        except Exception as e:
             flash(f"Warning: Could not check CSV modification time: {e}", "warning")

    if reload_needed:
        if load_csv_data():
             try:
                session['last_loaded_csv_mod_time'] = os.path.getmtime(CSV_FILE_PATH)
                session['update_counter'] = 0
                flash(f"Data reloaded from {os.path.basename(CSV_FILE_PATH)}.", "info")
             except Exception as e:
                 flash(f"Error updating session after reload: {e}", "error")
        else:
            flash(f"CRITICAL: Failed to load CSV data from '{CSV_FILE_PATH}'.", "critical")

# ... (login, logout, users_management, add_user routes are unchanged) ...
@app.route('/login', methods=['GET', 'POST'])
def login():
    """Handles user login."""
    if current_user.is_authenticated:
        return redirect(url_for('index'))

    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']

        user = User.query.get(username) # Get user from DB
        if user and user.verify_password(password):
            login_user(user)
            flash('Logged in successfully.', 'success')
            next_page = request.args.get('next')
            return redirect(next_page or url_for('index'))
        else:
            flash('Invalid username or password.', 'error')
    return render_template('login.html', messages=flash_messages())

@app.route('/logout')
@login_required
def logout():
    """Logs out the current user."""
    logout_user()
    flash('You have been logged out.', 'info')
    return redirect(url_for('login'))

@app.route('/users')
@login_required
def users_management():
    """Displays user management page (admin only)."""
    if not current_user.is_admin:
        flash("You do not have permission to view user management.", "error")
        return redirect(url_for('index'))
    all_users = User.query.all()
    return render_template('users.html', users=all_users, messages=flash_messages())

@app.route('/add_user', methods=['POST'])
@login_required
def add_user():
    """Handles adding a new user (admin only)."""
    if not current_user.is_admin:
        flash("You do not have permission to add users.", "error")
        return redirect(url_for('index'))

    username = request.form.get('username').strip()
    password = request.form.get('password').strip()
    is_admin = request.form.get('is_admin') == 'on' # Checkbox value

    if not username or not password:
        flash("Username and Password cannot be empty.", "error")
        return redirect(url_for('users_management'))

    if User.query.get(username):
        flash(f"User '{username}' already exists.", "error")
        return redirect(url_for('users_management'))

    try:
        new_user = User(id=username, is_admin=is_admin)
        new_user.set_password(password)
        db.session.add(new_user)
        db.session.commit()
        flash(f"User '{username}' added successfully.", "success")
    except Exception as e:
        db.session.rollback()
        flash(f"Error adding user: {e}", "error")
    return redirect(url_for('users_management'))
@app.route('/', methods=['GET'])
@login_required
def index():
    if not data:
        return render_template('index.html', error_message="Failed to load CSV data.", data_loaded=False, messages=flash_messages())

    target_original_index_str = request.args.get('index')
    if request.args.get('filter') is not None:
        session['show_only_incomplete'] = (request.args.get('filter') == 'incomplete')
        display_indices = get_current_display_list_indices()
        current_original_index = display_indices[0] if display_indices else 0
    else:
        current_original_index = int(target_original_index_str) if target_original_index_str else 0

    if not (0 <= current_original_index < len(data)):
         current_original_index = 0

    display_indices = get_current_display_list_indices()
    if not display_indices:
         return render_template('index.html', filter_active=session.get('show_only_incomplete'), no_rows_after_filter=True, total_original_rows=len(data), data_loaded=True, messages=flash_messages())

    if current_original_index not in display_indices:
         current_original_index = display_indices[0]
         flash("Requested item filtered out, showing first available item.", "info")

    current_row = data[current_original_index]
    display_info = get_display_info_for_original_index(current_original_index) or {'display_index': 0, 'total_display_count': len(display_indices)}

    label_img_path, macro_img_path = None, None
    label_img_exists, macro_img_exists = False, False
    if current_row.get('_identifier'):
        base_id = current_row['_identifier']
        label_filename, macro_filename = f"{base_id}_label.png", f"{base_id}_macro.png"
        if os.path.exists(os.path.join(IMAGE_BASE_DIR, 'label', label_filename)):
            label_img_path, label_img_exists = url_for('serve_image', subdir='label', filename=label_filename), True
        if os.path.exists(os.path.join(IMAGE_BASE_DIR, 'macro', macro_filename)):
            macro_img_path, macro_img_exists = url_for('serve_image', subdir='macro', filename=macro_filename), True

    return render_template('index.html', row=current_row, original_index=current_original_index, display_index=display_info['display_index'], total_display_count=display_info['total_display_count'], total_original_rows=len(data), label_img_path=label_img_path, macro_img_path=macro_img_path, label_img_exists=label_img_exists, macro_img_exists=macro_img_exists, filter_active=session.get('show_only_incomplete'), messages=flash_messages(), data_loaded=True)


@app.route('/update', methods=['POST'])
@login_required
def update():
    """Handles updating record data, including BlockNumber and Accession ID counts."""
    global data
    if not data:
        flash("Error: Data not loaded, cannot update.", "error")
        return redirect(url_for('index'))

    try:
        original_index = int(request.form['original_index'])
        action = request.form.get('action')

        # --- Get all submitted form data ---
        submitted_accession_id = request.form.get('accession_id', '').strip()
        submitted_stain = request.form.get('stain', '').strip()
        submitted_block_number = request.form.get('block_number', '').strip() # NEW
        submitted_complete_checked = request.form.get('complete') == 'on'

        # --- Validation ---
        if action in ['next', 'next_incorrect'] and (not submitted_accession_id or not submitted_stain):
            flash("Accession ID and Stain cannot be empty to move forward.", "error")
            return redirect(url_for('index', index=original_index))

        if 0 <= original_index < len(data):
            row = data[original_index]
            data_changed = False
            accession_id_changed = False # NEW: Flag to trigger recalculation

            # --- Check for changes and update data ---
            if row.get('AccessionID') != submitted_accession_id:
                row['AccessionID'] = submitted_accession_id
                data_changed = True
                accession_id_changed = True # IMPORTANT: Set flag

            if row.get('Stain') != submitted_stain:
                row['Stain'] = submitted_stain
                data_changed = True

            if row.get('BlockNumber') != submitted_block_number: # NEW
                row['BlockNumber'] = submitted_block_number
                data_changed = True

            # --- Handle 'Complete' status logic ---
            is_complete_target = False
            can_be_complete = bool(submitted_accession_id and submitted_stain)
            if submitted_complete_checked:
                if can_be_complete:
                    is_complete_target = True
                else:
                    flash("Cannot mark Complete: Accession ID and Stain must be filled.", "warning")
            
            if row.get('_is_complete') != is_complete_target:
                 row['_is_complete'] = is_complete_target
                 row['Complete'] = str(is_complete_target).capitalize()
                 data_changed = True

            # --- Post-update actions ---
            if data_changed:
                flash(f"Item {get_display_info_for_original_index(original_index)['display_index'] + 1} updated.", "info")
                try:
                    current_user.correction_count += 1
                    db.session.commit()
                except Exception as e:
                    db.session.rollback()
                    flash(f"Error updating correction count: {e}", "error")

                session['update_counter'] = session.get('update_counter', 0) + 1
                if session['update_counter'] >= INTERMEDIATE_BACKUP_FREQUENCY:
                    save_intermediate_backup()

            # NEW: If Accession ID changed, recalculate counts for the whole dataset
            if accession_id_changed:
                _recalculate_accession_counts()
                flash("Accession ID count updated.", "info")

        else:
            flash("Error: Invalid index received for update.", "error")
            return redirect(url_for('index'))

        # --- Navigation ---
        target_index = original_index
        if action in ['next', 'prev', 'next_incorrect']:
            nav_index = find_navigation_index(original_index, action)
            if nav_index is not None:
                target_index = nav_index

        return redirect(url_for('index', index=target_index))

    except Exception as e:
        flash(f"Critical Error processing update: {e}", "error")
        return redirect(url_for('index'))

# ... (jump, search, save, serve_image, flash_messages routes are unchanged) ...
@app.route('/jump', methods=['POST'])
@login_required
def jump():
    """Handles jumping to a specific item in the display queue."""
    if not data: return redirect(url_for('index'))

    try:
        jump_target_display_index_1based = int(request.form.get('jump_to_index', '1'))
        jump_target_display_index_0based = jump_target_display_index_1based - 1

        display_indices = get_current_display_list_indices()
        if not display_indices:
             flash("No items to jump to in current view.", "warning")
             return redirect(url_for('index'))

        if 0 <= jump_target_display_index_0based < len(display_indices):
            target_original_index = display_indices[jump_target_display_index_0based]
            flash(f"Jumped to item {jump_target_display_index_1based}.", "info")
            return redirect(url_for('index', index=target_original_index))
        else:
            flash(f"Invalid jump target: {jump_target_display_index_1based}. Please enter a number between 1 and {len(display_indices)}.", "error")
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
@login_required
def search():
    """Handles searching for a specific Accession ID or Patient Identifier."""
    if not data: return redirect(url_for('index'))

    search_term = request.form.get('search_term', '').strip()
    if not search_term:
        flash("Please enter a search term (Accession ID or Filename ID).", "warning")
        current_index = request.form.get('original_index', 0)
        return redirect(url_for('index', index=current_index))

    found_original_index = -1

    for i, row in enumerate(data):
        # Case-insensitive search
        if row.get('AccessionID', '').lower() == search_term.lower():
            found_original_index = i
            break
        if row.get('_identifier', '').lower() == search_term.lower():
             found_original_index = i
             break

    if found_original_index != -1:
         session['show_only_incomplete'] = False # Typically useful to show all when searching
         flash(f"Found matching item for '{search_term}'. Filter set to 'Show All'.", "info")
         return redirect(url_for('index', index=found_original_index))
    else:
         flash(f"No item found matching Accession ID or Filename ID '{search_term}'.", "warning")
         current_index = request.form.get('original_index', 0)
         return redirect(url_for('index', index=current_index))

@app.route('/save', methods=['POST'])
@login_required
def save():
    """Handles saving all in-memory data back to the primary CSV file, with backup."""
    if not _create_backup():
        flash("Save cancelled because backup failed.", "error")
    else:
        if save_csv_data(target_path=CSV_FILE_PATH):
            flash(f"Data successfully saved to {CSV_FILE_PATH}", "success")
            session['update_counter'] = 0
        else:
            flash(f"Data saving FAILED. Check logs. Recovery data might be in {INTERMEDIATE_BACKUP_FILE}", "critical")

    current_index = request.form.get('original_index', 0)
    return redirect(url_for('index', index=current_index))
@app.route('/images/<subdir>/<path:filename>')
def serve_image(subdir, filename):
    """Serves image files from specified subdirectories."""
    print(f"DEBUG: serve_image called with subdir={subdir}, filename={filename}")
    print(f"DEBUG: IMAGE_BASE_DIR={IMAGE_BASE_DIR}")
    print(f"DEBUG: Current working directory={os.getcwd()}")
    
    if subdir not in ['label', 'macro', 'thumbnail']: 
        print(f"DEBUG: Invalid subdir {subdir}")
        return "Invalid image category", 404
    
    image_dir = os.path.join(IMAGE_BASE_DIR, subdir)
    print(f"DEBUG: image_dir={image_dir}")
    print(f"DEBUG: image_dir exists={os.path.exists(image_dir)}")
    print(f"DEBUG: image_dir isdir={os.path.isdir(image_dir)}")
    
    if not os.path.isdir(image_dir):
        print(f"DEBUG: Image directory {image_dir} not found")
        return f"Image directory for {subdir} not found.", 404
    
    full_path = os.path.join(image_dir, filename)
    print(f"DEBUG: full_path={full_path}")
    print(f"DEBUG: full_path exists={os.path.exists(full_path)}")
    
    if not os.path.exists(full_path):
        print(f"DEBUG: File does not exist at {full_path}")
        return "Image file not found.", 404
    
    try:
        # Use absolute path for the directory to avoid path resolution issues
        abs_image_dir = os.path.abspath(image_dir)
        print(f"DEBUG: Using absolute path: {abs_image_dir}")
        return send_from_directory(abs_image_dir, filename)
    except Exception as e:
        print(f"DEBUG: Exception serving file {filename} from {image_dir}: {e}")
        return "Error serving image file.", 500

def flash_messages():
    """Helper to retrieve and format flash messages."""
    messages = []
    flashed = get_flashed_messages(with_categories=True)
    if flashed:
        for category, message in flashed:
            css_class = category
            if category == 'critical': css_class += ' flash-critical'
            messages.append({'category': category, 'message': message, 'css_class': css_class})
    return messages
# --- Startup Logic ---
if __name__ == '__main__':
    print("INFO: Application starting...")

    with app.app_context():
        db.create_all()
        if not User.query.get('admin'):
            admin_user_obj = User(id='admin', is_admin=True)
            admin_user_obj.set_password('<something@great>')
            db.session.add(admin_user_obj)
            db.session.commit()
            print("INFO: Admin user 'admin' created. PLEASE CHANGE THE PASSWORD!")
        else:
            print("INFO: Admin user 'admin' already exists.")

    if not os.path.exists(CSV_FILE_PATH):
        print(f"CRITICAL: Main CSV '{CSV_FILE_PATH}' not found. Attempting recovery...")
        if os.path.exists(INTERMEDIATE_BACKUP_FILE):
            try:
                shutil.copy2(INTERMEDIATE_BACKUP_FILE, CSV_FILE_PATH)
                print(f"INFO: Recovery successful from '{INTERMEDIATE_BACKUP_FILE}'.")
            except Exception as e:
                print(f"CRITICAL: Failed to copy recovery file: {e}. Exiting.")
                exit(1)
        else:
            print("CRITICAL: No main CSV and no recovery file found. Exiting.")
            exit(1)

    if not load_csv_data():
        print("CRITICAL: Initial data load failed. Exiting.")
        exit(1)
    else:
        print(f"INFO: Application loaded with {len(data)} records.")

    if not os.path.exists(IMAGE_BASE_DIR):
        print(f"WARNING: Image base directory '{IMAGE_BASE_DIR}' not found.")

    print("INFO: Starting Flask development server...")
    app.run(debug=True, host='0.0.0.0')