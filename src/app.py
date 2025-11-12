"""
This script implements a Flask web application designed for the manual review and correction
of data extracted from whole-slide images (WSIs). It provides a user interface for operators
to verify and amend information like Accession IDs and Stain types that have been processed
by an automated pipeline (e.g., OCR).

The application features:
- User authentication (login/logout) with role-based access (standard user vs. admin).
- An admin panel for user management (adding new users).
- A robust queuing system that "leases" data rows to users for a fixed duration to prevent
  simultaneous edits. Expired leases are automatically returned to the queue.
- Dynamic loading and saving of data from/to a central CSV file.
- Automatic creation of backups before saving any changes.
- A user-friendly interface displaying slide images (macro/label) and form fields for data entry.
- Logic to pre-fill information based on other slides from the same patient/case.
- A command-line interface (CLI) for initializing the database and user accounts.
"""

# ==============================================================================
# 1. IMPORTS
# ==============================================================================
import csv
import datetime
import logging
import os
import shutil
from collections import Counter, defaultdict
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

# Flask and its extensions for web framework, user management, and database
from flask import (
    Flask,
    flash,
    get_flashed_messages,
    redirect,
    render_template,
    request,
    send_from_directory,
    session,
    url_for,
)
from flask.cli import with_appcontext
from flask_login import (
    LoginManager,
    UserMixin,
    current_user,
    login_required,
    login_user,
    logout_user,
)
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func
from werkzeug.security import check_password_hash, generate_password_hash


# ==============================================================================
# 2. CONFIGURATION
# ==============================================================================
class Config:
    """Central configuration class for the Flask application."""

    # A secret key is required for session management and security.
    # It should be a long, random string and kept secret in production.
    SECRET_KEY = os.environ.get(
        "SECRET_KEY", "a-super-secret-key-that-you-should-change"
    )

    # Database URI. For this app, we use a simple SQLite database file.
    SQLALCHEMY_DATABASE_URI = "sqlite:///users.db"
    # Disable an SQLAlchemy feature that is not needed and adds overhead.
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # The base directory where all data (images, CSV) is located.
    # The path is relative to the location of this script.
    IMAGE_BASE_DIR = r"..\NP-22-data"
    # The full path to the primary CSV file that the application reads from and writes to.
    CSV_FILE_PATH = os.path.join(IMAGE_BASE_DIR, "ocr_processed_parsed.csv")
    # Directory to store timestamped backups of the CSV before saving.
    BACKUP_DIR = "csv_backups"

    # Default password for the initial 'admin' user.
    # It is highly recommended to use an environment variable for this in production.
    ADMIN_DEFAULT_PASSWORD = os.environ.get(
        "ADMIN_DEFAULT_PASSWORD", "change_this_password"
    )

    # --- Queue Settings ---
    # The duration (in seconds) a user can hold a "lease" on a queue item before it's
    # automatically returned to the pool for others.
    LEASE_DURATION_SECONDS = 300  # 5 minutes


# ==============================================================================
# 3. LOGGING SETUP
# ==============================================================================
def setup_logging(app: Flask) -> None:
    """Configures comprehensive logging for the application.

    This setup creates two log handlers:
    1. A RotatingFileHandler to save detailed logs to a file (`logs/app.log`),
       which automatically rotates when the file size limit is reached.
    2. A StreamHandler to print informative logs to the console.
    """
    if not os.path.exists("logs"):
        os.mkdir("logs")

    # Handler for writing logs to a file.
    file_handler = RotatingFileHandler("logs/app.log", maxBytes=102400, backupCount=10)
    file_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]"
        )
    )
    file_handler.setLevel(logging.INFO)

    # Handler for printing logs to the console.
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    )
    console_handler.setLevel(logging.INFO)

    # Add both handlers to the Flask application's logger.
    app.logger.addHandler(file_handler)
    app.logger.addHandler(console_handler)
    app.logger.setLevel(logging.INFO)
    app.logger.info("Application startup")


# ==============================================================================
# 4. APPLICATION & EXTENSIONS INITIALIZATION
# ==============================================================================
# --- Path Setup ---
# Determine absolute paths for the application's root, instance, and template folders.
# This makes the app runnable from any directory.
base_dir = os.path.abspath(os.path.dirname(__file__))
# The 'instance' folder is where Flask stores instance-specific files like the SQLite DB.
instance_path = os.path.join(base_dir, "instance")
template_dir = os.path.join(base_dir, "templates")

# --- Flask App Initialization ---
app = Flask(__name__, template_folder=template_dir, instance_path=instance_path)
app.config.from_object(Config)

# Ensure the instance directory exists; SQLAlchemy needs it to create the database file.
os.makedirs(app.instance_path, exist_ok=True)

# Initialize logging for the application.
setup_logging(app)

# --- Extensions Initialization ---
# Initialize SQLAlchemy for database operations.
db = SQLAlchemy(app)
# Initialize Flask-Login for handling user sessions.
login_manager = LoginManager()
login_manager.init_app(app)
# Redirect users to the 'login' page if they try to access a protected page without being logged in.
login_manager.login_view = "login"
login_manager.login_message = "Please log in to access this page."
login_manager.login_message_category = "warning"


# ==============================================================================
# 5. CUSTOM EXCEPTIONS
# ==============================================================================
class DataLoadError(Exception):
    """Custom exception raised for errors during the CSV data loading process."""
    pass


class DataSaveError(Exception):
    """Custom exception raised for errors during the CSV data saving process."""
    pass


class BackupError(Exception):
    """Custom exception raised for errors during the backup creation process."""
    pass


# ==============================================================================
# 6. DATABASE MODELS (Using Flask-SQLAlchemy)
# ==============================================================================
class User(UserMixin, db.Model):
    """Represents a user account in the database."""
    # The user's unique identifier (e.g., username).
    id = db.Column(db.String(80), primary_key=True, unique=True, nullable=False)
    # The hashed password for security.
    password_hash = db.Column(db.String(128), nullable=False)
    # A simple counter for user activity/stats.
    correction_count = db.Column(db.Integer, default=0, nullable=False)
    # A boolean flag to determine if the user has administrative privileges.
    is_admin = db.Column(db.Boolean, default=False, nullable=False)

    def set_password(self, password: str) -> None:
        """Hashes the provided password and stores it."""
        self.password_hash = generate_password_hash(password)

    def verify_password(self, password: str) -> bool:
        """Checks if the provided password matches the stored hash."""
        return check_password_hash(self.password_hash, password)

    def __repr__(self) -> str:
        return f"<User {self.id}>"


class QueueItem(db.Model):
    """Represents a single row from the CSV in the processing queue.

    This model tracks the state of each item (e.g., slide) to prevent multiple
    users from working on the same item simultaneously.
    """
    id = db.Column(db.Integer, primary_key=True)
    # The original 0-based index of the row in the CSV file. This links the
    # queue item back to the in-memory data.
    original_index = db.Column(db.Integer, nullable=False, index=True, unique=True)
    # The current status of the item:
    # - 'pending': Available for any user to take.
    # - 'leased': Currently assigned to a user.
    # - 'completed': Work is finished for this item.
    status = db.Column(db.String(20), nullable=False, default="pending", index=True)
    # Foreign key to the User who has leased this item.
    leased_by_id = db.Column(db.String(80), db.ForeignKey("user.id"), nullable=True, index=True)
    # Timestamp of when the lease was granted.
    leased_at = db.Column(db.DateTime, nullable=True)
    # Foreign key to the User who completed this item.
    completed_by_id = db.Column(db.String(80), db.ForeignKey("user.id"), nullable=True, index=True)
    # Timestamp of when the item was marked as complete.
    completed_at = db.Column(db.DateTime, nullable=True)

    # SQLAlchemy relationships to easily access the User objects.
    leased_by = db.relationship("User", foreign_keys=[leased_by_id], backref="leases")
    completed_by = db.relationship("User", foreign_keys=[completed_by_id], backref="completed_items")

    def __repr__(self) -> str:
        return f"<QueueItem {self.original_index} - {self.status}>"


@login_manager.user_loader
def load_user(user_id: str) -> Optional[User]:
    """Flask-Login callback function to load a user from the database by their ID."""
    return db.session.get(User, user_id)


# ==============================================================================
# 7. GLOBAL DATA STORE
# ==============================================================================
# In-memory storage for the CSV data. This is simple and fast but has a key limitation:
# it is NOT safe for production environments with multiple workers (like Gunicorn),
# as each worker would have its own separate copy of the data.
# This approach is suitable for single-worker development or small-scale deployments.
data: List[Dict[str, Any]] = []
headers: List[str] = []


# ==============================================================================
# 8. HELPER FUNCTIONS
# ==============================================================================
def _release_expired_leases():
    """Scans for and releases any item leases that have expired.

    This function is a critical part of the queue system. It finds all items with a
    'leased' status whose `leased_at` timestamp is older than the configured lease
    duration and resets their status to 'pending'.
    """
    lease_duration = datetime.timedelta(seconds=app.config["LEASE_DURATION_SECONDS"])
    expired_time = datetime.datetime.utcnow() - lease_duration

    # Find all items that are leased and whose lease time has passed.
    expired_items = db.session.execute(
        db.select(QueueItem).filter(
            QueueItem.status == "leased", QueueItem.leased_at < expired_time
        )
    ).scalars().all()

    if expired_items:
        count = 0
        for item in expired_items:
            # A safety check to prevent errors if the CSV was reloaded and is now shorter.
            if item.original_index < len(data):
                acc_id = data[item.original_index].get("AccessionID", "Unknown")
                app.logger.info(
                    f"Lease expired for item {item.original_index} ({acc_id}), leased by {item.leased_by_id}."
                )
                # Reset the item's state.
                item.status = "pending"
                item.leased_by_id = None
                item.leased_at = None
                count += 1
        db.session.commit()
        if count > 0:
            flash(
                f"{count} item(s) had expired leases and were returned to the queue.",
                "warning",
            )


def _recalculate_accession_counts() -> None:
    """Recalculates and updates the usage count for each AccessionID.

    This function iterates through all rows in the global `data` list, counts the
    occurrences of each AccessionID, and stores the count in a special `_accession_id_count`
    field. This is used in the UI to alert users if an ID is used multiple times.
    """
    global data
    if not data:
        return

    # Use collections.Counter for an efficient way to count hashable objects.
    id_counts = Counter(
        row.get("AccessionID", "").strip()
        for row in data
        if row.get("AccessionID", "").strip()
    )
    # Update each row with the calculated count.
    for row in data:
        current_id = row.get("AccessionID", "").strip()
        row["_accession_id_count"] = id_counts[current_id] if current_id else 0


def _is_row_incomplete(row_dict: Dict[str, Any]) -> bool:
    """Checks if a data row is marked as incomplete based on internal metadata."""
    return not row_dict.get("_is_complete", False)


def get_current_display_list_indices() -> List[int]:
    """Returns a list of original data indices to be displayed.

    This function respects the user's current filter preference, which is stored
    in the session. It returns all indices if the user wants to see all items, or
    only the indices of incomplete items.
    """
    if not data:
        return []
    if session.get("show_only_incomplete"):
        return [i for i, row in enumerate(data) if _is_row_incomplete(row)]
    return list(range(len(data)))


# ==============================================================================
# 9. CORE DATA I/O FUNCTIONS
# ==============================================================================
def load_csv_data(file_path: str = Config.CSV_FILE_PATH) -> None:
    """Loads and processes data from the specified CSV file into global memory.

    This function reads the CSV, enriches each row with internal metadata
    (like `_original_index`, `_identifier`, `_is_complete`), calculates
    patient-level statistics, and populates the global `data` and `headers` lists.
    """
    global data, headers
    app.logger.info(f"Loading CSV data from: {file_path}")

    if not os.path.exists(file_path):
        raise DataLoadError(f"CSV file not found: {file_path}")

    _data: List[Dict[str, Any]] = []
    # These headers are essential for the application to function correctly.
    critical_headers = [
        "AccessionID", "Stain", "ParsingQCPassed", "original_slide_location"
    ]

    try:
        with open(file_path, "r", newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile, delimiter=",")
            _headers = reader.fieldnames

            if not _headers:
                raise DataLoadError("CSV file is empty or has no header.")
            
            # Check for missing critical headers and log a warning if any are found.
            missing = [h for h in critical_headers if h not in _headers]
            if missing:
                app.logger.warning(
                    f"CSV is missing expected headers: {missing}. Functionality may be limited."
                )

            for i, row in enumerate(reader):
                # --- Add Internal Metadata (prefixed with '_') ---
                # Store the original row number.
                row["_original_index"] = i
                # Create a unique identifier for the patient/case from the filename stem.
                orig_path = row.get("original_slide_location")
                row["_identifier"] = Path(orig_path).stem if orig_path else f"Unknown_{i}"
                # Map OCR text columns to internal keys for consistent access.
                row["_label_text"] = row.get("label_text", "N/A")
                row["_macro_text"] = row.get("macro_text", "N/A")
                # Map image path columns to internal keys.
                row["_label_path"] = row.get("label_path")
                row["_macro_path"] = row.get("macro_path")
                
                # --- Standardize Editable Fields ---
                # Ensure these fields exist and strip whitespace.
                row["AccessionID"] = row.get("AccessionID", "").strip()
                row["Stain"] = row.get("Stain", "").strip()
                # The pipeline may not produce a BlockNumber, but we add the key to allow user input.
                row["BlockNumber"] = row.get("BlockNumber", "").strip()
                
                # --- Determine Completion Status ---
                # Convert the 'ParsingQCPassed' column (e.g., "TRUE") to a boolean.
                qc_passed_str = row.get("ParsingQCPassed", "").strip()
                row["_is_complete"] = bool(qc_passed_str)
                _data.append(row)

        # --- Post-processing: Calculate per-patient file statistics ---
        # Group rows by the patient identifier.
        patient_slide_ids = defaultdict(list)
        for i, row in enumerate(_data):
            patient_slide_ids[row["_identifier"]].append(i)
        
        # Annotate each row with its position within its patient group (e.g., "File 1 of 3").
        for _, original_indices in patient_slide_ids.items():
            total = len(original_indices)
            for j, original_idx in enumerate(sorted(original_indices)):
                _data[original_idx]["_total_patient_files"] = total
                _data[original_idx]["_patient_file_number"] = j + 1

        # Atomically update the global variables.
        data, headers = _data, _headers
        _recalculate_accession_counts()
        app.logger.info(f"Loaded {len(data)} rows.")

    except Exception as e:
        # If loading fails, clear the global data to prevent using stale/corrupt data.
        data, headers = [], []
        raise DataLoadError(f"Error reading CSV: {e}")


def save_csv_data(target_path: str = Config.CSV_FILE_PATH) -> None:
    """Saves the current in-memory data back to the CSV file.

    This function performs an atomic write by first writing to a temporary file
    and then replacing the original file. This prevents data corruption if the
    application crashes during the save operation.
    """
    global data, headers
    if not data or not headers:
        app.logger.warning("Save aborted: No data in memory.")
        return

    app.logger.info(f"Saving {len(data)} rows to {target_path}")

    # Define the desired order of columns in the output CSV file.
    priority_fields = ["AccessionID", "Stain", "BlockNumber", "ParsingQCPassed"]
    # Get all other original headers.
    pipeline_fields = [h for h in headers if h not in priority_fields]
    # Combine them, ensuring no duplicates and preserving order.
    fieldnames = list(dict.fromkeys(priority_fields + pipeline_fields))
    
    # Use a temporary file for the initial write.
    temp_path = target_path + ".tmp"
    try:
        with open(temp_path, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(
                csvfile,
                fieldnames=fieldnames,
                delimiter=",",
                extrasaction="ignore",  # Ignore our internal '_' fields.
                quoting=csv.QUOTE_MINIMAL,
            )
            writer.writeheader()

            for row in data:
                write_row = row.copy()
                # Map the internal `_is_complete` boolean back to the CSV string representation.
                write_row["ParsingQCPassed"] = "TRUE" if row.get("_is_complete") else ""
                writer.writerow(write_row)

        # If writing to temp file succeeds, replace the original file. This is an atomic operation.
        os.replace(temp_path, target_path)
        # Update the last modified time in the session to prevent immediate auto-reloading.
        session["last_loaded_csv_mod_time"] = os.path.getmtime(target_path)
        app.logger.info("Save successful.")

    except Exception as e:
        # If an error occurs, clean up the temporary file.
        if os.path.exists(temp_path):
            os.remove(temp_path)
        raise DataSaveError(f"Failed to save CSV: {e}")


def _create_backup() -> None:
    """Creates a timestamped backup of the current CSV file."""
    if not os.path.exists(Config.CSV_FILE_PATH):
        return
    try:
        os.makedirs(Config.BACKUP_DIR, exist_ok=True)
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = os.path.basename(Config.CSV_FILE_PATH)
        backup_path = os.path.join(Config.BACKUP_DIR, f"{filename}_{timestamp}.bak")
        shutil.copy2(Config.CSV_FILE_PATH, backup_path)
    except Exception as e:
        raise BackupError(f"Backup failed: {e}")


# ==============================================================================
# 10. FLASK ROUTES
# ==============================================================================
@app.before_request
def before_request_handler():
    """A function that runs before every request.

    It handles two main tasks:
    1. Checks if the CSV file on disk has been modified by an external process.
       If so, it automatically reloads the data to ensure the app is up-to-date.
    2. Ensures the session variable for the incomplete filter is initialized.
    """
    # Exclude certain routes (like static files and login) from this check.
    if request.endpoint in ["static", "serve_relative_image", "login", "logout"]:
        return

    session.setdefault("show_only_incomplete", False)

    path = Config.CSV_FILE_PATH
    if not os.path.exists(path):
        if data:  # File was deleted while the app was running.
            app.logger.critical(f"FATAL: CSV file disappeared from {path}. Clearing data.")
            data.clear()
        return

    try:
        mod_time = os.path.getmtime(path)
        # Reload if data is not yet loaded OR if the file's modification time has changed.
        if not data or mod_time != session.get("last_loaded_csv_mod_time"):
            app.logger.info("CSV file change detected. Reloading...")
            load_csv_data()
            session["last_loaded_csv_mod_time"] = mod_time
            flash("Data reloaded from disk.", "info")
    except DataLoadError as e:
        app.logger.error(f"Auto-reload failed: {e}")
        flash("Error: Could not auto-reload data from disk.", "error")


# --- Authentication Routes ---
@app.route("/login", methods=["GET", "POST"])
def login():
    """Handles user login."""
    if current_user.is_authenticated:
        return redirect(url_for("index"))
    
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        user = db.session.get(User, username)
        
        if user and user.verify_password(password):
            login_user(user)
            app.logger.info(f"User '{username}' logged in successfully.")
            # Redirect to the page they were trying to access, or to the index.
            return redirect(request.args.get("next") or url_for("index"))
        
        flash("Invalid username or password.", "error")
        
    return render_template("login.html", messages=flash_messages())


@app.route("/logout")
@login_required
def logout():
    """Logs the current user out."""
    logout_user()
    flash("You have been logged out.", "info")
    return redirect(url_for("login"))


# --- Admin Routes ---
@app.route("/users")
@login_required
def users_management():
    """Displays the user management page (Admin only)."""
    if not current_user.is_admin:
        flash("You do not have permission to access this page.", "error")
        return redirect(url_for("index"))
        
    users = db.session.execute(db.select(User)).scalars().all()
    return render_template("users.html", users=users, messages=flash_messages())


@app.route("/add_user", methods=["POST"])
@login_required
def add_user():
    """Handles the form submission for adding a new user (Admin only)."""
    if not current_user.is_admin:
        return redirect(url_for("index"))
        
    username = request.form.get("username", "").strip()
    password = request.form.get("password", "").strip()
    
    if not username or not password:
        flash("Username and password are required.", "error")
        return redirect(url_for("users_management"))
        
    if db.session.get(User, username):
        flash(f"User '{username}' already exists.", "error")
        return redirect(url_for("users_management"))
        
    try:
        u = User(id=username, is_admin=(request.form.get("is_admin") == "on"))
        u.set_password(password)
        db.session.add(u)
        db.session.commit()
        flash(f"User '{username}' created successfully.", "success")
    except Exception as e:
        db.session.rollback()
        flash(f"An error occurred while adding the user: {e}", "error")
        
    return redirect(url_for("users_management"))


# --- Main Application Routes ---
@app.route("/", methods=["GET"])
@login_required
def index():
    """The main page of the application for data correction."""
    if not data:
        return render_template(
            "index.html",
            error_message="CSV data could not be loaded. Please check the file path and logs.",
            data_loaded=False,
            messages=flash_messages(),
        )

    # Clean up any leases that have timed out.
    _release_expired_leases()

    # --- Logic to determine which item to display ---
    item_to_display = None
    requested_index_str = request.args.get("index")

    # Priority 1: User requested a specific index via URL parameter.
    if requested_index_str:
        try:
            idx = int(requested_index_str)
            if 0 <= idx < len(data):
                # Release any other leases the user might have before granting a new one.
                existing_leases = db.session.execute(
                    db.select(QueueItem).filter_by(leased_by_id=current_user.id, status="leased")
                ).scalars().all()
                for lease in existing_leases:
                    if lease.original_index != idx:
                        lease.status = "pending"
                        lease.leased_by = None
                        lease.leased_at = None
                
                # Get the queue item for the requested index.
                qi = db.session.execute(
                    db.select(QueueItem).filter_by(original_index=idx)
                ).scalars().first()
                
                if qi:
                    if qi.status == "leased" and qi.leased_by_id != current_user.id:
                        flash("This item is currently leased by another user. Viewing in read-only mode.", "warning")
                    elif qi.status != "completed":
                        # Grant a lease to the current user.
                        qi.status = "leased"
                        qi.leased_by_id = current_user.id
                        qi.leased_at = datetime.datetime.utcnow()
                    item_to_display = qi
        except (ValueError, TypeError):
            flash("Invalid index provided in URL.", "error")
            pass  # Fall through to default logic.

    # If no specific index was requested or found, use the default queue logic.
    if not item_to_display:
        # Priority 2: Check if the user already has an active lease.
        active_lease = db.session.execute(
            db.select(QueueItem).filter_by(leased_by_id=current_user.id, status="leased")
        ).scalars().first()

        if active_lease:
            item_to_display = active_lease
        else:
            # Priority 3: Get the next available 'pending' item from the queue.
            next_pending_item = db.session.execute(
                db.select(QueueItem).filter_by(status="pending").order_by(QueueItem.original_index)
            ).scalars().first()
            
            if next_pending_item:
                # Grant a lease for this new item.
                next_pending_item.status = "leased"
                next_pending_item.leased_by_id = current_user.id
                next_pending_item.leased_at = datetime.datetime.utcnow()
                item_to_display = next_pending_item
            else:
                # Priority 4: No items are left in the queue.
                db.session.commit()  # Save any lease releases from earlier.
                total = db.session.query(func.count(QueueItem.id)).scalar()
                done = db.session.query(func.count(QueueItem.id)).filter_by(status="completed").scalar()
                return render_template(
                    "index.html",
                    no_items_left=True,
                    completed_count=done,
                    total_count=total,
                    messages=flash_messages(),
                )

    # Save any changes to the queue (e.g., new leases) to the database.
    db.session.commit()

    current_index = item_to_display.original_index
    row_data = data[current_index]
    display_row_data = row_data.copy()

    # --- Pre-fill Logic: Propagate AccessionID and Stain from sibling files ---
    identifier = display_row_data.get("_identifier")
    if not display_row_data.get("AccessionID") and identifier:
        # Find another row with the same patient identifier that already has an AccessionID.
        for r in data:
            if r.get("_identifier") == identifier and r.get("AccessionID"):
                display_row_data["AccessionID"] = r["AccessionID"]
                flash(f"Auto-filled Accession ID '{r['AccessionID']}' from a related file.", "info")
                # If stain is also empty, propagate it as well.
                if not display_row_data.get("Stain") and r.get("Stain"):
                    display_row_data["Stain"] = r["Stain"]
                break

    # --- Image Path Resolution ---
    label_image_url, macro_image_url = None, None
    label_image_exists, macro_image_exists = False, False

    # Resolve and validate the path for the label image.
    csv_label_path = display_row_data.get("_label_path")
    if csv_label_path:
        # Sanitize path: remove the base directory if it was mistakenly included in the CSV.
        if 'NP-22-data' in csv_label_path:
            path_parts = csv_label_path.split('NP-22-data', 1)
            if len(path_parts) > 1:
                csv_label_path = path_parts[1].lstrip('.\\/')
        
        full_path = os.path.join(Config.IMAGE_BASE_DIR, csv_label_path)
        if os.path.exists(full_path):
            label_image_url = url_for("serve_relative_image", filepath=csv_label_path)
            label_image_exists = True

    # Resolve and validate the path for the macro image.
    csv_macro_path = display_row_data.get("_macro_path")
    if csv_macro_path:
        if 'NP-22-data' in csv_macro_path:
            path_parts = csv_macro_path.split('NP-22-data', 1)
            if len(path_parts) > 1:
                csv_macro_path = path_parts[1].lstrip('.\\/')
        
        full_path = os.path.join(Config.IMAGE_BASE_DIR, csv_macro_path)
        if os.path.exists(full_path):
            macro_image_url = url_for("serve_relative_image", filepath=csv_macro_path)
            macro_image_exists = True

    # --- UI Statistics ---
    queue_stats = {
        "pending": db.session.query(func.count(QueueItem.id)).filter_by(status="pending").scalar(),
        "leased": db.session.query(func.count(QueueItem.id)).filter_by(status="leased").scalar(),
        "completed": db.session.query(func.count(QueueItem.id)).filter_by(status="completed").scalar(),
    }
    
    # Get the 5 most recently completed items by the current user for the history panel.
    recently_completed = db.session.execute(
        db.select(QueueItem)
        .filter_by(completed_by_id=current_user.id)
        .order_by(QueueItem.completed_at.desc())
        .limit(5)
    ).scalars().all()
    # Annotate these items with their AccessionID for display.
    for r in recently_completed:
        if r.original_index < len(data):
            r.accession_id = data[r.original_index].get("AccessionID", "N/A")

    return render_template(
        "index.html",
        row=display_row_data,
        original_index=current_index,
        total_original_rows=len(data),
        label_img_path=label_image_url,
        macro_img_path=macro_image_url,
        label_img_exists=label_image_exists,
        macro_img_exists=macro_image_exists,
        messages=flash_messages(),
        data_loaded=True,
        queue_stats=queue_stats,
        lease_info=item_to_display,
        datetime=datetime.datetime,  # Pass modules to template for use in Jinja2
        timedelta=datetime.timedelta,
        recently_completed=recently_completed,
    )


@app.route("/history")
@login_required
def history():
    """Displays the user's full history of completed items."""
    history_items = db.session.execute(
        db.select(QueueItem)
        .filter_by(completed_by_id=current_user.id)
        .order_by(QueueItem.completed_at.desc())
    ).scalars().all()
    
    # Annotate each history item with its AccessionID.
    for item in history_items:
        if item.original_index < len(data):
            row = data[item.original_index]
            item.accession_id = row.get("AccessionID", "N/A")

    return render_template("history.html", completed_items=history_items, messages=flash_messages())


@app.route("/update", methods=["POST"])
@login_required
def update():
    """Handles the form submission for saving corrections."""
    if not data:
        return redirect(url_for("index"))
        
    try:
        idx = int(request.form["original_index"])
        qi = db.session.execute(
            db.select(QueueItem).filter_by(original_index=idx)
        ).scalars().first()

        # Check if the user is allowed to save. A user can save if they hold the lease.
        # We also allow a "forced" save if the lease expired but they were the last holder,
        # effectively re-leasing the item to them.
        is_forced_save = False
        if not qi or (qi.leased_by_id != current_user.id and qi.status != "completed"):
            is_forced_save = True  # Allow save, implicitly taking over the lease.
        elif qi and qi.status == "completed":
            flash("Cannot save changes: This item has already been completed.", "error")
            return redirect(url_for("index"))

        # Get the corresponding row from the in-memory data list.
        row_to_update = data[idx]

        # Get new values from the form.
        new_accession_id = request.form.get("accession_id", "").strip()
        new_stain = request.form.get("stain", "").strip()
        new_block_number = request.form.get("block_number", "").strip()
        is_marked_complete = request.form.get("complete") == "on"

        # --- Update the in-memory data if changes were made ---
        has_changed = False
        accession_id_has_changed = row_to_update["AccessionID"] != new_accession_id
        if accession_id_has_changed:
            row_to_update["AccessionID"] = new_accession_id
            has_changed = True
        if row_to_update["Stain"] != new_stain:
            row_to_update["Stain"] = new_stain
            has_changed = True
        if row_to_update["BlockNumber"] != new_block_number:
            row_to_update["BlockNumber"] = new_block_number
            has_changed = True

        # --- Handle completion status ---
        # An item can only be marked complete if the required fields are filled.
        is_valid_to_complete = bool(new_accession_id and new_stain)
        if is_marked_complete and not is_valid_to_complete:
            flash("Cannot mark as complete: Accession ID and Stain are required.", "warning")
            is_marked_complete = False

        if row_to_update["_is_complete"] != is_marked_complete:
            row_to_update["_is_complete"] = is_marked_complete
            has_changed = True

        # --- If any data was changed, commit changes to DB and disk ---
        if has_changed:
            current_user.correction_count += 1
            if accession_id_has_changed:
                _recalculate_accession_counts() # Update counts if an ID changed.

            # Update the queue item's status in the database.
            if request.form.get("action") == "next" and is_marked_complete:
                # If user clicks "Save and Next" and item is complete...
                qi.status = "completed"
                qi.completed_by = current_user
                qi.completed_at = datetime.datetime.utcnow()
            elif is_forced_save:
                # If this was a forced save on a non-completed item, re-establish the lease.
                qi.status = "leased"
                qi.leased_by = current_user
                qi.leased_at = datetime.datetime.utcnow()
            
            db.session.commit()

            # Persist changes to the CSV file on disk.
            try:
                _create_backup()
                save_csv_data()
                flash("Changes saved successfully.", "success")
            except Exception as e:
                app.logger.error(f"Save operation failed: {e}")
                flash("CRITICAL: Error saving changes to the CSV file.", "error")
        
        # Redirect to the main page, which will automatically show the next item.
        return redirect(url_for("index"))

    except (ValueError, KeyError, IndexError) as e:
        app.logger.error(f"Update failed due to invalid form data or index: {e}")
        flash("An error occurred during the update. Please try again.", "error")
        return redirect(url_for("index"))


@app.route("/release", methods=["POST"])
@login_required
def release_lease():
    """Manually releases all leases held by the current user."""
    leases = db.session.execute(
        db.select(QueueItem).filter_by(leased_by_id=current_user.id, status="leased")
    ).scalars().all()
    
    if leases:
        for lease in leases:
            lease.status = "pending"
            lease.leased_by = None
            lease.leased_at = None
        db.session.commit()
        flash(f"Successfully released {len(leases)} item(s) back to the queue.", "info")
        
    return redirect(url_for("index"))


@app.route("/search", methods=["POST"])
@login_required
def search():
    """Searches for an item by AccessionID, filename, or BlockNumber."""
    if not data:
        return redirect(url_for("index"))
        
    search_term = request.form.get("search_term", "").strip().lower()
    if not search_term:
        return redirect(url_for("index"))

    # Iterate through the data to find the first match.
    for i, row in enumerate(data):
        if (
            search_term in row.get("AccessionID", "").lower() or
            search_term in row.get("_identifier", "").lower() or
            search_term == row.get("BlockNumber", "").lower()
        ):
            # Redirect to the index page with the found item's index.
            return redirect(url_for("index", index=i))

    flash(f"No item found matching '{search_term}'.", "warning")
    return redirect(url_for("index"))


@app.route("/data_images/<path:filepath>")
@login_required
def serve_relative_image(filepath: str):
    """Securely serves image files from the configured data directory.

    This route takes a relative path (as found in the CSV) and serves the
    corresponding file. It includes a security check to prevent path traversal
    attacks, ensuring that only files within the `IMAGE_BASE_DIR` can be accessed.
    """
    # Get absolute paths for security validation.
    abs_image_dir = os.path.abspath(Config.IMAGE_BASE_DIR)
    abs_file_path = os.path.abspath(os.path.join(abs_image_dir, filepath))

    # --- SECURITY CHECK ---
    # Ensure the requested file path is genuinely inside the allowed image directory.
    if os.path.commonpath([abs_image_dir, abs_file_path]) != abs_image_dir:
        app.logger.warning(f"Path traversal attempt blocked for filepath: {filepath}")
        return "Access denied: Invalid file path.", 403

    if not os.path.exists(abs_file_path):
        return "Image not found on server.", 404

    directory, filename = os.path.split(abs_file_path)
    return send_from_directory(directory, filename)


def flash_messages() -> List[Dict[str, str]]:
    """A helper function to format flashed messages for consumption by templates."""
    return [
        {"category": category, "message": message}
        for category, message in get_flashed_messages(with_categories=True)
    ]


# ==============================================================================
# 11. CLI COMMANDS
# ==============================================================================
@app.cli.command("init-db")
@with_appcontext
def init_db_command():
    """CLI command to initialize the database and populate the queue.

    This command should be run once during initial setup. It performs three tasks:
    1. Creates all database tables (User, QueueItem).
    2. Creates a default 'admin' user with a configurable password.
    3. Scans the CSV file and populates the QueueItem table, setting the initial
       status of each item based on whether it's already marked as complete.
    """
    print("--- Initializing Database and Queue ---")
    print(f"Database path: {app.config['SQLALCHEMY_DATABASE_URI']}")
    db.create_all()

    # Create the default admin user if it doesn't exist.
    if not db.session.get(User, "admin"):
        u = User(id="admin", is_admin=True)
        u.set_password(Config.ADMIN_DEFAULT_PASSWORD)
        db.session.add(u)
        print(f"Created default 'admin' user with password: '{Config.ADMIN_DEFAULT_PASSWORD}'")
        print("IMPORTANT: Change this password in a production environment.")
    else:
        print("'admin' user already exists.")

    # Populate the queue from the CSV file.
    if os.path.exists(Config.CSV_FILE_PATH):
        try:
            load_csv_data()
            # Get all existing queue items to avoid creating duplicates.
            existing_items = db.session.execute(db.select(QueueItem)).scalars().all()
            existing_indices = {item.original_index for item in existing_items}
            
            new_items_to_add = []
            for row in data:
                if row["_original_index"] not in existing_indices:
                    # If 'ParsingQCPassed' is true in the CSV, the item starts as 'completed'.
                    # Otherwise, it's 'pending' and needs review.
                    status = "completed" if row["_is_complete"] else "pending"
                    new_items_to_add.append(
                        QueueItem(original_index=row["_original_index"], status=status)
                    )

            if new_items_to_add:
                # Use bulk_save_objects for efficiency when adding many items.
                db.session.bulk_save_objects(new_items_to_add)
                db.session.commit()
                print(f"Successfully added {len(new_items_to_add)} new items to the processing queue.")
            else:
                print("Queue is already up to date with the CSV file.")
        except Exception as e:
            print(f"ERROR: Could not populate queue from CSV. Reason: {e}")
    else:
        print(f"WARNING: CSV file not found at {Config.CSV_FILE_PATH}. Queue was not populated.")

    db.session.commit()
    print("--- Initialization complete. ---")


# ==============================================================================
# 12. SCRIPT EXECUTION
# ==============================================================================
if __name__ == "__main__":
    # When running the script directly (e.g., `python app.py`),
    # attempt an initial load of the CSV data.
    if os.path.exists(Config.CSV_FILE_PATH):
        try:
            # Use the app context to ensure extensions are available.
            with app.app_context():
                load_csv_data()
        except Exception as e:
            print(f"WARNING: Failed to pre-load CSV on startup: {e}")

    # Start the Flask development server.
    # `debug=True` enables auto-reloading and an interactive debugger.
    # `host="0.0.0.0"` makes the server accessible from other devices on the network.
    app.run(debug=True, host="0.0.0.0")
