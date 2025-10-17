import csv
import datetime
import logging
import os
import re
import shutil
from collections import Counter, defaultdict
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, List, Optional, Tuple, Union
from typing import Counter as CounterType

from flask import (
    Flask,
    Response,
    flash,
    get_flashed_messages,
    jsonify,
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
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.wrappers.request import Request


# ==============================================================================
# 1. CONFIGURATION
# ==============================================================================
class Config:
    """Application configuration settings."""

    SECRET_KEY = os.environ.get(
        "SECRET_KEY", "a-super-secret-key-that-you-should-change"
    )
    SQLALCHEMY_DATABASE_URI = "sqlite:///users.db"
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # File Paths and Settings
    CSV_FILE_PATH = r"Data/combined_data_ocr_processed.csv"
    IMAGE_BASE_DIR = r"Data"
    BACKUP_DIR = "csv_backups"

    # Admin User Default Password (use environment variable in production)
    ADMIN_DEFAULT_PASSWORD = os.environ.get(
        "ADMIN_DEFAULT_PASSWORD", "change_this_password"
    )

    # Queue Settings
    LEASE_DURATION_SECONDS = 300  # 5 minutes


# ==============================================================================
# 2. LOGGING SETUP
# ==============================================================================
def setup_logging(app: Flask) -> None:
    """Configures comprehensive logging for the application."""
    if not os.path.exists("logs"):
        os.mkdir("logs")

    # File handler for detailed logging
    file_handler = RotatingFileHandler("logs/app.log", maxBytes=10240, backupCount=10)
    file_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]"
        )
    )
    file_handler.setLevel(logging.INFO)

    # Console handler for general output
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    )
    console_handler.setLevel(logging.INFO)

    app.logger.addHandler(file_handler)
    app.logger.addHandler(console_handler)
    app.logger.setLevel(logging.INFO)
    app.logger.info("Application startup")


# ==============================================================================
# 3. APPLICATION & EXTENSIONS INITIALIZATION
# ==============================================================================
instance_path = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "instance")
)
app = Flask(__name__, template_folder="../templates", instance_path=instance_path)
app.config.from_object(Config)

# Initialize logging
setup_logging(app)

db = SQLAlchemy(app)
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "login"
login_manager.login_message = "Please log in to access this page."
login_manager.login_message_category = "warning"


# ==============================================================================
# 4. CUSTOM EXCEPTIONS
# ==============================================================================
class DataLoadError(Exception):
    """Custom exception for errors during CSV data loading."""

    pass


class DataSaveError(Exception):
    """Custom exception for errors during CSV data saving."""

    pass


class BackupError(Exception):
    """Custom exception for errors during backup creation."""

    pass


# ==============================================================================
# 5. DATABASE MODELS (Flask-Login)
# ==============================================================================
class User(UserMixin, db.Model):
    """Represents a user in the database."""

    id = db.Column(db.String(80), primary_key=True, unique=True, nullable=False)
    password_hash = db.Column(db.String(128), nullable=False)
    correction_count = db.Column(db.Integer, default=0, nullable=False)
    is_admin = db.Column(db.Boolean, default=False, nullable=False)

    def set_password(self, password: str) -> None:
        """Hashes and sets the user's password."""
        self.password_hash = generate_password_hash(password)

    def verify_password(self, password: str) -> bool:
        """Verifies a given password against the stored hash."""
        return check_password_hash(self.password_hash, password)

    def __repr__(self) -> str:
        return f"<User {self.id}>"


class QueueItem(db.Model):
    """Represents an item in the processing queue."""

    id = db.Column(db.Integer, primary_key=True)
    original_index = db.Column(db.Integer, nullable=False, index=True, unique=True)
    status = db.Column(
        db.String(20), nullable=False, default="pending", index=True
    )  # pending, leased, completed
    leased_by_id = db.Column(
        db.String(80), db.ForeignKey("user.id"), nullable=True, index=True
    )
    leased_at = db.Column(db.DateTime, nullable=True)
    completed_by_id = db.Column(
        db.String(80), db.ForeignKey("user.id"), nullable=True, index=True
    )
    completed_at = db.Column(db.DateTime, nullable=True)

    leased_by = db.relationship(
        "User", foreign_keys=[leased_by_id], backref=db.backref("leases", lazy=True)
    )
    completed_by = db.relationship(
        "User",
        foreign_keys=[completed_by_id],
        backref=db.backref("completed_items", lazy=True),
    )

    def __repr__(self) -> str:
        return f"<QueueItem {self.original_index} - {self.status}>"


@login_manager.user_loader
def load_user(user_id: str) -> Optional[User]:
    """Loads a user from the database by their ID for Flask-Login."""
    return User.query.get(user_id)


# ==============================================================================
# 6. GLOBAL DATA STORE
# ==============================================================================
# NOTE: Using global variables for data storage is simple for single-worker
# development servers but is not safe for production environments with multiple
# workers (like Gunicorn). Each worker would have its own copy of the data,
# leading to inconsistencies. A more robust solution would involve a shared
# data store like Redis or a database.
data: List[Dict[str, Any]] = []
headers: List[str] = []


# ==============================================================================
# 7. HELPER FUNCTIONS (Data Processing & Navigation)
# ==============================================================================
def _release_expired_leases():
    """Scans for and releases any leases that have expired."""
    lease_duration = datetime.timedelta(seconds=app.config["LEASE_DURATION_SECONDS"])
    expired_time = datetime.datetime.utcnow() - lease_duration

    expired_items = QueueItem.query.filter(
        QueueItem.status == "leased", QueueItem.leased_at < expired_time
    ).all()

    if expired_items:
        for item in expired_items:
            app.logger.info(
                f"Lease for item {item.original_index} expired (leased by {item.leased_by_id}). Releasing."
            )
            item.status = "pending"
            item.leased_by_id = None
            item.leased_at = None
        db.session.commit()
        flash(
            f"{len(expired_items)} item(s) had expired leases and were returned to the queue.",
            "warning",
        )


def _recalculate_accession_counts() -> None:
    """Recalculates and updates the count for each AccessionID in the global data."""
    global data
    if not data:
        return
    app.logger.info("Recalculating AccessionID counts for all rows.")
    id_counts: CounterType[str] = Counter(
        row.get("AccessionID", "").strip()
        for row in data
        if row.get("AccessionID", "").strip()
    )
    for row in data:
        current_id = row.get("AccessionID", "").strip()
        row["_accession_id_count"] = id_counts[current_id] if current_id else 0


def parse_original_line(line_str: str) -> Tuple[Optional[str], str, str]:
    """Parses the 'OriginalLine' string to extract identifier, label, and macro text."""
    identifier: Optional[str] = None
    label_text, macro_text = "N/A", "N/A"

    parts = line_str.split(";")
    if parts:
        identifier = parts[0].strip().replace('"', "")

    label_match = re.search(r"Label:\s*(.*?)(?:;Macro:|$)", line_str)
    if label_match:
        label_text = label_match.group(1).strip().strip('"')

    macro_match = re.search(r"Macro:\s*(.*?)(?:;|$)", line_str)
    if macro_match:
        macro_text = macro_match.group(1).strip().strip('"')

    return identifier, label_text, macro_text


def _is_row_incomplete(row_dict: Dict[str, Any]) -> bool:
    """Checks if a row is marked as incomplete."""
    return not row_dict.get("_is_complete", False)


def get_current_display_list_indices() -> List[int]:
    """Returns a list of original data indices based on the current filter."""
    if not data:
        return []
    if session.get("show_only_incomplete"):
        return [i for i, row in enumerate(data) if _is_row_incomplete(row)]
    return list(range(len(data)))


def get_display_info_for_original_index(
    original_index: int,
) -> Optional[Dict[str, int]]:
    """Gets the display index and count for a given original data index."""
    display_indices = get_current_display_list_indices()
    try:
        return {
            "display_index": display_indices.index(original_index),
            "total_display_count": len(display_indices),
        }
    except ValueError:
        app.logger.warning(
            f"Original index {original_index} not found in current display list."
        )
        return None


def find_navigation_index(current_original_index: int, direction: str) -> Optional[int]:
    """Finds the next/previous original index based on the navigation direction and filter."""
    display_indices = get_current_display_list_indices()
    if not display_indices:
        return None

    try:
        current_display_pos = display_indices.index(current_original_index)
    except ValueError:
        # If current index is not in the filtered list, start from the beginning
        return display_indices[0]

    if direction == "next":
        next_pos = current_display_pos + 1
        return display_indices[next_pos] if next_pos < len(display_indices) else None
    elif direction == "prev":
        prev_pos = current_display_pos - 1
        return display_indices[prev_pos] if prev_pos >= 0 else None
    elif direction == "next_incorrect":
        # Search forward from the current position
        for i in range(current_display_pos + 1, len(display_indices)):
            if _is_row_incomplete(data[display_indices[i]]):
                return display_indices[i]
        # If not found, wrap around and search from the beginning
        for i in range(current_display_pos + 1):
            if _is_row_incomplete(data[display_indices[i]]):
                flash(
                    "No further incomplete rows found. Restarting search from beginning.",
                    "info",
                )
                return display_indices[i]
        flash("No incomplete rows found in the entire dataset.", "info")
        return None

    return None


# ==============================================================================
# 8. CORE DATA I/O FUNCTIONS
# ==============================================================================
def load_csv_data(file_path: str = Config.CSV_FILE_PATH) -> None:
    """
    Loads and processes data from the specified CSV file into the global store.
    Raises DataLoadError on failure.
    """
    global data, headers
    app.logger.info(f"Attempting to load CSV data from: {file_path}")

    if not os.path.exists(file_path):
        raise DataLoadError(f"CSV file not found at path: {file_path}")

    _data: List[Dict[str, Any]] = []
    required_headers = ["AccessionID", "Stain", "Complete", "OriginalLine"]

    try:
        with open(file_path, "r", newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile, delimiter=";")
            _headers = reader.fieldnames
            if not _headers or not all(h in _headers for h in required_headers):
                raise DataLoadError(
                    f"CSV file is missing one of the required headers: {required_headers}"
                )

            for i, row in enumerate(reader):
                row["_original_index"] = i
                identifier, label, macro = parse_original_line(
                    row.get("OriginalLine", "")
                )
                row["_identifier"], row["_label_text"], row["_macro_text"] = (
                    identifier,
                    label,
                    macro,
                )
                row["_is_complete"] = (
                    str(row.get("Complete", "")).strip().lower() == "true"
                )
                row["AccessionID"] = row.get("AccessionID", "")
                row["Stain"] = row.get("Stain", "")
                row["BlockNumber"] = row.get("BlockNumber", "")
                _data.append(row)

        # Post-processing to calculate patient file counts
        patient_slide_ids: Dict[str, List[int]] = defaultdict(list)
        for i, row in enumerate(_data):
            if row["_identifier"]:
                patient_slide_ids[row["_identifier"]].append(i)

        for _, original_indices in patient_slide_ids.items():
            total = len(original_indices)
            original_indices.sort()
            for j, original_index in enumerate(original_indices):
                _data[original_index]["_total_patient_files"] = total
                _data[original_index]["_patient_file_number"] = j + 1

        data, headers = _data, _headers
        _recalculate_accession_counts()
        app.logger.info(f"Successfully loaded and processed {len(data)} rows from CSV.")

    except Exception as e:
        data, headers = [], []
        raise DataLoadError(f"An unexpected error occurred while reading the CSV: {e}")


def save_csv_data(target_path: str = Config.CSV_FILE_PATH) -> None:
    """
    Saves the current in-memory data to the target CSV file atomically.
    Raises DataSaveError on failure.
    """
    global data, headers
    if not headers:
        raise DataSaveError("Cannot save data: headers are not loaded.")
    if not data:
        app.logger.warning("Save called with no data in memory. Nothing to save.")
        return

    app.logger.info(f"Attempting to save {len(data)} rows to {target_path}")
    core_headers = [
        "AccessionID",
        "Stain",
        "BlockNumber",
        "Complete",
        "AccessionID_Count",
        "OriginalLine",
    ]
    write_headers = core_headers + sorted(
        [h for h in headers if not h.startswith("_") and h not in core_headers]
    )

    temp_path = target_path + ".tmp"
    try:
        with open(temp_path, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(
                csvfile,
                fieldnames=write_headers,
                delimiter=";",
                extrasaction="ignore",
                quoting=csv.QUOTE_NONNUMERIC,
            )
            writer.writeheader()
            for row in data:
                write_row = row.copy()
                write_row["Complete"] = str(row.get("_is_complete", False)).capitalize()
                write_row["AccessionID_Count"] = row.get("_accession_id_count", 0)
                writer.writerow(write_row)

        # Atomic replace operation
        os.replace(temp_path, target_path)
        session["last_loaded_csv_mod_time"] = os.path.getmtime(target_path)
        app.logger.info(f"Successfully saved data to {target_path}")

    except (IOError, OSError, csv.Error) as e:
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError as remove_err:
                app.logger.error(
                    f"Failed to remove temporary save file {temp_path}: {remove_err}"
                )
        raise DataSaveError(f"Failed to save data to {target_path}: {e}")


def _create_backup() -> None:
    """
    Creates a timestamped backup of the main CSV file.
    Raises BackupError on failure.
    """
    if not os.path.exists(Config.CSV_FILE_PATH):
        raise BackupError("Cannot create backup: Source CSV file does not exist.")

    try:
        if not os.path.exists(Config.BACKUP_DIR):
            os.makedirs(Config.BACKUP_DIR)
            app.logger.info(f"Created backup directory: {Config.BACKUP_DIR}")

        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        base_filename = os.path.basename(Config.CSV_FILE_PATH)
        backup_path = os.path.join(
            Config.BACKUP_DIR, f"{base_filename}_{timestamp}.bak"
        )

        shutil.copy2(Config.CSV_FILE_PATH, backup_path)
        app.logger.info(f"Backup created successfully at {backup_path}")

    except (IOError, OSError) as e:
        raise BackupError(f"Failed to create backup: {e}")


# ==============================================================================
# 9. FLASK HOOKS & ROUTES
# ==============================================================================
@app.before_request
def before_request_handler() -> None:
    """
    Runs before each request. Logs the request and ensures data is loaded.
    """
    # Log API hit
    user_id = current_user.id if current_user.is_authenticated else "Anonymous"
    app.logger.info(
        f"API Hit: {request.remote_addr} - {user_id} - {request.method} {request.path}"
    )

    # Skip data loading for static files and auth routes
    if request.endpoint in [
        "static",
        "serve_image",
        "login",
        "logout",
        "add_user",
        "users_management",
    ]:
        return

    # Initialize session variables if not present
    session.setdefault("show_only_incomplete", False)
    session.setdefault("last_loaded_csv_mod_time", 0)

    reload_needed = False
    if not data:
        app.logger.info("Data is not in memory. Triggering initial load.")
        reload_needed = True
    else:
        try:
            current_mod_time = os.path.getmtime(Config.CSV_FILE_PATH)
            if current_mod_time != session.get("last_loaded_csv_mod_time", 0):
                app.logger.info("CSV file on disk has changed. Triggering reload.")
                reload_needed = True
        except FileNotFoundError:
            app.logger.error(
                f"Main CSV file '{Config.CSV_FILE_PATH}' disappeared. Cannot check mod time."
            )
            flash(
                f"CRITICAL: Main CSV file '{Config.CSV_FILE_PATH}' not found.",
                "critical",
            )
            return
        except Exception as e:
            app.logger.warning(f"Could not check CSV modification time: {e}")
            flash(f"Warning: Could not check CSV modification time: {e}", "warning")

    if reload_needed:
        try:
            load_csv_data()
            session["last_loaded_csv_mod_time"] = os.path.getmtime(Config.CSV_FILE_PATH)
        except DataLoadError as e:
            app.logger.critical(f"Failed to load/reload CSV data: {e}")
            flash(f"CRITICAL: Failed to load/reload CSV data. Error: {e}", "critical")


# --- Authentication Routes ---
@app.route("/login", methods=["GET", "POST"])
def login() -> Union[Response, str]:
    """Handles user login."""
    if current_user.is_authenticated:
        return redirect(url_for("index"))

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")

        user = User.query.get(username)
        if user and user.verify_password(password):
            login_user(user)
            app.logger.info(f"User '{username}' logged in successfully.")
            flash("Logged in successfully.", "success")
            next_page = request.args.get("next")
            return redirect(next_page or url_for("index"))
        else:
            app.logger.warning(f"Failed login attempt for username: '{username}'.")
            flash("Invalid username or password.", "error")

    return render_template("login.html", messages=flash_messages())


@app.route("/logout")
@login_required
def logout() -> Response:
    """Logs out the current user."""
    app.logger.info(f"User '{current_user.id}' logged out.")
    logout_user()
    flash("You have been logged out.", "info")
    return redirect(url_for("login"))


# --- Admin / User Management Routes ---
@app.route("/users")
@login_required
def users_management() -> Union[Response, str]:
    """Displays user management page (admin only)."""
    if not current_user.is_admin:
        app.logger.warning(
            f"User '{current_user.id}' attempted to access admin page without permission."
        )
        flash("You do not have permission to view user management.", "error")
        return redirect(url_for("index"))

    all_users = User.query.all()
    return render_template("users.html", users=all_users, messages=flash_messages())


@app.route("/add_user", methods=["POST"])
@login_required
def add_user() -> Response:
    """Handles adding a new user (admin only)."""
    if not current_user.is_admin:
        app.logger.warning(
            f"User '{current_user.id}' attempted to add a user without permission."
        )
        flash("You do not have permission to add users.", "error")
        return redirect(url_for("index"))

    username = request.form.get("username", "").strip()
    password = request.form.get("password", "").strip()
    is_admin = request.form.get("is_admin") == "on"

    if not username or not password:
        flash("Username and Password cannot be empty.", "error")
        return redirect(url_for("users_management"))

    if User.query.get(username):
        flash(f"User '{username}' already exists.", "error")
        return redirect(url_for("users_management"))

    try:
        new_user = User(id=username, is_admin=is_admin)
        new_user.set_password(password)
        db.session.add(new_user)
        db.session.commit()
        app.logger.info(f"Admin '{current_user.id}' created new user '{username}'.")
        flash(f"User '{username}' added successfully.", "success")
    except Exception as e:
        db.session.rollback()
        app.logger.error(f"Error adding user '{username}': {e}")
        flash(f"Error adding user: {e}", "error")

    return redirect(url_for("users_management"))


# --- Core Application Routes ---
@app.route("/", methods=["GET"])
@login_required
def index() -> str:
    """Displays the main data correction interface, managing the item queue."""
    if not data:
        return render_template(
            "index.html",
            error_message="Failed to load CSV data.",
            data_loaded=False,
            messages=flash_messages(),
        )

    _release_expired_leases()

    requested_index_str = request.args.get("index")
    if requested_index_str:
        try:
            requested_index = int(requested_index_str)
            if not (0 <= requested_index < len(data)):
                flash("Invalid index requested.", "error")
                return redirect(url_for("index"))

            # Release current lease if any
            active_lease = QueueItem.query.filter_by(
                leased_by_id=current_user.id, status="leased"
            ).first()
            if active_lease:
                active_lease.status = "pending"
                active_lease.leased_by_id = None
                active_lease.leased_at = None
                db.session.commit()

            item_to_display = QueueItem.query.filter_by(
                original_index=requested_index
            ).first()
            if not item_to_display:
                flash("The requested item is not in the queue.", "error")
                return redirect(url_for("index"))

            if (
                item_to_display.status == "leased"
                and item_to_display.leased_by_id != current_user.id
            ):
                flash(
                    f"Item with Accession ID: {data[item_to_display.original_index]['AccessionID']} is leased by another user. Viewing in read-only mode.",
                    "warning",
                )
            else:
                item_to_display.status = "leased"
                item_to_display.leased_by_id = current_user.id
                item_to_display.leased_at = datetime.datetime.utcnow()
                db.session.commit()
                flash(
                    f"Viewing item with Accession ID: {data[item_to_display.original_index]['AccessionID']}.",
                    "info",
                )

        except ValueError:
            flash("Invalid index format.", "error")
            return redirect(url_for("index"))
    else:
        # Check if the user has an active lease
        active_lease = QueueItem.query.filter_by(
            leased_by_id=current_user.id, status="leased"
        ).first()

        if active_lease:
            item_to_display = active_lease
            flash(
                f"You are currently working on item with Accession ID: {data[item_to_display.original_index]['AccessionID']}. Don't forget to save or release it.",
                "info",
            )
        else:
            # Get the next available item from the queue
            item_to_display = (
                QueueItem.query.filter_by(status="pending")
                .order_by(QueueItem.original_index)
                .first()
            )

            if item_to_display:
                # Lease the item
                item_to_display.status = "leased"
                item_to_display.leased_by_id = current_user.id
                item_to_display.leased_at = datetime.datetime.utcnow()
                db.session.commit()
                app.logger.info(
                    f"User '{current_user.id}' leased item {item_to_display.original_index}."
                )
                flash(
                    f"Assigned new item with Accession ID: {data[item_to_display.original_index]['AccessionID']} from the queue.",
                    "success",
                )
            else:
                # No items left in the queue
                completed_count = QueueItem.query.filter_by(status="completed").count()
                total_count = QueueItem.query.count()
                return render_template(
                    "index.html",
                    no_items_left=True,
                    completed_count=completed_count,
                    total_count=total_count,
                    messages=flash_messages(),
                )

    current_original_index = item_to_display.original_index
    current_row = data[current_original_index]

    # Create a copy to avoid modifying the global data just by viewing.
    # This 'display_row' will be passed to the template.
    display_row = current_row.copy()

    # --- Pre-fill logic based on other records ---
    prefilled_fields = []

    # 1. Pre-fill AccessionID based on other files from the same patient (_identifier)
    if not display_row.get("AccessionID") and display_row.get("_identifier"):
        identifier_to_match = display_row.get("_identifier")
        for row in data:
            if row.get("_identifier") == identifier_to_match and row.get("AccessionID"):
                suggested_accession_id = row.get("AccessionID")
                display_row["AccessionID"] = suggested_accession_id
                app.logger.info(
                    f"Pre-filling AccessionID for identifier {identifier_to_match} with value '{suggested_accession_id}'."
                )
                prefilled_fields.append(f"Accession ID ('{suggested_accession_id}')")
                break

    # 2. Pre-fill 'Stain' based on other entries with the same (or newly pre-filled) AccessionID
    accession_id_to_match = display_row.get("AccessionID")
    if not display_row.get("Stain") and accession_id_to_match:
        for row in data:
            if row.get("AccessionID") == accession_id_to_match and row.get("Stain"):
                suggested_stain = row.get("Stain")
                display_row["Stain"] = suggested_stain
                app.logger.info(
                    f"Pre-filling Stain for AccessionID {accession_id_to_match} with value '{suggested_stain}'."
                )
                prefilled_fields.append(f"Stain ('{suggested_stain}')")
                break

    if prefilled_fields:
        flash(
            f"Prefilled the following fields based on other records: {', '.join(prefilled_fields)}. Please verify.",
            "info",
        )

    # Image path logic
    label_img_path, macro_img_path = None, None
    label_img_exists, macro_img_exists = False, False
    if display_row.get("_identifier"):
        base_id = display_row["_identifier"]
        label_fn, macro_fn = f"{base_id}_label.png", f"{base_id}_macro.png"
        if os.path.exists(os.path.join(Config.IMAGE_BASE_DIR, "label", label_fn)):
            label_img_path, label_img_exists = (
                url_for("serve_image", subdir="label", filename=label_fn),
                True,
            )
        if os.path.exists(os.path.join(Config.IMAGE_BASE_DIR, "macro", macro_fn)):
            macro_img_path, macro_img_exists = (
                url_for("serve_image", subdir="macro", filename=macro_fn),
                True,
            )

    queue_stats = {
        "pending": QueueItem.query.filter_by(status="pending").count(),
        "leased": QueueItem.query.filter_by(status="leased").count(),
        "completed": QueueItem.query.filter_by(status="completed").count(),
    }

    recently_completed_items = (
        QueueItem.query.filter_by(completed_by_id=current_user.id)
        .order_by(QueueItem.completed_at.desc())
        .limit(5)
        .all()
    )

    # Add AccessionID to recently_completed_items
    for item in recently_completed_items:
        item.accession_id = data[item.original_index]["AccessionID"]

    return render_template(
        "index.html",
        row=display_row,
        original_index=current_original_index,
        total_original_rows=len(data),
        label_img_path=label_img_path,
        macro_img_path=macro_img_path,
        label_img_exists=label_img_exists,
        macro_img_exists=macro_img_exists,
        messages=flash_messages(),
        data_loaded=True,
        queue_stats=queue_stats,
        lease_info=item_to_display,
        datetime=datetime.datetime,
        timedelta=datetime.timedelta,
        recently_completed=recently_completed_items,
    )


def _apply_row_updates(row: Dict[str, Any], form: Request.form) -> Tuple[bool, bool]:
    """Applies form data to a data row and returns change status."""
    data_changed, accession_id_changed = False, False

    submitted_accession_id = form.get("accession_id", "").strip()
    submitted_stain = form.get("stain", "").strip()
    submitted_block_number = form.get("block_number", "").strip()
    submitted_complete_checked = form.get("complete") == "on"

    if row.get("AccessionID") != submitted_accession_id:
        row["AccessionID"] = submitted_accession_id
        data_changed, accession_id_changed = True, True
    if row.get("Stain") != submitted_stain:
        row["Stain"], data_changed = submitted_stain, True
    if row.get("BlockNumber") != submitted_block_number:
        row["BlockNumber"], data_changed = submitted_block_number, True

    is_complete_target = submitted_complete_checked and bool(
        submitted_accession_id and submitted_stain
    )
    if submitted_complete_checked != row.get("_is_complete"):
        if submitted_complete_checked and not is_complete_target:
            flash(
                "Cannot mark as 'Complete': Accession ID and Stain must be filled.",
                "warning",
            )
        else:
            row["_is_complete"] = submitted_complete_checked
            row["Complete"] = str(submitted_complete_checked).capitalize()
            data_changed = True

    return data_changed, accession_id_changed


@app.route("/update", methods=["POST"])
@login_required
def update() -> Response:
    """Handles form submission for updating a data row, respecting the leasing system."""
    if not data:
        return redirect(url_for("index"))

    try:
        original_index = int(request.form["original_index"])
        if not (0 <= original_index < len(data)):
            raise IndexError("Invalid original_index received from form.")

        # Verify the lease
        queue_item = QueueItem.query.filter_by(original_index=original_index).first()
        if not queue_item or queue_item.leased_by_id != current_user.id:
            flash(
                "You do not hold the lease for this item. It may have expired or been taken by another user.",
                "error",
            )
            return redirect(url_for("index"))

        row = data[original_index]
        data_changed, accession_id_changed = _apply_row_updates(row, request.form)

        if data_changed:
            app.logger.info(
                f"User '{current_user.id}' updated row index {original_index}."
            )
            try:
                current_user.correction_count += 1
                db.session.commit()
            except Exception as e:
                db.session.rollback()
                app.logger.error(
                    f"Failed to update correction count for user '{current_user.id}': {e}"
                )
                flash(f"Error updating correction count: {e}", "error")

        if accession_id_changed:
            _recalculate_accession_counts()
            flash("Accession ID counts updated across dataset.", "info")

        # Mark as complete and save
        if request.form.get("action") == "next":
            queue_item.status = "completed"
            queue_item.completed_by_id = current_user.id
            queue_item.completed_at = datetime.datetime.utcnow()
            flash(f"Item {original_index + 1} marked as complete.", "success")

        db.session.commit()

        if data_changed:
            try:
                _create_backup()
                save_csv_data()
                flash(
                    f"Changes automatically saved to {os.path.basename(Config.CSV_FILE_PATH)}.",
                    "success",
                )
            except BackupError as e:
                app.logger.error(f"SAVE FAILED due to backup error: {e}")
                flash(
                    f"Save cancelled: Backup failed. Changes are not saved to disk. Error: {e}",
                    "error",
                )
            except DataSaveError as e:
                app.logger.critical(f"DATA SAVE FAILED: {e}")
                flash(
                    f"CRITICAL: Data saving FAILED. See logs for details. Error: {e}",
                    "critical",
                )

        return redirect(url_for("index"))

    except (ValueError, IndexError) as e:
        app.logger.error(f"Error processing update due to invalid index: {e}")
        flash(f"Error: Invalid index received for update.", "error")
        return redirect(url_for("index"))
    except Exception as e:
        app.logger.critical(f"Critical error processing update: {e}", exc_info=True)
        flash(f"A critical error occurred while processing the update: {e}", "critical")
        return redirect(url_for("index"))


@app.route("/release_lease", methods=["POST"])
@login_required
def release_lease() -> Response:
    """Releases the current user's active lease."""
    active_lease = QueueItem.query.filter_by(
        leased_by_id=current_user.id, status="leased"
    ).first()
    if active_lease:
        app.logger.info(
            f"User '{current_user.id}' manually released lease for item {active_lease.original_index}."
        )
        active_lease.status = "pending"
        active_lease.leased_by_id = None
        active_lease.leased_at = None
        db.session.commit()
        flash(
            f"Lease for item {active_lease.original_index + 1} has been released.",
            "info",
        )
    else:
        flash("You do not have an active lease to release.", "warning")
    return redirect(url_for("index"))


@app.route("/history")
@login_required
def history() -> str:
    """Displays the user's annotation history."""
    completed_items = (
        QueueItem.query.filter_by(completed_by_id=current_user.id)
        .order_by(QueueItem.completed_at.desc())
        .all()
    )

    # Add AccessionID to completed_items
    for item in completed_items:
        item.accession_id = data[item.original_index]["AccessionID"]

    return render_template(
        "history.html", completed_items=completed_items, messages=flash_messages()
    )


@app.route("/jump", methods=["POST"])
@login_required
def jump() -> Response:
    """Handles jumping to a specific item in the display queue."""
    if not data:
        return redirect(url_for("index"))
    current_index = request.form.get("original_index", 0)

    try:
        jump_target_1based = int(request.form.get("jump_to_index", "1"))
        jump_target_0based = jump_target_1based - 1

        display_indices = get_current_display_list_indices()
        if not display_indices:
            flash("No items to jump to in current view.", "warning")
            return redirect(url_for("index", index=current_index))

        if 0 <= jump_target_0based < len(display_indices):
            target_original_index = display_indices[jump_target_0based]
            flash(f"Jumped to item {jump_target_1based}.", "info")
            return redirect(url_for("index", index=target_original_index))
        else:
            flash(
                f"Invalid jump target: {jump_target_1based}. Please enter a number between 1 and {len(display_indices)}.",
                "error",
            )
            return redirect(url_for("index", index=current_index))

    except ValueError:
        flash("Invalid input for jump target. Please enter a number.", "error")
        return redirect(url_for("index", index=current_index))


@app.route("/search", methods=["POST"])
@login_required
def search() -> Response:
    """Handles searching for a specific Accession ID or Patient Identifier."""
    if not data:
        return redirect(url_for("index"))
    current_index = request.form.get("original_index", 0)
    search_term = request.form.get("search_term", "").strip().lower()

    if not search_term:
        flash("Please enter a search term (Accession ID or Filename ID).", "warning")
        return redirect(url_for("index", index=current_index))

    for i, row in enumerate(data):
        if (
            row.get("AccessionID", "").lower() == search_term
            or row.get("_identifier", "").lower() == search_term
        ):
            session["show_only_incomplete"] = False
            flash(
                f"Found matching item for '{search_term}'. Filter set to 'Show All'.",
                "info",
            )
            return redirect(url_for("index", index=i))

    flash(
        f"No item found matching Accession ID or Filename ID '{search_term}'.",
        "warning",
    )
    return redirect(url_for("index", index=current_index))


@app.route("/images/<subdir>/<path:filename>")
def serve_image(subdir: str, filename: str) -> Union[Response, Tuple[str, int]]:
    """Serves image files securely from the configured image directory."""
    if subdir not in ["label", "macro", "thumbnail"]:
        app.logger.warning(f"Invalid image subdirectory requested: {subdir}")
        return "Invalid image category", 404

    image_dir = os.path.join(Config.IMAGE_BASE_DIR, subdir)

    try:
        return send_from_directory(os.path.abspath(image_dir), filename)
    except FileNotFoundError:
        app.logger.warning(f"Image file not found: {os.path.join(image_dir, filename)}")
        return "Image file not found.", 404
    except Exception as e:
        app.logger.error(f"Exception serving file {filename} from {image_dir}: {e}")
        return "Error serving image file.", 500


def flash_messages() -> List[Dict[str, str]]:
    """Helper to format flashed messages for templates."""
    messages = []
    for category, message in get_flashed_messages(with_categories=True):
        css_class = "flash-critical" if category == "critical" else category
        messages.append(
            {"category": category, "message": message, "css_class": css_class}
        )
    return messages


# ==============================================================================
# 10. CLI COMMANDS & STARTUP
# ==============================================================================
@app.cli.command("init-db")
@with_appcontext
def init_db_command():
    """Initializes the database, creates the default admin user, and populates the queue."""
    db.create_all()
    print("INFO: Database tables created.")

    # Create admin user
    if not User.query.get("admin"):
        admin_user = User(id="admin", is_admin=True)
        admin_user.set_password(Config.ADMIN_DEFAULT_PASSWORD)
        db.session.add(admin_user)
        db.session.commit()
        print(f"INFO: Admin user 'admin' created.")
        print(f"IMPORTANT: The default password is '{Config.ADMIN_DEFAULT_PASSWORD}'.")
        print("         Please change this immediately after logging in, or set the")
        print("         'ADMIN_DEFAULT_PASSWORD' environment variable before running.")
    else:
        print("INFO: Admin user 'admin' already exists.")

    # Populate the queue
    try:
        print("INFO: Loading CSV data to populate queue...")
        load_csv_data()
        print(f"INFO: Found {len(data)} rows in the CSV.")

        existing_indices = {item.original_index for item in QueueItem.query.all()}
        print(f"INFO: Found {len(existing_indices)} existing items in the queue.")

        new_items = []
        for i, row in enumerate(data):
            if i not in existing_indices and not row.get("_is_complete"):
                new_items.append(QueueItem(original_index=i, status="pending"))

        if new_items:
            db.session.bulk_save_objects(new_items)
            db.session.commit()
            print(f"INFO: Added {len(new_items)} new items to the queue.")
        else:
            print("INFO: No new items to add to the queue.")

    except DataLoadError as e:
        print(f"ERROR: Failed to load CSV data for queue population: {e}")
    except Exception as e:
        db.session.rollback()
        print(f"ERROR: An unexpected error occurred during queue population: {e}")


if __name__ == "__main__":
    with app.app_context():
        # Ensure the database file exists, guide user if not.
        # The path from the config is relative to the instance folder.
        db_path = os.path.join(
            app.instance_path,
            app.config["SQLALCHEMY_DATABASE_URI"].replace("sqlite:///", ""),
        )
        if not os.path.exists(db_path):
            app.logger.warning(
                f"Database not found at {db_path}. Please run 'flask init-db' to create it."
            )
            print("\nWARNING: Database not found.")
            print(
                f"Please run the following command in your terminal to initialize it:"
            )
            print("flask init-db\n")
            exit(1)

    # Initial data load on startup
    try:
        load_csv_data()
    except DataLoadError as e:
        app.logger.critical(
            f"Initial data load failed: {e}. The application cannot start."
        )
        print(f"\nCRITICAL: Initial data load failed: {e}")
        print(
            "The application cannot start without the data file. Please check the path and file integrity."
        )
        exit(1)

    app.logger.info("Starting Flask development server...")
    app.run(debug=True, host="0.0.0.0")
