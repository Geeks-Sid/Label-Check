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
import sys
import threading
from collections import Counter, defaultdict
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

# Flask and its extensions for web framework, user management
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
from werkzeug.security import check_password_hash, generate_password_hash

# ==============================================================================
# 2. CONFIGURATION
# ==============================================================================
class Config:
    """Central configuration class for the Flask application."""

    # A secret key is required for session management and security.
    SECRET_KEY = os.environ.get(
        "SECRET_KEY", "a-super-secret-key-that-you-should-change"
    )

    # --- Path Configuration ---
    # Robustly determine the project root directory.
    # We assume this file (app.py) is in <project_root>/src
    BASE_DIR = os.path.abspath(os.path.dirname(__file__))
    PROJECT_ROOT = os.path.dirname(BASE_DIR)

    # The base directory where all data (images, CSV) is located.
    # Using absolute path ensures we can run the app from anywhere.
    IMAGE_BASE_DIR = PROJECT_ROOT

    # The full path to the primary CSV file.
    CSV_FILE_PATH = os.path.join(IMAGE_BASE_DIR, "enriched.csv")
    
    # Directory to store timestamped backups.
    BACKUP_DIR = os.path.join(BASE_DIR, "csv_backups")

    # Instance directory for local data persistence
    INSTANCE_DIR = os.path.join(BASE_DIR, "instance")
    
    # CSV persistence files
    USERS_CSV_PATH = os.path.join(INSTANCE_DIR, "users.csv")
    QUEUE_CSV_PATH = os.path.join(INSTANCE_DIR, "queue.csv")

    # Default password for the initial 'admin' user.
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
    """Configures comprehensive logging for the application."""
    if not os.path.exists("logs"):
        os.mkdir("logs")

    file_handler = RotatingFileHandler("logs/app.log", maxBytes=102400, backupCount=10)
    file_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]"
        )
    )
    file_handler.setLevel(logging.INFO)

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
# 4. APPLICATION & EXTENSIONS INITIALIZATION
# ==============================================================================
base_dir = os.path.abspath(os.path.dirname(__file__))
instance_path = os.path.join(base_dir, "instance")
template_dir = os.path.join(base_dir, "templates")

app = Flask(__name__, template_folder=template_dir, instance_path=instance_path)
app.config.from_object(Config)
os.makedirs(app.instance_path, exist_ok=True)

setup_logging(app)

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "login"
login_manager.login_message = "Please log in to access this page."
login_manager.login_message_category = "warning"


# ==============================================================================
# 5. CUSTOM EXCEPTIONS
# ==============================================================================
class DataLoadError(Exception):
    pass

class DataSaveError(Exception):
    pass

class BackupError(Exception):
    pass


# ==============================================================================
# 6. PERSISTENCE MODELS (CSV BASED)
# ==============================================================================
class User(UserMixin):
    """Represents a user account."""
    def __init__(self, id: str, password_hash: str, correction_count: int = 0, is_admin: bool = False):
        self.id = id
        self.password_hash = password_hash
        self.correction_count = int(correction_count)
        # Handle string 'True'/'False' from CSV loading
        if isinstance(is_admin, str):
            self.is_admin = is_admin.lower() == 'true'
        else:
            self.is_admin = bool(is_admin)

    def set_password(self, password: str) -> None:
        self.password_hash = generate_password_hash(password)

    def verify_password(self, password: str) -> bool:
        return check_password_hash(self.password_hash, password)

    def to_dict(self) -> Dict[str, str]:
        return {
            "id": self.id,
            "password_hash": self.password_hash,
            "correction_count": str(self.correction_count),
            "is_admin": str(self.is_admin)
        }

    def __repr__(self) -> str:
        return f"<User {self.id}>"


class QueueItem:
    """Represents a single row from the CSV in the processing queue."""
    def __init__(self, original_index: int, status: str = "pending", 
                 leased_by_id: Optional[str] = None, leased_at: Optional[Union[str, datetime.datetime]] = None,
                 completed_by_id: Optional[str] = None, completed_at: Optional[Union[str, datetime.datetime]] = None,
                 row_id: Optional[int] = None):
        self.id = row_id # ID is strictly internal/optional for QueueItem in this CSV context, but we keep track if needed.
        self.original_index = int(original_index)
        self.status = status
        self.leased_by_id = leased_by_id if leased_by_id != "" else None
        
        # Date parsing logic
        self.leased_at = self._parse_date(leased_at)
        self.completed_by_id = completed_by_id if completed_by_id != "" else None
        self.completed_at = self._parse_date(completed_at)

    def _parse_date(self, date_val: Union[str, datetime.datetime, None]) -> Optional[datetime.datetime]:
        if not date_val:
            return None
        if isinstance(date_val, datetime.datetime):
            return date_val
        try:
            return datetime.datetime.fromisoformat(date_val)
        except ValueError:
            return None

    def _format_date(self, date_val: Optional[datetime.datetime]) -> str:
        return date_val.isoformat() if date_val else ""

    def to_dict(self) -> Dict[str, str]:
        return {
            "original_index": str(self.original_index),
            "status": self.status,
            "leased_by_id": self.leased_by_id if self.leased_by_id else "",
            "leased_at": self._format_date(self.leased_at),
            "completed_by_id": self.completed_by_id if self.completed_by_id else "",
            "completed_at": self._format_date(self.completed_at)
        }

    @property
    def leased_by(self):
        """Helper to resolve user object for template compatibility."""
        if self.leased_by_id:
            # Access global user_manager
            return user_manager.get(self.leased_by_id)
        return None

    def __repr__(self) -> str:
        return f"<QueueItem {self.original_index} - {self.status}>"


class CSVManager:
    """Generic CSV persistence manager."""
    def __init__(self, filepath: str, fieldnames: List[str]):
        self.filepath = filepath
        self.fieldnames = fieldnames
        self._lock = threading.Lock()

    def _ensure_file(self):
        if not os.path.exists(self.filepath):
            with open(self.filepath, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=self.fieldnames)
                writer.writeheader()

    def read_all(self) -> List[Dict[str, str]]:
        self._ensure_file()
        with self._lock:
            try:
                with open(self.filepath, 'r', newline='', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    return list(reader)
            except Exception as e:
                app.logger.error(f"Error reading {self.filepath}: {e}")
                return []

    def write_all(self, data: List[Dict[str, str]]) -> None:
        with self._lock:
            try:
                # write atomic
                temp_path = self.filepath + ".tmp"
                with open(temp_path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=self.fieldnames)
                    writer.writeheader()
                    writer.writerows(data)
                
                os.replace(temp_path, self.filepath)
            except Exception as e:
                app.logger.error(f"Error writing {self.filepath}: {e}")
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                raise DataSaveError(f"Failed to save CSV {self.filepath}: {e}")


class UserManager(CSVManager):
    def __init__(self):
        super().__init__(Config.USERS_CSV_PATH, ["id", "password_hash", "correction_count", "is_admin"])
        # Cache users in memory for performance, similar to DB
        self.users: Dict[str, User] = {}
        self.load()

    def load(self):
        rows = self.read_all()
        self.users = {}
        for row in rows:
            u = User(
                id=row["id"],
                password_hash=row["password_hash"],
                correction_count=int(row["correction_count"]),
                is_admin=row["is_admin"]
            )
            self.users[u.id] = u

    def save(self):
        data = [u.to_dict() for u in self.users.values()]
        self.write_all(data)

    def get(self, user_id: str) -> Optional[User]:
        return self.users.get(user_id)

    def add(self, user: User):
        self.users[user.id] = user
        self.save()  # Auto-save on add due to simple architecture

    def update(self, user: User):
        self.users[user.id] = user
        self.save()
    
    def get_all(self) -> List[User]:
        return list(self.users.values())


class QueueManager(CSVManager):
    def __init__(self):
        super().__init__(Config.QUEUE_CSV_PATH, ["original_index", "status", "leased_by_id", "leased_at", "completed_by_id", "completed_at"])
        self.items: Dict[int, QueueItem] = {}
        self.load()

    def load(self):
        rows = self.read_all()
        self.items = {}
        for row in rows:
            try:
                idx = int(row["original_index"])
                item = QueueItem(
                    original_index=idx,
                    status=row["status"],
                    leased_by_id=row.get("leased_by_id"),
                    leased_at=row.get("leased_at"),
                    completed_by_id=row.get("completed_by_id"),
                    completed_at=row.get("completed_at"),
                )
                self.items[idx] = item
            except ValueError:
                continue

    def save(self):
        data = [item.to_dict() for item in self.items.values()]
        self.write_all(data)

    def get(self, original_index: int) -> Optional[QueueItem]:
        return self.items.get(original_index)
    
    def add(self, item: QueueItem):
        self.items[item.original_index] = item
        # Batch add usually calls save manually, but for single integrity:
        # self.save() 
    
    def get_all(self) -> List[QueueItem]:
        return list(self.items.values())

    def update(self):
        """Persist current state."""
        self.save()


# Initialize Managers
user_manager = UserManager()
queue_manager = QueueManager()


@login_manager.user_loader
def load_user(user_id: str) -> Optional[User]:
    return user_manager.get(user_id)


# ==============================================================================
# 7. DATA MANAGER
# ==============================================================================
class DataManager:
    """Manages the in-memory CSV data state, loading, and saving."""
    def __init__(self):
        self.data: List[Dict[str, Any]] = []
        self.headers: List[str] = []
        self._lock = threading.Lock() # Ensure thread safety for data access
        self.critical_headers = ["AccessionID", "Stain", "ParsingQCPassed", "original_slide_path"]

    def load_data(self, file_path: str = Config.CSV_FILE_PATH) -> None:
        """Loads CSV data into memory safely."""
        with self._lock:
            app.logger.info(f"Loading CSV data from: {file_path}")
            if not os.path.exists(file_path):
                raise DataLoadError(f"CSV file not found: {file_path}")

            _data: List[Dict[str, Any]] = []
            try:
                with open(file_path, "r", newline="", encoding="utf-8") as csvfile:
                    reader = csv.DictReader(csvfile, delimiter=",")
                    _headers = reader.fieldnames

                    if not _headers:
                        raise DataLoadError("CSV file is empty or has no header.")
                    
                    missing = [h for h in self.critical_headers if h not in _headers]
                    if missing:
                        app.logger.warning(
                            f"CSV is missing expected headers: {missing}. Functionality may be limited."
                        )

                    for i, row in enumerate(reader):
                        row["_original_index"] = i
                        orig_path = row.get("original_slide_location")
                        row["_identifier"] = Path(orig_path).stem if orig_path else f"Unknown_{i}"
                        row["_label_text"] = row.get("label_text", "N/A")
                        row["_macro_text"] = row.get("macro_text", "N/A")
                        row["_label_path"] = row.get("label_path")
                        row["_macro_path"] = row.get("macro_path")
                        
                        row["AccessionID"] = row.get("AccessionID", "").strip()
                        row["Stain"] = row.get("Stain", "").strip()
                        row["BlockNumber"] = row.get("BlockNumber", "").strip()
                        
                        qc_passed_str = row.get("ParsingQCPassed", "").strip()
                        row["_is_complete"] = bool(qc_passed_str)
                        _data.append(row)

                # Post-processing: Calculate per-patient file statistics
                patient_slide_ids = defaultdict(list)
                for i, row in enumerate(_data):
                    patient_slide_ids[row["_identifier"]].append(i)
                
                for _, original_indices in patient_slide_ids.items():
                    total = len(original_indices)
                    for j, original_idx in enumerate(sorted(original_indices)):
                        _data[original_idx]["_total_patient_files"] = total
                        _data[original_idx]["_patient_file_number"] = j + 1

                self.data = _data
                self.headers = _headers
                self._recalculate_accession_counts()
                app.logger.info(f"Loaded {len(self.data)} rows.")

            except Exception as e:
                self.data, self.headers = [], []
                raise DataLoadError(f"Error reading CSV: {e}")

    def save_data(self, target_path: str = Config.CSV_FILE_PATH) -> None:
        """Saves current data to CSV atomically."""
        with self._lock:
            if not self.data or not self.headers:
                app.logger.warning("Save aborted: No data in memory.")
                return

            app.logger.info(f"Saving {len(self.data)} rows to {target_path}")

            priority_fields = ["AccessionID", "Stain", "BlockNumber", "ParsingQCPassed"]
            pipeline_fields = [h for h in self.headers if h not in priority_fields]
            fieldnames = list(dict.fromkeys(priority_fields + pipeline_fields))
            
            temp_path = target_path + ".tmp"
            try:
                with open(temp_path, "w", newline="", encoding="utf-8") as csvfile:
                    writer = csv.DictWriter(
                        csvfile,
                        fieldnames=fieldnames,
                        delimiter=",",
                        extrasaction="ignore",
                        quoting=csv.QUOTE_MINIMAL,
                    )
                    writer.writeheader()

                    for row in self.data:
                        write_row = row.copy()
                        write_row["ParsingQCPassed"] = "TRUE" if row.get("_is_complete") else ""
                        writer.writerow(write_row)

                # Atomic replace
                if os.path.exists(target_path):
                    os.replace(temp_path, target_path)
                else:
                    os.rename(temp_path, target_path)
                    
                session["last_loaded_csv_mod_time"] = os.path.getmtime(target_path)
                app.logger.info("Save successful.")
            except Exception as e:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                raise DataSaveError(f"Failed to save CSV: {e}")

    def _recalculate_accession_counts(self) -> None:
        """Internal helper to count AccessionID occurrences."""
        if not self.data:
            return
        id_counts = Counter(
            row.get("AccessionID", "").strip()
            for row in self.data
            if row.get("AccessionID", "").strip()
        )
        for row in self.data:
            current_id = row.get("AccessionID", "").strip()
            row["_accession_id_count"] = id_counts[current_id] if current_id else 0
    
    def get_row(self, index: int) -> Optional[Dict[str, Any]]:
        with self._lock:
            if 0 <= index < len(self.data):
                return self.data[index]
            return None

    def update_row(self, index: int, updates: Dict[str, Any]) -> bool:
        """Updates a row and triggers recalculations if needed."""
        with self._lock:
            if not (0 <= index < len(self.data)):
                return False
            
            row = self.data[index]
            has_changed = False
            recalc_counts = False

            for key, value in updates.items():
                if row.get(key) != value:
                    row[key] = value
                    has_changed = True
                    if key == "AccessionID":
                        recalc_counts = True
            
            if recalc_counts:
                self._recalculate_accession_counts()
                
            return has_changed

    def clear(self):
        with self._lock:
            self.data = []
            self.headers = []

    def get_absolute_path(self, relative_path: str) -> Optional[str]:
        """Resolves a relative path from the CSV to an absolute system path."""
        if not relative_path:
            return None
            
        # Handle the specific NP-22-data prefix issue if present
        cleaned_path = relative_path
        if 'NP-22-data' in cleaned_path:
            path_parts = cleaned_path.split('NP-22-data', 1)
            if len(path_parts) > 1:
                cleaned_path = path_parts[1].lstrip('.\\/')
        
        full_path = os.path.join(Config.IMAGE_BASE_DIR, cleaned_path)
        return os.path.abspath(full_path)

    def check_paths(self) -> List[str]:
        """Checks if all image paths in the loaded data exist and are readable."""
        missing_or_unreadable = []
        with self._lock:
            for i, row in enumerate(self.data):
                # Check Label and Macro paths
                for key in ["_label_path", "_macro_path"]:
                    rel_path = row.get(key)
                    if rel_path:
                        abs_path = self.get_absolute_path(rel_path)
                        if not abs_path or not os.path.exists(abs_path):
                             missing_or_unreadable.append(f"Row {i+1} ({key}): Path not found -> {abs_path}")
                        elif not os.access(abs_path, os.R_OK):
                             missing_or_unreadable.append(f"Row {i+1} ({key}): Path not readable -> {abs_path}")
        return missing_or_unreadable

# Initialize Global DataManager
data_manager = DataManager()

# ==============================================================================
# 8. HELPER FUNCTIONS
# ==============================================================================
def _release_expired_leases():
    """Scans for and releases any item leases that have expired."""
    lease_duration = datetime.timedelta(seconds=app.config["LEASE_DURATION_SECONDS"])
    expired_time = datetime.datetime.utcnow() - lease_duration

    # Look for expired leases in QueueManager
    expired_items = [
        item for item in queue_manager.get_all()
        if item.status == "leased" and item.leased_at and item.leased_at < expired_time
    ]

    if expired_items:
        count = 0
        for item in expired_items:
            try:
                row = data_manager.get_row(item.original_index)
                acc_id = row.get("AccessionID", "Unknown") if row else "Unknown"
                
                app.logger.info(
                    f"Lease expired for item {item.original_index} ({acc_id}), leased by {item.leased_by_id}."
                )
                item.status = "pending"
                item.leased_by_id = None
                item.leased_at = None
                count += 1
            except Exception as e:
                app.logger.error(f"Error releasing lease for item {item.original_index}: {e}")
        
        if count > 0:
            queue_manager.save()
            flash(
                f"{count} item(s) had expired leases and were returned to the queue.",
                "warning",
            )


def _create_backup(suffix: str = "") -> None:
    """Creates a timestamped backup of the current CSV file."""
    if not os.path.exists(Config.CSV_FILE_PATH):
        return
    try:
        os.makedirs(Config.BACKUP_DIR, exist_ok=True)
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = os.path.basename(Config.CSV_FILE_PATH)
        name_part = f"{filename}_{timestamp}"
        if suffix:
            name_part += f"_{suffix}"
        backup_path = os.path.join(Config.BACKUP_DIR, f"{name_part}.bak")
        shutil.copy2(Config.CSV_FILE_PATH, backup_path)
    except Exception as e:
        raise BackupError(f"Backup failed: {e}")


def _is_row_incomplete(row_dict: Dict[str, Any]) -> bool:
    return not row_dict.get("_is_complete", False)


def flash_messages() -> List[Dict[str, str]]:
    return [
        {"category": category, "message": message}
        for category, message in get_flashed_messages(with_categories=True)
    ]


# ==============================================================================
# 9. FLASK ROUTES
# ==============================================================================
@app.before_request
def before_request_handler():
    if request.endpoint in ["static", "serve_relative_image", "login", "logout"]:
        return

    session.setdefault("show_only_incomplete", False)
    path = Config.CSV_FILE_PATH
    
    if not os.path.exists(path):
        if data_manager.data:
            app.logger.critical(f"FATAL: CSV file disappeared from {path}. Clearing data.")
            data_manager.clear()
        return

    try:
        mod_time = os.path.getmtime(path)
        if not data_manager.data or mod_time != session.get("last_loaded_csv_mod_time"):
            app.logger.info("CSV file change detected or not loaded. Loading...")
            data_manager.load_data()
            session["last_loaded_csv_mod_time"] = mod_time
            flash("Data loaded/refreshed from disk.", "info")
    except DataLoadError as e:
        app.logger.error(f"Auto-reload failed: {e}")
        flash("Error: Could not auto-reload data from disk.", "error")


@app.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        return redirect(url_for("index"))
    
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        # Use UserManager
        user = user_manager.get(username)
        
        if user and user.verify_password(password):
            login_user(user)
            app.logger.info(f"User '{username}' logged in successfully.")
            return redirect(request.args.get("next") or url_for("index"))
        
        flash("Invalid username or password.", "error")
        
    return render_template("login.html", messages=flash_messages())


@app.route("/logout")
@login_required
def logout():
    logout_user()
    flash("You have been logged out.", "info")
    return redirect(url_for("login"))


@app.route("/users")
@login_required
def users_management():
    if not current_user.is_admin:
        flash("You do not have permission to access this page.", "error")
        return redirect(url_for("index"))
        
    users = user_manager.get_all()
    return render_template("users.html", users=users, messages=flash_messages())


@app.route("/add_user", methods=["POST"])
@login_required
def add_user():
    if not current_user.is_admin:
        return redirect(url_for("index"))
        
    username = request.form.get("username", "").strip()
    password = request.form.get("password", "").strip()
    
    if not username or not password:
        flash("Username and password are required.", "error")
        return redirect(url_for("users_management"))
        
    if user_manager.get(username):
        flash(f"User '{username}' already exists.", "error")
        return redirect(url_for("users_management"))
        
    try:
        u = User(id=username, password_hash="", is_admin=(request.form.get("is_admin") == "on"))
        u.set_password(password)
        user_manager.add(u)
        flash(f"User '{username}' created successfully.", "success")
    except Exception as e:
        flash(f"An error occurred while adding the user: {e}", "error")
        
    return redirect(url_for("users_management"))


@app.route("/", methods=["GET"])
@login_required
def index():
    if not data_manager.data:
        return render_template(
            "index.html",
            error_message="CSV data could not be loaded. Please check the file path and logs.",
            data_loaded=False,
            messages=flash_messages(),
        )

    _release_expired_leases()
    queue_manager.load() # Refresh queue from disk in case other processes updated it? Or just rely on in-memory for this single-process app?
    # Since this is likely a single-worker Flask app (debug mode), in-memory shared state is OK, but for robustness with file changes:
    # We will trust the queue_manager state which is in-memory and persisted only on save. 
    # NOTE: If multiple workers, we should reload. Assuming single process for simplicity of CSV backend.

    item_to_display = None
    requested_index_str = request.args.get("index")

    # 1. User requested specific index
    if requested_index_str:
        try:
            idx = int(requested_index_str)
            if 0 <= idx < len(data_manager.data):
                # Release existing leases for this user that are not the requested one
                existing_leases = [
                    l for l in queue_manager.get_all() 
                    if l.leased_by_id == current_user.id and l.status == "leased"
                ]
                for lease in existing_leases:
                    if lease.original_index != idx:
                        lease.status = "pending"
                        lease.leased_by_id = None
                        lease.leased_at = None
                
                qi = queue_manager.get(idx)
                if not qi: 
                    # Should exist if created in init, but if not create ephemeral or fail?
                    # We assume queue is sync'd. 
                    qi = QueueItem(original_index=idx)
                    queue_manager.add(qi)

                if qi.status == "leased" and qi.leased_by_id != current_user.id:
                    flash("This item is currently leased by another user. Viewing in read-only mode.", "warning")
                elif qi.status != "completed":
                    # Acquiring lease
                    qi.status = "leased"
                    qi.leased_by_id = current_user.id
                    qi.leased_at = datetime.datetime.utcnow()
                
                queue_manager.save()
                item_to_display = qi
        except (ValueError, TypeError):
            flash("Invalid index provided in URL.", "error")

    # 2. Check active lease
    if not item_to_display:
        active_lease = next(
            (i for i in queue_manager.get_all() if i.leased_by_id == current_user.id and i.status == "leased"),
            None
        )

        if active_lease:
            item_to_display = active_lease
        else:
            # 3. Get next pending
            # Sort by original_index 
            pending_items = sorted(
                [i for i in queue_manager.get_all() if i.status == "pending"],
                key=lambda x: x.original_index
            )
            
            if pending_items:
                next_pending_item = pending_items[0]
                next_pending_item.status = "leased"
                next_pending_item.leased_by_id = current_user.id
                next_pending_item.leased_at = datetime.datetime.utcnow()
                queue_manager.save()
                item_to_display = next_pending_item
            else:
                # 4. No items left
                total = len(queue_manager.items)
                done = len([i for i in queue_manager.get_all() if i.status == "completed"])
                return render_template(
                    "index.html",
                    no_items_left=True,
                    completed_count=done,
                    total_count=total,
                    messages=flash_messages(),
                )

    current_index = item_to_display.original_index
    row_data = data_manager.get_row(current_index)
    if not row_data:
        flash("Error: Database index mismatch with CSV. Reloading data...", "error")
        data_manager.load_data()
        return redirect(url_for("index"))

    display_row_data = row_data.copy()

    # Pre-fill logic safe lookup
    identifier = display_row_data.get("_identifier")
    if not display_row_data.get("AccessionID") and identifier:
        for r in data_manager.data:
            if r.get("_identifier") == identifier and r.get("AccessionID"):
                display_row_data["AccessionID"] = r["AccessionID"]
                flash(f"Auto-filled Accession ID '{r['AccessionID']}' from a related file.", "info")
                if not display_row_data.get("Stain") and r.get("Stain"):
                    display_row_data["Stain"] = r["Stain"]
                break

    # Image Paths
    label_image_url, macro_image_url = None, None
    label_image_exists, macro_image_exists = False, False

    def resolve_image_path(csv_path_key):
        csv_path = display_row_data.get(csv_path_key)
        if csv_path:
            full_path = data_manager.get_absolute_path(csv_path)
            if full_path and os.path.exists(full_path):
                cleaned_path = csv_path
                if 'NP-22-data' in cleaned_path:
                     parts = cleaned_path.split('NP-22-data', 1)
                     if len(parts) > 1:
                         cleaned_path = parts[1].lstrip('.\\/')
                
                return url_for("serve_relative_image", filepath=cleaned_path), True
        return None, False

    label_image_url, label_image_exists = resolve_image_path("_label_path")
    macro_image_url, macro_image_exists = resolve_image_path("_macro_path")

    queue_stats = {
        "pending": len([i for i in queue_manager.get_all() if i.status == "pending"]),
        "leased": len([i for i in queue_manager.get_all() if i.status == "leased"]),
        "completed": len([i for i in queue_manager.get_all() if i.status == "completed"]),
    }
    
    recently_completed_items = sorted(
        [i for i in queue_manager.get_all() if i.completed_by_id == current_user.id],
        key=lambda x: x.completed_at if x.completed_at else datetime.datetime.min,
        reverse=True
    )[:5]
    
    # Enrich for template (needs accession_id)
    recently_completed = []
    for r in recently_completed_items:
        rr = data_manager.get_row(r.original_index)
        # Create a proxy object or dict for template
        r_dict = r.to_dict()
        r_dict['accession_id'] = rr.get("AccessionID", "N/A") if rr else "N/A"
        # Overwrite string date with object for strftime support in template
        r_dict['completed_at'] = r.completed_at 
        recently_completed.append(r_dict)

    return render_template(
        "index.html",
        row=display_row_data,
        original_index=current_index,
        total_original_rows=len(data_manager.data),
        label_img_path=label_image_url,
        macro_img_path=macro_image_url,
        label_img_exists=label_image_exists,
        macro_img_exists=macro_image_exists,
        messages=flash_messages(),
        data_loaded=True,
        queue_stats=queue_stats,
        lease_info=item_to_display,
        datetime=datetime.datetime,
        timedelta=datetime.timedelta,
        recently_completed=recently_completed,
    )


@app.route("/update", methods=["POST"])
@login_required
def update():
    """Handles the form submission for saving corrections."""
    if not data_manager.data:
        return redirect(url_for("index"))
        
    try:
        idx = int(request.form.get("original_index", -1))
        if idx < 0:
            raise ValueError("Invalid index")

        qi = queue_manager.get(idx)

        if not qi:
            flash("Error: Item not found in queue.", "error")
            return redirect(url_for("index"))

        # --- SAFETY CHECK: LEASE VALIDATION ---
        is_forced_save = False
        
        # Case A: Item is completed.
        if qi.status == "completed":
            flash("Cannot save changes: This item has already been completed.", "error")
            return redirect(url_for("index"))

        # Case B: I hold the lease.
        if qi.leased_by_id == current_user.id:
            pass # Valid save

        # Case C: Leased by SOMEONE ELSE.
        elif qi.status == "leased" and qi.leased_by_id != current_user.id:
            # Check for lease expiry just in case
            _release_expired_leases()
            # Reload queue just to be sure
            qi = queue_manager.get(idx)
            if qi.status == "leased" and qi.leased_by_id != current_user.id:
                flash("SAVE BLOCKED: This item is currently currently leased by another user.", "error")
                return redirect(url_for("index"))
            # If after refresh it's effectively pending, we fall through to Case D.
            is_forced_save = True

        # Case D: Item is pending (lease expired or never leased).
        elif qi.status == "pending":
            is_forced_save = True # Allowed to pick up

        # --- Update Data ---
        new_values = {
            "AccessionID": request.form.get("accession_id", "").strip(),
            "Stain": request.form.get("stain", "").strip(),
            "BlockNumber": request.form.get("block_number", "").strip(),
            "_is_complete": request.form.get("complete") == "on"
        }
        
        # Validation for completion
        if new_values["_is_complete"]:
            if not new_values["AccessionID"] or not new_values["Stain"]:
                flash("Cannot mark as complete: Accession ID and Stain are required.", "warning")
                new_values["_is_complete"] = False

        # Apply updates
        has_changed = data_manager.update_row(idx, new_values)

        if has_changed:
            current_user.correction_count += 1
            user_manager.save()
            
            if request.form.get("action") == "next" and new_values["_is_complete"]:
                qi.status = "completed"
                qi.completed_by_id = current_user.id
                qi.completed_at = datetime.datetime.utcnow()
            elif is_forced_save:
                qi.status = "leased"
                qi.leased_by_id = current_user.id
                qi.leased_at = datetime.datetime.utcnow()
            
            queue_manager.save()

            try:
                _create_backup()
                data_manager.save_data()
                flash("Changes saved successfully.", "success")
                
                # --- CHECK IF LIST IS DONE ---
                remaining = len([i for i in queue_manager.get_all() if i.status != "completed"])
                
                if remaining == 0:
                    flash("🎉 ALL ITEMS COMPLETED! A final comprehensive backup has been created.", "success")
                    app.logger.info("All items completed. Creating final backup.")
                    _create_backup(suffix="FINAL_COMPLETED")

            except Exception as e:
                app.logger.error(f"Save operation failed: {e}")
                flash("CRITICAL: Error saving changes to the CSV file.", "error")

        return redirect(url_for("index"))

    except Exception as e:
        app.logger.error(f"Update failed: {e}")
        flash("An error occurred during the update.", "error")
        return redirect(url_for("index"))


@app.route("/history")
@login_required
def history():
    history_items = sorted(
        [i for i in queue_manager.get_all() if i.completed_by_id == current_user.id],
        key=lambda x: x.completed_at if x.completed_at else datetime.datetime.min,
        reverse=True
    )
    
    # Enhance for template
    display_history = []
    for item in history_items:
        d = item.to_dict()
        row = data_manager.get_row(item.original_index)
        d['accession_id'] = row.get("AccessionID", "N/A") if row else "N/A"
        d['completed_at'] = item.completed_at
        display_history.append(d)

    return render_template("history.html", completed_items=display_history, messages=flash_messages())


@app.route("/release", methods=["POST"])
@login_required
def release_lease():
    leases = [
        l for l in queue_manager.get_all() 
        if l.leased_by_id == current_user.id and l.status == "leased"
    ]
    
    if leases:
        for lease in leases:
            lease.status = "pending"
            lease.leased_by_id = None
            lease.leased_at = None
        queue_manager.save()
        flash(f"Successfully released {len(leases)} item(s) back to the queue.", "info")
        
    return redirect(url_for("index"))


@app.route("/search", methods=["POST"])
@login_required
def search():
    if not data_manager.data:
        return redirect(url_for("index"))
        
    search_term = request.form.get("search_term", "").strip().lower()
    if not search_term:
        return redirect(url_for("index"))

    for i, row in enumerate(data_manager.data):
        if (
            search_term in row.get("AccessionID", "").lower() or
            search_term in row.get("_identifier", "").lower() or
            search_term == row.get("BlockNumber", "").lower()
        ):
            return redirect(url_for("index", index=i))

    flash(f"No item found matching '{search_term}'.", "warning")
    return redirect(url_for("index"))


@app.route("/data_images/<path:filepath>")
@login_required
def serve_relative_image(filepath: str):
    abs_image_dir = os.path.abspath(Config.IMAGE_BASE_DIR)
    abs_file_path = os.path.abspath(os.path.join(abs_image_dir, filepath))

    if os.path.commonpath([abs_image_dir, abs_file_path]) != abs_image_dir:
        app.logger.warning(f"Path traversal attempt blocked for filepath: {filepath}")
        return "Access denied: Invalid file path.", 403

    if not os.path.exists(abs_file_path):
        return "Image not found on server.", 404

    directory, filename = os.path.split(abs_file_path)
    return send_from_directory(directory, filename)


# ==============================================================================
# 10. CLI COMMANDS
# ==============================================================================
@app.cli.command("init-db")
@with_appcontext
def init_db_command():
    print("--- Initializing App Persistence (CSV) ---")
    
    # Init Users
    if not user_manager.get("admin"):
        u = User(id="admin", password_hash="", is_admin=True)
        u.set_password(Config.ADMIN_DEFAULT_PASSWORD)
        user_manager.add(u)
        print(f"Created default 'admin' user in {Config.USERS_CSV_PATH}")
    else:
        print("'admin' user already exists.")

    # Init Queue from Data CSV
    if os.path.exists(Config.CSV_FILE_PATH):
        try:
            data_manager.load_data()
            print("Verifying data integrity (checking file paths)...")
            errors = data_manager.check_paths()
            if errors:
                print("CRITICAL ERROR: Found missing or unreadable files.")
                for e in errors[:10]:
                    print(f"  - {e}")
                if len(errors) > 10:
                    print(f"  ... and {len(errors) - 10} more issues.")
                print("Aborting initialization due to data integrity/safety check.")
                return

            existing_indices = {item.original_index for item in queue_manager.get_all()}
            
            new_items_count = 0
            for row in data_manager.data:
                idx = row["_original_index"]
                if idx not in existing_indices:
                    status = "completed" if row["_is_complete"] else "pending"
                    qi = QueueItem(original_index=idx, status=status)
                    queue_manager.add(qi)
                    new_items_count += 1
            
            queue_manager.save()
            print(f"Successfully added/synced {new_items_count} items to the processing queue.")
            
        except Exception as e:
            print(f"ERROR: Could not populate queue from CSV. Reason: {e}")
    else:
        print(f"WARNING: CSV file not found at {Config.CSV_FILE_PATH}. Queue was not populated.")

    print("--- Initialization complete. ---")


if __name__ == "__main__":
    if os.path.exists(Config.CSV_FILE_PATH):
        try:
            with app.app_context():
                data_manager.load_data()
                print("Verifying data integrity (checking file paths)...")
                errors = data_manager.check_paths()
                if errors:
                    print("\n" + "="*60)
                    print("CRITICAL ERROR: Found missing or unreadable files.")
                    print("The application cannot start until these are resolved.")
                    print("="*60)
                    for e in errors[:20]:
                        print(f"  - {e}")
                    if len(errors) > 20:
                        print(f"  ... and {len(errors) - 20} more issues.")
                    print("="*60 + "\n")
                    sys.exit(1)
                print("Data integrity check passed.")
        except Exception as e:
            print(f"FATAL: Failed during startup checks: {e}")
            sys.exit(1)

    app.run(debug=True, host="0.0.0.0")
