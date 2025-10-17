import csv
import datetime
import logging
import os
import shutil
from collections import Counter, defaultdict
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

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

    # Points to the directory containing images and the CSV
    # Assuming structure: Data/final_enriched_mapping.csv and Data/macro/..., Data/label/...
    IMAGE_BASE_DIR = r"..\NP-22-data"
    CSV_FILE_PATH = os.path.join(IMAGE_BASE_DIR, "ocr_processed_parsed.csv")
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
    file_handler = RotatingFileHandler("logs/app.log", maxBytes=102400, backupCount=10)
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
# Determine path relative to this script file
base_dir = os.path.abspath(os.path.dirname(__file__))
instance_path = os.path.join(base_dir, "instance")
template_dir = os.path.join(base_dir, "templates")

app = Flask(__name__, template_folder=template_dir, instance_path=instance_path)
app.config.from_object(Config)

# Ensure instance directory exists for SQLite DB
os.makedirs(app.instance_path, exist_ok=True)

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
    return db.session.get(User, user_id)


# ==============================================================================
# 6. GLOBAL DATA STORE
# ==============================================================================
# NOTE: Not safe for multi-worker production environments (e.g., Gunicorn).
data: List[Dict[str, Any]] = []
headers: List[str] = []


# ==============================================================================
# 7. HELPER FUNCTIONS
# ==============================================================================
def _release_expired_leases():
    """Scans for and releases any leases that have expired."""
    lease_duration = datetime.timedelta(seconds=app.config["LEASE_DURATION_SECONDS"])
    expired_time = datetime.datetime.utcnow() - lease_duration

    expired_items = (
        db.session.execute(
            db.select(QueueItem).filter(
                QueueItem.status == "leased", QueueItem.leased_at < expired_time
            )
        )
        .scalars()
        .all()
    )

    if expired_items:
        count = 0
        for item in expired_items:
            # Double check in case data was reloaded and index is out of bounds
            if item.original_index < len(data):
                acc_id = data[item.original_index].get("AccessionID", "Unknown")
                app.logger.info(
                    f"Lease expired for item {item.original_index} ({acc_id}), leased by {item.leased_by_id}."
                )
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
    """Recalculates and updates the count for each AccessionID in the global data."""
    global data
    if not data:
        return
    # app.logger.debug("Recalculating AccessionID counts.")
    id_counts = Counter(
        row.get("AccessionID", "").strip()
        for row in data
        if row.get("AccessionID", "").strip()
    )
    for row in data:
        current_id = row.get("AccessionID", "").strip()
        row["_accession_id_count"] = id_counts[current_id] if current_id else 0


def _is_row_incomplete(row_dict: Dict[str, Any]) -> bool:
    """Checks if a row is internally marked as incomplete."""
    return not row_dict.get("_is_complete", False)


def get_current_display_list_indices() -> List[int]:
    """Returns list of original indices based on 'show_only_incomplete' filter."""
    if not data:
        return []
    if session.get("show_only_incomplete"):
        return [i for i, row in enumerate(data) if _is_row_incomplete(row)]
    return list(range(len(data)))


# ==============================================================================
# 8. CORE DATA I/O FUNCTIONS (UPDATED FOR NEW CSV SCHEMA)
# ==============================================================================
def load_csv_data(file_path: str = Config.CSV_FILE_PATH) -> None:
    """
    Loads data from the new pipeline CSV format into global memory.
    Expected columns: macro_path, label_path, original_slide_location, label_text,
                      macro_text, AccessionID, Stain, ParsingQCPassed, etc.
    """
    global data, headers
    app.logger.info(f"Loading CSV data from: {file_path}")

    if not os.path.exists(file_path):
        raise DataLoadError(f"CSV file not found: {file_path}")

    _data: List[Dict[str, Any]] = []
    # Columns essential for the UI to function
    critial_headers = [
        "AccessionID",
        "Stain",
        "ParsingQCPassed",
        "original_slide_location",
    ]

    try:
        with open(file_path, "r", newline="", encoding="utf-8") as csvfile:
            # Use comma delimiter for the new CSV format
            reader = csv.DictReader(csvfile, delimiter=",")
            _headers = reader.fieldnames

            if not _headers:
                raise DataLoadError("CSV file is empty or has no header.")

            missing = [h for h in critial_headers if h not in _headers]
            if missing:
                # Don't crash, but warn. The pipeline might have failed partially.
                app.logger.warning(
                    f"CSV is missing expected headers: {missing}. Functionality may be limited."
                )

            for i, row in enumerate(reader):
                # --- Internal Metadata ---
                row["_original_index"] = i

                # Derive identifier from original filename for grouping
                orig_path = row.get("original_slide_location")
                row["_identifier"] = (
                    Path(orig_path).stem if orig_path else f"Unknown_{i}"
                )

                # Map OCR text for display
                row["_label_text"] = row.get("label_text", "N/A")
                row["_macro_text"] = row.get("macro_text", "N/A")

                # Map image paths
                row["_label_path"] = row.get("label_path")
                row["_macro_path"] = row.get("macro_path")

                # --- Editable Fields ---
                # Ensure they exist even if empty in CSV
                row["AccessionID"] = row.get("AccessionID", "").strip()
                row["Stain"] = row.get("Stain", "").strip()
                # BlockNumber is not in pipeline CSV, but allow users to add it
                row["BlockNumber"] = row.get("BlockNumber", "").strip()

                # --- Completion Status ---
                # Map 'ParsingQCPassed' column to internal boolean.
                # Assumes non-empty string (e.g., "TRUE") means QC passed.
                qc_passed_str = row.get("ParsingQCPassed", "").strip()
                row["_is_complete"] = bool(qc_passed_str)

                _data.append(row)

        # Calculate per-patient file statistics
        patient_slide_ids = defaultdict(list)
        for i, row in enumerate(_data):
            patient_slide_ids[row["_identifier"]].append(i)

        for _, original_indices in patient_slide_ids.items():
            total = len(original_indices)
            for j, original_idx in enumerate(sorted(original_indices)):
                _data[original_idx]["_total_patient_files"] = total
                _data[original_idx]["_patient_file_number"] = j + 1

        data, headers = _data, _headers
        _recalculate_accession_counts()
        app.logger.info(f"Loaded {len(data)} rows.")

    except Exception as e:
        data, headers = [], []
        raise DataLoadError(f"Error reading CSV: {e}")


def save_csv_data(target_path: str = Config.CSV_FILE_PATH) -> None:
    """
    Saves in-memory data back to CSV, mapping internal state to CSV columns.
    """
    global data, headers
    if not data or not headers:
        return

    app.logger.info(f"Saving {len(data)} rows to {target_path}")

    # Define field order: Core editable fields first, then original pipeline fields, then user-added
    priority_fields = ["AccessionID", "Stain", "BlockNumber", "ParsingQCPassed"]
    pipeline_fields = [h for h in headers if h not in priority_fields]

    # Combine, removing duplicates while preserving order
    fieldnames = list(dict.fromkeys(priority_fields + pipeline_fields))

    temp_path = target_path + ".tmp"
    try:
        with open(temp_path, "w", newline="", encoding="utf-8") as csvfile:
            # Use comma delimiter and minimal quoting
            writer = csv.DictWriter(
                csvfile,
                fieldnames=fieldnames,
                delimiter=",",
                extrasaction="ignore",
                quoting=csv.QUOTE_MINIMAL,
            )
            writer.writeheader()

            for row in data:
                write_row = row.copy()
                # Map internal completion boolean back to CSV string column
                write_row["ParsingQCPassed"] = "TRUE" if row.get("_is_complete") else ""
                writer.writerow(write_row)

        os.replace(temp_path, target_path)
        session["last_loaded_csv_mod_time"] = os.path.getmtime(target_path)
        app.logger.info("Save successful.")

    except Exception as e:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        raise DataSaveError(f"Failed to save CSV: {e}")


def _create_backup() -> None:
    """Creates a timestamped copy of the CSV file."""
    if not os.path.exists(Config.CSV_FILE_PATH):
        return
    try:
        os.makedirs(Config.BACKUP_DIR, exist_ok=True)
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = os.path.basename(Config.CSV_FILE_PATH)
        backup_path = os.path.join(Config.BACKUP_DIR, f"{filename}_{timestamp}.bak")
        shutil.copy2(Config.CSV_FILE_PATH, backup_path)
        # app.logger.debug(f"Backup created: {backup_path}")
    except Exception as e:
        raise BackupError(f"Backup failed: {e}")


# ==============================================================================
# 9. FLASK ROUTES
# ==============================================================================
@app.before_request
def before_request_handler():
    """Checks for CSV changes on disk and reloads if necessary."""
    if request.endpoint in ["static", "serve_relative_image", "login", "logout"]:
        return

    session.setdefault("show_only_incomplete", False)

    path = Config.CSV_FILE_PATH
    if not os.path.exists(path):
        if data:  # File deleted while running
            app.logger.critical(f"CSV file disappeared: {path}")
            data.clear()
        return

    try:
        mod_time = os.path.getmtime(path)
        if not data or mod_time != session.get("last_loaded_csv_mod_time"):
            load_csv_data()
            session["last_loaded_csv_mod_time"] = mod_time
    except DataLoadError as e:
        app.logger.error(f"Auto-reload failed: {e}")


# --- Authentication ---
@app.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        return redirect(url_for("index"))
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        user = db.session.get(User, username)
        if user and user.verify_password(password):
            login_user(user)
            app.logger.info(f"User '{username}' logged in.")
            return redirect(request.args.get("next") or url_for("index"))
        flash("Invalid credentials.", "error")
    return render_template("login.html", messages=flash_messages())


@app.route("/logout")
@login_required
def logout():
    logout_user()
    flash("Logged out.", "info")
    return redirect(url_for("login"))


# --- Admin ---
@app.route("/users")
@login_required
def users_management():
    if not current_user.is_admin:
        return redirect(url_for("index"))
    users = db.session.execute(db.select(User)).scalars().all()
    return render_template("users.html", users=users, messages=flash_messages())


@app.route("/add_user", methods=["POST"])
@login_required
def add_user():
    if not current_user.is_admin:
        return redirect(url_for("index"))
    username = request.form.get("username", "").strip()
    password = request.form.get("password", "").strip()
    if not username or not password:
        flash("Missing username or password.", "error")
        return redirect(url_for("users_management"))
    if db.session.get(User, username):
        flash("User exists.", "error")
        return redirect(url_for("users_management"))
    try:
        u = User(id=username, is_admin=(request.form.get("is_admin") == "on"))
        u.set_password(password)
        db.session.add(u)
        db.session.commit()
        flash(f"User '{username}' added.", "success")
    except Exception as e:
        db.session.rollback()
        flash(f"Error: {e}", "error")
    return redirect(url_for("users_management"))


# --- Main App ---
@app.route("/", methods=["GET"])
@login_required
def index():
    if not data:
        return render_template(
            "index.html",
            error_message="CSV data not loaded.",
            data_loaded=False,
            messages=flash_messages(),
        )

    _release_expired_leases()

    # Determine which item to show
    item_to_display = None
    req_idx = request.args.get("index")

    if req_idx:
        try:
            idx = int(req_idx)
            if 0 <= idx < len(data):
                # Release existing leases if moving to a different index
                existing_leases = (
                    db.session.execute(
                        db.select(QueueItem).filter_by(
                            leased_by_id=current_user.id, status="leased"
                        )
                    )
                    .scalars()
                    .all()
                )
                for lease in existing_leases:
                    if lease.original_index != idx:
                        lease.status = "pending"
                        lease.leased_by = None
                        lease.leased_at = None

                qi = (
                    db.session.execute(
                        db.select(QueueItem).filter_by(original_index=idx)
                    )
                    .scalars()
                    .first()
                )
                if qi:
                    if qi.status == "leased" and qi.leased_by_id != current_user.id:
                        flash("Read-only mode: Item leased by another user.", "warning")
                    elif qi.status != "completed":
                        qi.status = "leased"
                        qi.leased_by_id = current_user.id
                        qi.leased_at = datetime.datetime.utcnow()
                    item_to_display = qi
        except ValueError:
            pass

    if not item_to_display:
        # 1. Continue current lease
        active = (
            db.session.execute(
                db.select(QueueItem).filter_by(
                    leased_by_id=current_user.id, status="leased"
                )
            )
            .scalars()
            .first()
        )

        if active:
            item_to_display = active
        else:
            # 2. Get next pending
            nxt = db.session.execute(
                db.select(QueueItem)
                .filter_by(status="pending")
                .order_by(QueueItem.original_index)
            ).scalars().first()
            if nxt:
                nxt.status = "leased"
                nxt.leased_by_id = current_user.id
                nxt.leased_at = datetime.datetime.utcnow()
                item_to_display = nxt
            else:
                # 3. Nothing left
                db.session.commit()  # Save any lease releases
                total = db.session.query(func.count(QueueItem.id)).scalar()
                done = (
                    db.session.query(func.count(QueueItem.id))
                    .filter_by(status="completed")
                    .scalar()
                )
                return render_template(
                    "index.html",
                    no_items_left=True,
                    completed_count=done,
                    total_count=total,
                    messages=flash_messages(),
                )

    db.session.commit()

    curr_idx = item_to_display.original_index
    row = data[curr_idx]
    display_row = row.copy()

    # Pre-fill logic (AccessionID propagation)
    ident = display_row.get("_identifier")
    if not display_row.get("AccessionID") and ident:
        # Find an AccessionID used by other files from same patient
        for r in data:
            if r.get("_identifier") == ident and r.get("AccessionID"):
                display_row["AccessionID"] = r["AccessionID"]
                flash(
                    f"Propagated Accession ID '{r['AccessionID']}' from sibling file.",
                    "info",
                )
                # Also propagate stain if empty
                if not display_row.get("Stain") and r.get("Stain"):
                    display_row["Stain"] = r["Stain"]
                break

    # Image path resolution based on CSV relative paths
    lbl_url, mac_url = None, None
    lbl_ok, mac_ok = False, False

    csv_lbl_path = display_row.get("_label_path")
    if csv_lbl_path:
        # If path from CSV incorrectly contains the base dir, remove it
        if 'NP-22-data' in csv_lbl_path:
            path_parts = csv_lbl_path.split('NP-22-data', 1)
            if len(path_parts) > 1:
                csv_lbl_path = path_parts[1].lstrip('.\\/')

        full_path = os.path.join(Config.IMAGE_BASE_DIR, csv_lbl_path)
        if os.path.exists(full_path):
            # Pass the relative path from CSV to the serving route
            lbl_url = url_for("serve_relative_image", filepath=csv_lbl_path)
            lbl_ok = True

    csv_mac_path = display_row.get("_macro_path")
    if csv_mac_path:
        # If path from CSV incorrectly contains the base dir, remove it
        if 'NP-22-data' in csv_mac_path:
            path_parts = csv_mac_path.split('NP-22-data', 1)
            if len(path_parts) > 1:
                csv_mac_path = path_parts[1].lstrip('.\\/')

        full_path = os.path.join(Config.IMAGE_BASE_DIR, csv_mac_path)
        if os.path.exists(full_path):
            mac_url = url_for("serve_relative_image", filepath=csv_mac_path)
            mac_ok = True

    # Stats for UI
    q_stats = {
        "pending": db.session.query(func.count(QueueItem.id))
        .filter_by(status="pending")
        .scalar(),
        "leased": db.session.query(func.count(QueueItem.id))
        .filter_by(status="leased")
        .scalar(),
        "completed": db.session.query(func.count(QueueItem.id))
        .filter_by(status="completed")
        .scalar(),
    }

    recent = (
        db.session.execute(
            db.select(QueueItem)
            .filter_by(completed_by_id=current_user.id)
            .order_by(QueueItem.completed_at.desc())
            .limit(5)
        )
        .scalars()
        .all()
    )
    for r in recent:
        if r.original_index < len(data):
            r.accession_id = data[r.original_index].get("AccessionID", "N/A")

    return render_template(
        "index.html",
        row=display_row,
        original_index=curr_idx,
        total_original_rows=len(data),
        label_img_path=lbl_url,
        macro_img_path=mac_url,
        label_img_exists=lbl_ok,
        macro_img_exists=mac_ok,
        messages=flash_messages(),
        data_loaded=True,
        queue_stats=q_stats,
        lease_info=item_to_display,
        datetime=datetime.datetime,
        timedelta=datetime.timedelta,
        recently_completed=recent,
    )


@app.route("/history")
@login_required
def history():
    """Displays the user's full history of completed items."""
    history_items = (
        db.session.execute(
            db.select(QueueItem)
            .filter_by(completed_by_id=current_user.id)
            .order_by(QueueItem.completed_at.desc())
        )
        .scalars()
        .all()
    )

    for item in history_items:
        if item.original_index < len(data):
            row = data[item.original_index]
            item.accession_id = row.get("AccessionID", "N/A")

    return render_template(
        "history.html",
        completed_items=history_items,
        messages=flash_messages(),
    )


@app.route("/update", methods=["POST"])
@login_required
def update():
    if not data:
        return redirect(url_for("index"))
    try:
        idx = int(request.form["original_index"])
        qi = (
            db.session.execute(db.select(QueueItem).filter_by(original_index=idx))
            .scalars()
            .first()
        )

        # Force complete even if lease expired/stolen if user hits "save"
        forced = False
        if not qi or qi.leased_by_id != current_user.id:
            if qi and qi.status != "completed":
                forced = True  # Allow save, take over lease implicitly
            else:
                flash("Cannot save: Item is completed or invalid.", "error")
                return redirect(url_for("index"))

        row = data[idx]

        # Update data
        new_acc = request.form.get("accession_id", "").strip()
        new_stain = request.form.get("stain", "").strip()
        new_block = request.form.get("block_number", "").strip()
        mark_complete = request.form.get("complete") == "on"

        changed = False
        acc_changed = row["AccessionID"] != new_acc

        if row["AccessionID"] != new_acc:
            row["AccessionID"] = new_acc
            changed = True
        if row["Stain"] != new_stain:
            row["Stain"] = new_stain
            changed = True
        if row["BlockNumber"] != new_block:
            row["BlockNumber"] = new_block
            changed = True

        # Completion logic
        valid_to_complete = bool(new_acc and new_stain)
        if mark_complete and not valid_to_complete:
            flash("Cannot mark complete: ID and Stain required.", "warning")
            mark_complete = False

        if row["_is_complete"] != mark_complete:
            row["_is_complete"] = mark_complete
            changed = True

        if changed:
            current_user.correction_count += 1
            if acc_changed:
                _recalculate_accession_counts()

            # Mark queue item completed if moving next
            if request.form.get("action") == "next" and mark_complete:
                qi.status = "completed"
                qi.completed_by = current_user
                qi.completed_at = datetime.datetime.utcnow()
            elif forced:
                # Re-establish lease if we forced a save on non-completed item
                qi.status = "leased"
                qi.leased_by = current_user
                qi.leased_at = datetime.datetime.utcnow()

            db.session.commit()

            # Save to disk
            try:
                _create_backup()
                save_csv_data()
                flash("Saved.", "success")
            except Exception as e:
                app.logger.error(f"Save failed: {e}")
                flash("Error saving to disk.", "error")

        return redirect(url_for("index"))

    except (ValueError, KeyError):
        return redirect(url_for("index"))


@app.route("/release", methods=["POST"])
@login_required
def release_lease():
    leases = (
        db.session.execute(
            db.select(QueueItem).filter_by(
                leased_by_id=current_user.id, status="leased"
            )
        )
        .scalars()
        .all()
    )
    if leases:
        for lease in leases:
            lease.status = "pending"
            lease.leased_by = None
            lease.leased_at = None
        db.session.commit()
        flash(f"Released {len(leases)} lease(s).", "info")
    return redirect(url_for("index"))


@app.route("/search", methods=["POST"])
@login_required
def search():
    if not data:
        return redirect(url_for("index"))
    term = request.form.get("search_term", "").strip().lower()
    if not term:
        return redirect(url_for("index"))

    for i, row in enumerate(data):
        # Search in ID, original filename stem, or block
        if (
            term in row.get("AccessionID", "").lower()
            or term in row.get("_identifier", "").lower()
            or term == row.get("BlockNumber", "").lower()
        ):
            return redirect(url_for("index", index=i))

    flash("Not found.", "warning")
    return redirect(url_for("index"))


# Updated image serving route to handle paths from CSV
@app.route("/data_images/<path:filepath>")
@login_required
def serve_relative_image(filepath: str):
    """Serves images based on relative paths found in the CSV."""
    # Security: ensure path doesn't escape IMAGE_BASE_DIR
    abs_image_dir = os.path.abspath(Config.IMAGE_BASE_DIR)
    abs_file_path = os.path.abspath(os.path.join(abs_image_dir, filepath))

    if not os.path.commonpath([abs_image_dir, abs_file_path]) == abs_image_dir:
        app.logger.warning(f"Security path traversal attempt: {filepath}")
        return "Invalid path", 403

    if not os.path.exists(abs_file_path):
        return "Image not found", 404

    directory, filename = os.path.split(abs_file_path)
    return send_from_directory(directory, filename)


def flash_messages() -> List[Dict[str, str]]:
    return [
        {"category": c, "message": m}
        for c, m in get_flashed_messages(with_categories=True)
    ]


# ==============================================================================
# 10. CLI COMMANDS
# ==============================================================================
@app.cli.command("init-db")
@with_appcontext
def init_db_command():
    """Initialize DB, create admin, populate queue from CSV."""
    print(f"Database path: {app.config['SQLALCHEMY_DATABASE_URI']}")
    db.create_all()

    if not db.session.get(User, "admin"):
        u = User(id="admin", is_admin=True)
        u.set_password(Config.ADMIN_DEFAULT_PASSWORD)
        db.session.add(u)
        print(f"Created 'admin' user with password: {Config.ADMIN_DEFAULT_PASSWORD}")

    # Populate queue based on 'ParsingQCPassed' column
    if os.path.exists(Config.CSV_FILE_PATH):
        try:
            load_csv_data()
            existing_items = db.session.execute(db.select(QueueItem)).scalars().all()
            existing = {i.original_index for i in existing_items}
            new_items = []
            for row in data:
                if row["_original_index"] not in existing:
                    # If ParsingQCPassed is truthy, it's completed. Otherwise pending.
                    status = "completed" if row["_is_complete"] else "pending"
                    new_items.append(
                        QueueItem(original_index=row["_original_index"], status=status)
                    )

            if new_items:
                db.session.bulk_save_objects(new_items)
                db.session.commit()
                print(f"Added {len(new_items)} items to queue.")
            else:
                print("Queue is up to date.")
        except Exception as e:
            print(f"Error populating queue: {e}")
    else:
        print(f"CSV not found at {Config.CSV_FILE_PATH}. Queue not populated.")

    db.session.commit()
    print("Initialization complete.")


if __name__ == "__main__":
    # Attempt initial load
    if os.path.exists(Config.CSV_FILE_PATH):
        try:
            load_csv_data()
        except Exception as e:
            print(f"Failed to load CSV on startup: {e}")

    app.run(debug=True, host="0.0.0.0")
