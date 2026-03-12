import argparse
import subprocess
import shutil, os
import webbrowser
import time
import platform
import sys, signal
from pathlib import Path

def parse_except(e: Exception):
    if e.returncode < 0:
        print(f"Terminated by signal: {e}")
    else:
        match e.returncode:
            case 1:
                print(f"ERR -- general:\n\t{e.stderr}")
            case 2:
                print(f"ERR -- misuse of shell built-ins:\n\t{e.stderr}")
            case 126:
                print(f"ERR -- cannot execute command:\n\t{e.stderr}")
            case 127:
                print(f"ERR -- command not found:\n\t{e.stderr}")
            case _:
                print(f"ERR -- fatal error signal:\n\t{e.stderr}")


# use different browser protocol depending on platform
def open_browser_wsl(url):
    # Check if we are actually in WSL
    if "microsoft-standard" in platform.uname().release.lower():
        # Use PowerShell to 'Start' the URL on the Windows side
        subprocess.run(["powershell.exe", "-Command", f"Start-Process '{url}'"])
    else:
        import webbrowser
        webbrowser.open(url)


def kill_existing_flask(port=5000):
    """Finds and kills any process currently using the specified port."""
    try:
        # Get the PID of the process using the port
        pid = subprocess.check_output(["lsof", "-ti", f":{port}"]).decode().strip()
        if pid:
            print(f"Stopping existing process on port {port} (PID: {pid})...")
            # Kill the process (and its children)
            subprocess.run(["kill", "-9", pid])
            time.sleep(1) # Give the OS a second to actually release the port
    except subprocess.CalledProcessError:
        # No process was using the port
        pass


# close app on SIGINT
qc_app = None
def signal_handler(sig, frame):
    """Kills the subprocess when the user hits Ctrl+C."""
    global qc_app
    print("\n\x1b[1mShutting down QC app...\x1b[0m")
    if qc_app:
        qc_app.terminate() # Polite request to stop
    sys.exit(0)

# Register the handler
signal.signal(signal.SIGINT, signal_handler)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
            description="Pipeline for the separate modules inside of Label-Check. Streamlines the process to QC and handles argument passing between modules."
    )
    parser.add_argument(
            "--input_dir", required=True, help="Input directory of images to be processed"
    )
    parser.add_argument(
            "--output_dir", required=True, help="Output directory to place macros, labels, and thumbnails and wherein to conduct QC"
    )
    parser.add_argument(
            "--start_from", required=False, help="Stage of the pipeline to start from (1/macro, 2/ocr, 3/name, app)", choices=['1', 'macro', '2', 'ocr', '3', 'name', 'app']
    )
    # create a config file for all of the other arguments, but these two must be provided at runtime
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    start_from = args.start_from
   
    if start_from in [None, '1', 'macro']:
        print("\x1b[1mExecuting 1_get_macro.py...\x1b[0m\n")
        try:
            subprocess.run(
                    [
                        "python", 
                        "src/1_get_macro.py", 
                        "--input_dir", 
                        input_dir, 
                        "--output_dir", 
                        output_dir
                    ], 
                    check=True, 
                    text=True
            )
        except Exception as e:
            parse_except(e)
            sys.exit(1)

    mapping_csv = output_dir / "slide_mapping.csv"
    ocr_csv = output_dir / "ocr.csv"

    if start_from in [None, '1', 'macro', '2', 'ocr']:
        print("\n\x1b[1mExecuting 2_run_dual_ocr.py...\x1b[0m\n")
        try:
            subprocess.run(
                    [
                        "python", 
                        "src/2_run_dual_ocr.py", 
                        "--mapping_csv", 
                        mapping_csv, 
                        "--output_csv", 
                        ocr_csv
                    ], 
                    check=True, 
                    text=True
            )
        except Exception as e:
            parse_except(e)
            sys.exit(1)

    enriched_csv = output_dir / "enriched.csv"

    if start_from != 'app':
        print("\n\x1b[1mExecuting 3_name-files.py...\x1b[0m\n")
        try:
            subprocess.run(
                    [
                        "python", 
                        "src/3_name-files.py", 
                        "--input_csv", 
                        ocr_csv, 
                        "--output_csv", 
                        enriched_csv
                    ], 
                    check=True, 
                    text=True
            )
        except Exception as e:
            parse_except(e)

    output_src = output_dir / "src"
    output_templates = output_src / "templates"
    os.makedirs(output_src, exist_ok=True)

    shutil.copy("src/app.py", output_src)
    shutil.copytree("src/templates/", output_templates, dirs_exist_ok=True)

    output_app = output_src / "app.py"

    print("\n\x1b[1mInitializing database...\x1b[0m\n")
    try:
        subprocess.run(
                [
                    "flask",
                    "--app",
                    output_app,
                    "init-db"
                ],
                check=True,
                text=True
        )
    except Exception as e:
        parse_except(e)
        sys.exit(1)

    current_src_path = os.path.abspath(output_src)

    # in case the app is still running in the background, kill it first
    kill_existing_flask() # default port 5000

    print("\n\x1b[1mOpening QC app...\x1b[0m\n")
    qc_app = subprocess.Popen(
        ["python", "-m", "flask", "run", "--host", "0.0.0.0"],
        cwd=output_src,  
        env={
            **os.environ, 
            "FLASK_APP": "app.py",
            "PYTHONPATH": current_src_path,
            "PYTHONDONTWRITEBYTECODE": "1"
        },
        stdin=subprocess.DEVNULL,
        stdout=None,
        stderr=None
    )
    time.sleep(2)
    open_browser_wsl("http://127.0.0.1:5000")

    # keep script alive to listen for SIGINT
    while True:
        time.sleep(1)
