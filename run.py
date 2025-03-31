#!/usr/bin/env python3

import os
import sys
import subprocess
import platform
import shutil

# Path to your main script (adjust as needed)
MAIN_SCRIPT = os.path.join("scripts", "main.py")

VENV_FOLDER = "venv"  # Virtual environment folder name
REQUIREMENTS_FILE = "requirements.txt"

def main():
    create_venv_if_needed()
    venv_python = get_venv_python()
    upgrade_pip(venv_python)
    install_requirements(venv_python)
    run_main_script(venv_python)

def create_venv_if_needed():
    if not os.path.exists(VENV_FOLDER):
        print(f"[INFO] Creating virtual environment: {VENV_FOLDER}")
        subprocess.check_call([sys.executable, "-m", "venv", VENV_FOLDER])
    else:
        print(f"[INFO] Found existing virtual environment: {VENV_FOLDER}")

def get_venv_python():
    """
    Determines the correct Python executable path in the virtual environment.
    If the venv folder exists but no valid Python executable is found,
    the venv will be recreated.
    """
    venv_dir = os.path.abspath(VENV_FOLDER)
    system_platform = platform.system().lower()

    if system_platform.startswith("win"):
        candidate = os.path.join(venv_dir, "Scripts", "python.exe")
    else:
        candidate = os.path.join(venv_dir, "bin", "python")
        if not os.path.exists(candidate):
            candidate = os.path.join(venv_dir, "bin", "python3")
    
    if os.path.exists(candidate):
        return candidate
    else:
        print(f"[INFO] Virtual environment exists but no valid Python found at {candidate}.")
        print("[INFO] Recreating the virtual environment...")
        shutil.rmtree(venv_dir)
        create_venv_if_needed()
        return get_venv_python()  # Recursively find the python path again

def upgrade_pip(venv_python):
    print("\n[INFO] Upgrading pip in the virtual environment...")
    subprocess.check_call([venv_python, "-m", "pip", "install", "--upgrade", "pip"])

def install_requirements(venv_python):
    if os.path.exists(REQUIREMENTS_FILE):
        print(f"\n[INFO] Installing dependencies from {REQUIREMENTS_FILE}...")
        subprocess.check_call([venv_python, "-m", "pip", "install", "-r", REQUIREMENTS_FILE])
    else:
        print(f"[WARNING] {REQUIREMENTS_FILE} not found. Skipping dependency installation.")

def run_main_script(venv_python):
    if not os.path.exists(MAIN_SCRIPT):
        print(f"[ERROR] {MAIN_SCRIPT} does not exist. Exiting.")
        sys.exit(1)
    print(f"\n[INFO] Running {MAIN_SCRIPT} inside the virtual environment...\n")
    subprocess.check_call([venv_python, MAIN_SCRIPT])

if __name__ == "__main__":
    main()
