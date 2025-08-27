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

# Folder structure to ensure
REQUIRED_FOLDERS = [
    "data/audits",
    "data/CassOutput",
    "data/rawData/AE33",
    "data/rawData/TCA",
    "data/SQLite",
]

def main():
    create_launcher_if_needed()
    ensure_data_folders_exist()
    create_venv_if_needed()
    venv_python = get_venv_python()
    upgrade_pip(venv_python)
    install_requirements(venv_python)
    run_main_script(venv_python)

def create_launcher_if_needed():
    script_path = os.path.abspath(__file__)
    base_dir = os.path.dirname(script_path)
    python_exec = sys.executable
    system_platform = platform.system().lower()

    if system_platform.startswith("win"):
        launcher_name = "DB CASS Manager.bat"
        launcher_path = os.path.join(base_dir, launcher_name)
        if not os.path.exists(launcher_path):
            with open(launcher_path, "w") as f:
                f.write(f'@echo off\n')
                f.write(f'cd /d "{base_dir}"\n')
                f.write(f'"{python_exec}" "{script_path}"\n')
                f.write("pause\n")
            print(f"[INFO] Created Windows launcher: {launcher_path}")

    elif system_platform == "darwin":
        launcher_name = "DB_CASS_Manager.command"
        launcher_path = os.path.join(base_dir, launcher_name)
        if not os.path.exists(launcher_path):
            with open(launcher_path, "w") as f:
                f.write("#!/bin/bash\n")
                f.write(f'cd "{base_dir}"\n')
                f.write(f'"{python_exec}" "{script_path}"\n')
                f.write('read -p "Press Enter to exit..."')
            os.chmod(launcher_path, 0o755)
            print(f"[INFO] Created macOS .command launcher: {launcher_path}")

    elif system_platform == "linux":
        launcher_name = "DB_CASS_Manager.desktop"
        launcher_path = os.path.join(base_dir, launcher_name)
        if not os.path.exists(launcher_path):
            with open(launcher_path, "w") as f:
                f.write("[Desktop Entry]\n")
                f.write("Type=Application\n")
                f.write(f"Name=DB CASS Manager\n")
                f.write(f"Exec=gnome-terminal -- bash -c 'cd \"{base_dir}\" && \"{python_exec}\" \"{script_path}\"; read -p \"Press Enter to exit...\"'\n")
                f.write("Terminal=true\n")
            os.chmod(launcher_path, 0o755)
            print(f"[INFO] Created Linux .desktop launcher: {launcher_path}")

def ensure_data_folders_exist():
    for folder in REQUIRED_FOLDERS:
        if not os.path.exists(folder):
            os.makedirs(folder)
            print(f"[INFO] Created missing folder: {folder}")
        else:
            print(f"[INFO] Folder already exists: {folder}")

def create_venv_if_needed():
    if not os.path.exists(VENV_FOLDER):
        print(f"[INFO] Creating virtual environment: {VENV_FOLDER}")
        subprocess.check_call([sys.executable, "-m", "venv", VENV_FOLDER])
    else:
        print(f"[INFO] Found existing virtual environment: {VENV_FOLDER}")

def get_venv_python():
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
        return get_venv_python()

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