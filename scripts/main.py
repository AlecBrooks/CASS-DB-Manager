#!/usr/bin/env python3

import os
import sys
import subprocess
import platform
import runpy
import sqlite3
import pandas as pd
from colorama import init, Fore, Style
init(autoreset=True)

def open_folder(path):
    if sys.platform.startswith("win32"):
        os.startfile(path)
    elif sys.platform.startswith("darwin"):
        subprocess.run(["open", path])
    elif sys.platform.startswith("linux"):
        subprocess.run(["xdg-open", path])
    else:
        raise OSError("Unsupported operating system")

def check_db_connection():
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        conf_dir = os.path.join(script_dir, '..', 'conf')
        db_conf_path = os.path.join(conf_dir, 'db.conf')

        db_config = {}
        with open(db_conf_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                if '#' in line:
                    line = line.split('#', 1)[0].strip()
                if '=' in line:
                    key, value = line.split('=', 1)
                    db_config[key.strip()] = value.strip()

        # Ensure dbPath is explicitly defined
        if 'dbPath' not in db_config:
            print(Fore.RED + "Error: 'dbPath' not found in db.conf." + Style.RESET_ALL)
            return False

        db_path = db_config['dbPath']

        # Check if file exists
        if not os.path.isfile(db_path):
            print(Fore.RED + f"Error: Database file not found at path: {db_path}" + Style.RESET_ALL)
            return False

        # Attempt to open the database
        conn = sqlite3.connect(db_path)
        conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        conn.close()
        return True

    except Exception as e:
        print(Fore.RED + f"Database connection error: {e}" + Style.RESET_ALL)
        return False

os.chdir(os.path.dirname(os.path.abspath(__file__)))

CONNECTED_MENU = "Main"
PUSH_MENU = "Upload"
AUDIT_MENU = "Audit"
ANALYSIS_MENU = "Analysis"
CONFIG_MENU = "Configuration"

menu_state = CONNECTED_MENU
exitFile = False

def open_file_with_default_editor(file_path):
    file_path = os.path.abspath(file_path)
    if platform.system() == "Darwin":
        subprocess.run(["open", file_path], check=True)
    elif platform.system() == "Linux":
        subprocess.run(["xdg-open", file_path], check=True)
    elif platform.system() == "Windows":
        subprocess.run(["start", file_path], check=True, shell=True)
    else:
        print("Unsupported OS")

def clear_screen():
    command = "cls" if os.name == "nt" else "clear"
    os.system(command)

def print_header():
    header = f"""
==========================================================================================
   _____           _____ _____   _____  ____    __  __                                   
  / ____|   /\\    / ____/ ____| |  __ \\|  _ \\  |  \\/  |                                  
 | |       /  \\  | (___| (___   | |  | | |_) | | \\  / | __ _ _ __   __ _  __ _  ___ _ __ 
 | |      / /\\ \\  \\___ \\\\___ \\  | |  | |  _ <  | |\\/| |/ _` | '_ \\ / _` |/ _` |/ _ \\ '__|
 | |____ / ____ \\ ____) |___) | | |__| | |_) | | |  | | (_| | | | | (_| | (_| |  __/ |   
  \\_____/_/    \\_\\_____/_____/  |_____/|____/  |_|  |_|\\__,_|_| |_|\\__,_|\\__, |\\___|_|   
                                                                          __/ |          
                                                                         |___/    
Menu: {menu_state}
                                                           CASS Database Manager - UNLV
==========================================================================================
    """
    print(Fore.CYAN + header + Style.RESET_ALL)

def print_footer():
    footer = """
==========================================================================================
    """
    print(Fore.CYAN + footer + Style.RESET_ALL)

def get_menu_choice(options):
    if platform.system() == "Windows":
        for idx, option in enumerate(options, start=1):
            print(Fore.YELLOW + f"{idx}. {option}" + Style.RESET_ALL)
        while True:
            try:
                choice = int(input(Fore.YELLOW + "Select an option by number: " + Style.RESET_ALL))
                if 1 <= choice <= len(options):
                    return choice - 1
                else:
                    print(Fore.RED + "Invalid option. Try again." + Style.RESET_ALL)
            except ValueError:
                print(Fore.RED + "Invalid input. Please enter a number." + Style.RESET_ALL)
    else:
        from simple_term_menu import TerminalMenu
        terminal_menu = TerminalMenu(options)
        return terminal_menu.show()

while not exitFile:
    clear_screen()
    print_header()

    if menu_state == CONNECTED_MENU:
        options = [" - Analysis", " - Upload Data", " - Audit", " - Configuration", " - Exit"]
    elif menu_state == PUSH_MENU:
        options = [" - AE33", " - TCA", " - Both", " - Data Folder", " - Back"]
    elif menu_state == AUDIT_MENU:
        options = [" - AE33 Time Gaps", " - TCA Time Gaps", " - Audits Folder", " - Back"]
    elif menu_state == ANALYSIS_MENU:
        options = [" - Run Analysis", " - Update Constants", " - Folder", " - Back"]
    elif menu_state == CONFIG_MENU:
        options = [" - Test DB Connection", " - Data Config", " - DB Config", " - DB Install", " - Back"]

    menu_entry_index = get_menu_choice(options)
    print_footer()

    if menu_state == CONNECTED_MENU:
        if menu_entry_index == 0:
            menu_state = ANALYSIS_MENU
        elif menu_entry_index == 1:
            menu_state = PUSH_MENU
        elif menu_entry_index == 2:
            menu_state = AUDIT_MENU
        elif menu_entry_index == 3:
            menu_state = CONFIG_MENU
        elif menu_entry_index == 4:
            exitFile = True

    elif menu_state == PUSH_MENU:
        if menu_entry_index == 0:
            subprocess.run([sys.executable, "dbPush.py", "ae33"])
            input("Please press Enter to continue.")
        elif menu_entry_index == 1:
            subprocess.run([sys.executable, "dbPush.py", "tca"])
            input("Please press Enter to continue.")
        elif menu_entry_index == 2:
            subprocess.run([sys.executable, "dbPush.py", "ae33"])
            subprocess.run([sys.executable, "dbPush.py", "tca"])
            input("Please press Enter to continue.")
        elif menu_entry_index == 3:
            open_folder(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "rawData")))
        elif menu_entry_index == 4:
            menu_state = CONNECTED_MENU

    elif menu_state == ANALYSIS_MENU:
        if menu_entry_index == 0:
            subprocess.run([sys.executable, "CassSpeciation.py"])
        elif menu_entry_index == 1:
            open_file_with_default_editor(os.path.join(os.path.dirname(__file__), "..", "conf", "constants.conf"))
        elif menu_entry_index == 2:
            open_folder(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "CASSOutput")))
        elif menu_entry_index == 3:
            menu_state = CONNECTED_MENU

    elif menu_state == AUDIT_MENU:
        if menu_entry_index == 0:  # AE33 Time Gaps
            script_dir = os.path.dirname(os.path.abspath(__file__))
            audit_script = os.path.join(script_dir, "audit.py")
            subprocess.run([sys.executable, audit_script, "ae33"])
        elif menu_entry_index == 1:  # TCA Time Gaps
            script_dir = os.path.dirname(os.path.abspath(__file__))
            audit_script = os.path.join(script_dir, "audit.py")
            subprocess.run([sys.executable, audit_script, "tca"])
        elif menu_entry_index == 2:  # Audits Folder
            open_folder(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "audits")))
        elif menu_entry_index == 3:  # Back
            menu_state = CONNECTED_MENU

    elif menu_state == CONFIG_MENU:
        if menu_entry_index == 0:
            result = check_db_connection()
            if result:
                print(Fore.GREEN + "Connection successful!" + Style.RESET_ALL)
            else:
                print(Fore.RED + "Connection failed. Press Enter to continue." + Style.RESET_ALL)
            input("Please press Enter.")
        elif menu_entry_index == 1:
            open_file_with_default_editor(os.path.join(os.path.dirname(__file__), "..", "conf", "data.conf"))
        elif menu_entry_index == 2:
            open_file_with_default_editor(os.path.join(os.path.dirname(__file__), "..", "conf", "db.conf"))
        elif menu_entry_index == 3:
            subprocess.run([sys.executable, "dbInstall.py"])
        elif menu_entry_index == 4:
            menu_state = CONNECTED_MENU