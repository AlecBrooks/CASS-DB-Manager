#!/usr/bin/env python3
import os
import sys
import sqlite3
import pandas as pd
from tabulate import tabulate
from datetime import datetime

def parse_config_file(filepath):
    config = {}
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '#' in line:
                line = line.split('#', 1)[0].strip()
            if '=' in line:
                key, value = line.split('=', 1)
                config[key.strip()] = value.strip()
    return config

def detect_gaps(timestamps: pd.Series, threshold_minutes: float):
    gap_start = []
    gap_end = []
    gap_minutes = []

    for prev, curr in zip(timestamps[:-1], timestamps[1:]):
        delta = (curr - prev).total_seconds() / 60
        if delta > threshold_minutes:
            gap_start.append(prev)
            gap_end.append(curr)
            gap_minutes.append(round(delta, 2))

    return pd.DataFrame({
        "gap_start": gap_start,
        "gap_end": gap_end,
        "gap_minutes": gap_minutes
    })

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)

    if len(sys.argv) >= 2:
        audit_target = sys.argv[1].strip().lower()
    else:
        audit_target = input("Which audit do you want to run? ('ae33' or 'tca'): ").strip().lower()

    if audit_target not in {"ae33", "tca"}:
        print("Invalid choice. Use 'ae33' or 'tca'.")
        sys.exit(1)

    conf_path = os.path.join(script_dir, "..", "conf", "db.conf")
    db_conf = parse_config_file(conf_path)
    db_path = db_conf.get("dbPath")
    table_name = db_conf.get("AE33_Table") if audit_target == "ae33" else db_conf.get("TCA_Table")
    time_column = "datetime" if audit_target == "ae33" else "StartTimeLocal"

    if not db_path or not os.path.exists(db_path):
        print(f"SQLite database not found at: {db_path}")
        if sys.stdin.isatty():
            input("Press Enter to continue...")
        sys.exit(1)

    try:
        conn = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True)
        query = f'SELECT "{time_column}" FROM "{table_name}" ORDER BY "{time_column}"'
        timestamps_df = pd.read_sql_query(query, conn, parse_dates=[time_column])
        conn.close()
    except Exception as e:
        print(f"Error loading timestamps: {e}")
        if sys.stdin.isatty():
            input("Press Enter to continue...")
        sys.exit(1)

    if timestamps_df.empty:
        print("No data found.")
        if sys.stdin.isatty():
            input("Press Enter to continue...")
        sys.exit(0)

    timestamps = timestamps_df[time_column].dropna().sort_values().reset_index(drop=True)
    diffs = timestamps.diff().dropna()
    mode_gap = diffs.mode()[0]
    threshold_minutes = mode_gap.total_seconds() / 60

    print(f"\nMost common interval in {audit_target.upper()} data: {round(threshold_minutes, 2)} minutes")
    print("Detecting gaps greater than that...\n")

    gaps_df = detect_gaps(timestamps, threshold_minutes)

    if not gaps_df.empty:
        print(tabulate(gaps_df, headers="keys", tablefmt="grid", showindex=False))
    else:
        print("[No gaps found]")
    print("-" * 40)

    while True:
        answer = input("Generate CSV report of the gaps? (yes/no): ").strip().lower()
        if answer == "yes":
            audits_dir = os.path.join(script_dir, "..", "data", "audits")
            os.makedirs(audits_dir, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_name = f"gaps_{audit_target}_{timestamp}.csv"
            report_path = os.path.join(audits_dir, report_name)
            gaps_df.to_csv(report_path, index=False)
            print(f"Report saved to {report_path}")
            break
        elif answer == "no":
            break
        else:
            print("Please type 'yes' or 'no'.")

    if sys.stdin.isatty():
        input("Press Enter to exit.")

if __name__ == "__main__":
    main()