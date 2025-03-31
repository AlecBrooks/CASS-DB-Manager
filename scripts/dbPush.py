#!/usr/bin/env python3
import argparse
import os
import re
import csv
import time
import logging
import sqlite3
from datetime import datetime
from tqdm import tqdm

def setup_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    if logger.hasHandlers():
        logger.handlers.clear()
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

def read_config(path):
    config = {}
    with open(path, 'r') as file:
        for line in file:
            if '=' in line and not line.strip().startswith('#'):
                key, value = line.strip().split("=", 1)
                config[key.strip()] = value.strip()
    return config

def extract_headers_ae33(file_path):
    with open(file_path, 'r') as file:
        for line in file:
            if line.startswith("Date"):
                headers = re.split(r'\s+|;', line.strip())
                headers = [h.strip() for h in headers if h.strip()]
                return ["datetime"] + [
                    "date" if h == "Date(yyyy/MM/dd)" else "time" if h == "Time(hh:mm:ss)" else h
                    for h in headers
                ]
    raise ValueError("No valid header found")

def create_ae33_table(cursor, headers, table):
    columns = ['"datetime" TEXT PRIMARY KEY']
    for h in headers[1:]:
        if h == "date" or h == "time":
            columns.append(f'"{h}" TEXT')
        else:
            columns.append(f'"{h}" REAL')
    cursor.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({", ".join(columns)});')

def row_exists(cursor, table, key_field, value):
    cursor.execute(f'SELECT COUNT(*) FROM "{table}" WHERE "{key_field}" = ?', (value,))
    return cursor.fetchone()[0] > 0

def process_file_ae33(path, cursor, headers, table, logger):
    with open(path, 'r') as file:
        lines = file.readlines()
    start = next(i for i, line in enumerate(lines) if line.startswith("Date"))
    rows = lines[start + 1:]
    added = 0
    for line in rows:
        row = re.split(r'\s+', line.strip())
        row = row[:len(headers) - 1]
        try:
            date_str = datetime.strptime(row[0], "%Y/%m/%d").strftime("%Y-%m-%d")
            time_str = datetime.strptime(row[1], "%H:%M:%S").strftime("%H:%M:%S")
            dt = f"{date_str} {time_str}"
            values = [dt, date_str, time_str] + row[2:]
            if len(values) != len(headers):
                continue
        except Exception:
            continue
        if row_exists(cursor, table, "datetime", dt):
            continue
        placeholders = ','.join(['?'] * len(headers))
        quoted_headers = [f'"{h}"' for h in headers]
        sql = f'INSERT INTO "{table}" ({", ".join(quoted_headers)}) VALUES ({placeholders})'
        try:
            cursor.execute(sql, values)
            added += 1
        except Exception as e:
            logger.error(f"Insert failed: {e}")
    return added

def extract_headers_tca(path):
    with open(path, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        headers = next(reader)
        return headers

def create_tca_table(cursor, table, headers):
    cols = ['"ID" INTEGER PRIMARY KEY']
    for h in headers[1:]:
        if "Time" in h:
            cols.append(f'"{h}" TEXT')
        elif h in ["TCcounts", "TCmass", "TCconc", "AE33_BC6", "AE33_b", "OC", "EC", "CO2", "Volume"]:
            cols.append(f'"{h}" REAL')
        else:
            cols.append(f'"{h}" TEXT')
    cols.append('"date" TEXT')
    cursor.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({", ".join(cols)});')

def process_file_tca(path, cursor, table, logger):
    with open(path, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        headers = next(reader)
        if "ID" not in headers:
            logger.error(f"Missing 'ID' column in {path}")
            return 0
        try:
            start_index = headers.index("StartTimeLocal")
        except ValueError:
            start_index = None
        create_tca_table(cursor, table, headers)
        added = 0
        for row in reader:
            if not row or len(row) != len(headers):
                continue
            try:
                record_id = int(row[0])
            except ValueError:
                continue
            if row_exists(cursor, table, "ID", record_id):
                continue
            row_data = []
            for i, val in enumerate(row):
                if "Time" in headers[i]:
                    try:
                        row_data.append(datetime.strptime(val, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S"))
                    except:
                        row_data.append(None)
                else:
                    row_data.append(val)
            date_val = row_data[start_index].split()[0] if start_index is not None and row_data[start_index] else None
            row_data.append(date_val)
            quoted_headers = [f'"{h}"' for h in headers]
            sql = f'INSERT INTO "{table}" ({", ".join(quoted_headers + ["date"])}) VALUES ({",".join(["?"] * (len(headers)+1))})'
            try:
                cursor.execute(sql, row_data)
                added += 1
            except Exception as e:
                logger.error(f"Insert failed: {e}")
        return added

def run_ae33():
    logger = setup_logger("AE33")
    CONF = read_config(os.path.join(os.path.dirname(__file__), "../conf/db.conf"))
    DATA = read_config(os.path.join(os.path.dirname(__file__), "../conf/data.conf"))
    db_path = CONF["dbPath"]
    table = CONF["AE33_Table"]
    data_path = DATA["AE33_data_Location"]
    prefix = DATA["AE33_FilePrefix"]

    if not os.path.exists(db_path):
        logger.error(f"Database file not found: {db_path}")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    files = []
    for root, _, fnames in os.walk(data_path):
        for f in fnames:
            if f.startswith(prefix):
                files.append(os.path.join(root, f))
    if not files:
        logger.info("No AE33 files found.")
        return
    headers = extract_headers_ae33(files[0])
    create_ae33_table(cursor, headers, table)
    total = 0
    with tqdm(total=len(files), desc="AE33", unit="file") as pbar:
        for f in files:
            added = process_file_ae33(f, cursor, headers, table, logger)
            conn.commit()
            total += added
            pbar.update(1)
    logger.info(f"Total AE33 rows added: {total}")
    conn.close()

def run_tca():
    logger = setup_logger("TCA")
    CONF = read_config(os.path.join(os.path.dirname(__file__), "../conf/db.conf"))
    DATA = read_config(os.path.join(os.path.dirname(__file__), "../conf/data.conf"))
    db_path = CONF["dbPath"]
    table = CONF["TCA_Table"]
    data_path = DATA["TCA_data_Location"]
    prefix = DATA["TCA_FilePrefix"]

    if not os.path.exists(db_path):
        logger.error(f"Database file not found: {db_path}")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    files = [
        os.path.join(data_path, f)
        for f in os.listdir(data_path)
        if f.startswith(prefix) and f.endswith(".csv")
    ]
    if not files:
        logger.info("No TCA files found.")
        return
    total = 0
    with tqdm(total=len(files), desc="TCA", unit="file") as pbar:
        for f in files:
            added = process_file_tca(f, cursor, table, logger)
            conn.commit()
            total += added
            pbar.update(1)
    logger.info(f"Total TCA rows added: {total}")
    conn.close()

def main():
    parser = argparse.ArgumentParser(description="Upload AE33 or TCA data to SQLite.")
    parser.add_argument("script", choices=["ae33", "tca"], help="Which script to run")
    args = parser.parse_args()
    if args.script == "ae33":
        run_ae33()
    elif args.script == "tca":
        run_tca()

if __name__ == "__main__":
    main()