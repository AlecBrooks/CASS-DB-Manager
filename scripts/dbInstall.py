import os
import sqlite3

def read_db_config():
    config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'conf', 'db.conf'))
    config = {}
    with open(config_path, 'r') as file:
        for line in file:
            if '=' in line and not line.strip().startswith('#'):
                key, val = map(str.strip, line.strip().split('=', 1))
                config[key] = val
    return config

def test_sqlite_read_write(db_path):
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Create test table
        cursor.execute("CREATE TABLE IF NOT EXISTS _install_test (id INTEGER PRIMARY KEY, msg TEXT)")
        cursor.execute("INSERT INTO _install_test (msg) VALUES (?)", ("write test",))

        # Read test
        cursor.execute("SELECT msg FROM _install_test LIMIT 1")
        result = cursor.fetchone()

        # Clean up
        cursor.execute("DROP TABLE _install_test")
        conn.commit()
        conn.close()

        return result is not None and result[0] == "write test"
    except Exception as e:
        print(f"[!] Error testing SQLite database: {e}")
        return False

def create_sqlite_db_if_missing(db_path):
    if os.path.exists(db_path):
        print(f"[✔] SQLite database already exists at: {db_path}")
    else:
        print(f"[*] Creating new SQLite database at: {db_path}")
        try:
            os.makedirs(os.path.dirname(db_path), exist_ok=True)
            conn = sqlite3.connect(db_path)
            conn.execute("CREATE TABLE IF NOT EXISTS _init (id INTEGER);")  # just create something
            conn.commit()
            conn.close()
            print("[✔] SQLite database created.")
        except Exception as e:
            raise RuntimeError(f"[X] Failed to create database: {e}")

def main():
    print("[*] Reading database configuration...")
    config = read_db_config()

    if "dbPath" not in config:
        raise RuntimeError("[X] db.conf is missing the 'dbPath' entry")

    db_path = config["dbPath"]

    create_sqlite_db_if_missing(db_path)

    print("[*] Verifying read/write access to the SQLite database...")
    if test_sqlite_read_write(db_path):
        print("[✔] Database read/write test passed.")
    else:
        raise RuntimeError("[X] Database read/write test failed.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(str(e))
    finally:
        input("\n[*] Press ENTER to exit...")