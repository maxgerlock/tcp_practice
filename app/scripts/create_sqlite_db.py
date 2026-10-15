import sqlite3
import sys
from pathlib import Path

from app.repository.sqlite_schema import DEFAULT_DB_PATH, create_table_sql


def main() -> None:
    db_path = Path(sys.argv[1] if len(sys.argv) > 1 else DEFAULT_DB_PATH)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        conn.execute(create_table_sql())
    conn.close()
    print(f"Created schema in {db_path}")


if __name__ == "__main__":
    main()
