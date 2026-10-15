import asyncio
import sys

from app.repository.sqlite_schema import DEFAULT_DB_PATH
from app.server import DEFAULT_PORT, serve


def main() -> None:
    port = int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_PORT
    db_path = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_DB_PATH
    asyncio.run(serve(port=port, db_path=db_path))


if __name__ == "__main__":
    main()
