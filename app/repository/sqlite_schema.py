"""SQLite schema for messages, derived from the `Message` dataclass."""

from collections.abc import Sequence
from dataclasses import fields
from typing import Any

from app.models.message import Message

DEFAULT_DB_PATH = "db/app.db"
TABLE_NAME = "messages"

_SQL_TYPES: dict[type, str] = {int: "INTEGER", str: "TEXT"}

COLUMNS: tuple[str, ...] = tuple(f.name for f in fields(Message))
PRIMARY_KEY: str = next(f.name for f in fields(Message) if f.metadata.get("primary_key"))


def _column_definition(name: str, python_type: Any) -> str:
    sql_type = _SQL_TYPES.get(python_type)
    if sql_type is None:
        raise TypeError(f"No SQLite type mapping for Message.{name}: {python_type!r}")
    if name == PRIMARY_KEY:
        return f"{name} {sql_type} PRIMARY KEY"
    return f"{name} {sql_type} NOT NULL"


def create_table_sql() -> str:
    columns = ",\n    ".join(_column_definition(f.name, f.type) for f in fields(Message))
    return f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} (\n    {columns}\n)"


def message_from_row(row: Sequence[Any]) -> Message:
    return Message(**dict(zip(COLUMNS, row, strict=True)))
