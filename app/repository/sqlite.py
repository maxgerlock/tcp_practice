from typing import Self

import aiosqlite

from app.models.message import Message
from app.repository.sqlite_schema import (
    COLUMNS,
    PRIMARY_KEY,
    TABLE_NAME,
    message_from_row,
)

_SELECT = f"SELECT {', '.join(COLUMNS)} FROM {TABLE_NAME}"


class SqliteMessageRepository:
    def __init__(self, conn: aiosqlite.Connection):
        self.conn = conn

    @classmethod
    async def open(cls, db_path: str) -> Self:
        return cls(await aiosqlite.connect(db_path))

    async def close(self) -> None:
        await self.conn.close()

    async def create_message(self, content: str) -> Message:
        async with self.conn.execute(
            f"INSERT INTO {TABLE_NAME} (message) VALUES (?)", (content,)
        ) as cursor:
            message_id = cursor.lastrowid
        await self.conn.commit()
        if message_id is None:
            raise RuntimeError("INSERT did not produce a row ID")
        return Message(message_id=message_id, message=content)

    async def get_all_messages(self) -> list[Message]:
        rows = await self.conn.execute_fetchall(f"{_SELECT} ORDER BY {PRIMARY_KEY}")
        return [message_from_row(row) for row in rows]

    async def get_message(self, message_id: int) -> Message | None:
        async with self.conn.execute(
            f"{_SELECT} WHERE {PRIMARY_KEY} = ?", (message_id,)
        ) as cursor:
            row = await cursor.fetchone()
        return message_from_row(row) if row is not None else None

    async def delete_message(self, message_id: int) -> None:
        await self.conn.execute(
            f"DELETE FROM {TABLE_NAME} WHERE {PRIMARY_KEY} = ?", (message_id,)
        )
        await self.conn.commit()
