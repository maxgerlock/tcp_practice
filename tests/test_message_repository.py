"""Storage-isolation tests.

The same contract runs against every `MessageRepository` implementation, and
`MessageService` (the protocol layer) runs unchanged on top of each one.
"""

import asyncio
import dataclasses
import os
import sqlite3
import tempfile
import unittest
from typing import cast

from app.connection_manager import ConnectionManager
from app.errors import CommandError
from app.message_service import MessageService
from app.models.connection import Connection
from app.models.message import Message
from app.repository.base import MessageRepository
from app.repository.in_memory import InMemoryMessageRepository
from app.repository.sqlite import SqliteMessageRepository
from app.repository.sqlite_schema import create_table_sql


class RepositoryFactory:
    """Mixin: subclasses build the repository under test."""

    async def make_repository(self) -> MessageRepository:
        raise NotImplementedError


class InMemoryFactory(RepositoryFactory):
    async def make_repository(self) -> MessageRepository:
        return InMemoryMessageRepository()


class SqliteFactory(RepositoryFactory, unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        db_dir = tempfile.TemporaryDirectory()
        self.addCleanup(db_dir.cleanup)
        self.db_path = os.path.join(db_dir.name, "test.db")
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(create_table_sql())
        conn.close()

    async def make_repository(self) -> MessageRepository:
        repo = await SqliteMessageRepository.open(self.db_path)
        self.addAsyncCleanup(repo.close)
        return repo


class RepositoryContract(RepositoryFactory, unittest.IsolatedAsyncioTestCase):
    async def test_create_assigns_increasing_ids_from_one(self):
        repo = await self.make_repository()
        first = await repo.create_message("first")
        second = await repo.create_message("second")
        self.assertEqual(first, Message(message_id=1, message="first"))
        self.assertEqual(second, Message(message_id=2, message="second"))

    async def test_get_message(self):
        repo = await self.make_repository()
        created = await repo.create_message("hello there")
        self.assertEqual(await repo.get_message(created.message_id), created)

    async def test_get_missing_message_returns_none(self):
        repo = await self.make_repository()
        await repo.create_message("only")
        self.assertIsNone(await repo.get_message(999))

    async def test_get_all_messages_in_id_order(self):
        repo = await self.make_repository()
        self.assertEqual(await repo.get_all_messages(), [])
        created = [await repo.create_message(f"m{i}") for i in range(5)]
        self.assertEqual(await repo.get_all_messages(), created)

    async def test_delete_message(self):
        repo = await self.make_repository()
        first = await repo.create_message("first")
        second = await repo.create_message("second")
        await repo.delete_message(first.message_id)
        await repo.delete_message(999)
        self.assertIsNone(await repo.get_message(first.message_id))
        self.assertEqual(await repo.get_all_messages(), [second])

    async def test_concurrent_creates_get_unique_ids(self):
        repo = await self.make_repository()
        created = await asyncio.gather(
            *(repo.create_message(f"m{i}") for i in range(50))
        )
        ids = [m.message_id for m in created]
        self.assertEqual(len(set(ids)), len(ids))
        stored = await repo.get_all_messages()
        self.assertEqual([m.message_id for m in stored], sorted(ids))
        self.assertEqual(
            {m.message_id: m.message for m in stored},
            {m.message_id: m.message for m in created},
        )

    async def test_returned_messages_are_immutable(self):
        repo = await self.make_repository()
        created = await repo.create_message("original")
        with self.assertRaises(dataclasses.FrozenInstanceError):
            created.message = "changed"  # type: ignore[misc]
        self.assertEqual((await repo.get_all_messages())[0].message, "original")


class InMemoryRepositoryTests(InMemoryFactory, RepositoryContract):
    pass


class SqliteRepositoryTests(SqliteFactory, RepositoryContract):
    async def test_messages_survive_reopen(self):
        repo = await self.make_repository()
        await repo.create_message("hello")
        await repo.create_message("world")

        reopened = await self.make_repository()
        self.assertEqual(
            await reopened.get_all_messages(),
            [
                Message(message_id=1, message="hello"),
                Message(message_id=2, message="world"),
            ],
        )
        self.assertEqual((await reopened.create_message("again")).message_id, 3)


def take_lines(connection: Connection) -> list[str]:
    """Drain and return every line the service queued for this connection."""
    lines: list[str] = []
    while not connection.output_queue.empty():
        line = connection.output_queue.get_nowait()
        if line is not None:
            lines.append(line)
    return lines


class ServiceContract(RepositoryFactory, unittest.IsolatedAsyncioTestCase):
    async def make_service(self) -> tuple[MessageService, Connection]:
        repo = await self.make_repository()
        manager = ConnectionManager()
        # The writer is never touched: nothing runs the connection's writer task,
        # so replies stay in its output queue for the test to inspect.
        connection = manager.create_connection("test", cast(asyncio.StreamWriter, None))
        return MessageService(repo, manager, connection), connection

    async def test_send_and_history(self):
        service, connection = await self.make_service()
        await service.send("first")
        await service.send("second")
        await service.history()
        self.assertEqual(
            take_lines(connection), ["OK 1", "OK 2", "1 first", "2 second", "END"]
        )

    async def test_get(self):
        service, connection = await self.make_service()
        await service.send("first")
        await service.send("second")
        take_lines(connection)
        await service.get("2")
        self.assertEqual(take_lines(connection), ["2 second"])

    async def test_get_missing(self):
        service, _ = await self.make_service()
        with self.assertRaisesRegex(CommandError, "^message not found$"):
            await service.get("999")

    async def test_get_malformed(self):
        service, _ = await self.make_service()
        for message_id in ["abc", "-1", "1.5"]:
            with (
                self.subTest(message_id=message_id),
                self.assertRaisesRegex(CommandError, "^invalid message id$"),
            ):
                await service.get(message_id)


class InMemoryServiceTests(InMemoryFactory, ServiceContract):
    pass


class SqliteServiceTests(SqliteFactory, ServiceContract):
    pass


del RepositoryContract, ServiceContract

if __name__ == "__main__":
    unittest.main()
