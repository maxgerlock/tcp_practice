import asyncio
import itertools

from app.models.connection import Connection
from app.models.message import Message


class ConnectionManager:
    def __init__(self) -> None:
        self.connections: dict[int, Connection] = {}
        self.subscribers: set[Connection] = set()
        self.connection_ids = itertools.count(1)

    async def broadcast(self, message: Message) -> None:
        for subscriber in list(self.subscribers):
            try:
                subscriber.send_line(f"MESSAGE {message.message_id} {message.message}")
                subscriber.add_pending_ack(message.message_id)
            except asyncio.QueueFull:
                self.subscribers.discard(subscriber)  # and close it

    async def subscribe(self, connection: Connection) -> None:
        if connection.id not in self.connections:
            raise ValueError(f"Connection {connection.id} not found")
        self.subscribers.add(connection)

    async def unsubscribe(self, connection: Connection) -> None:
        if connection.id not in self.connections:
            raise ValueError(f"Connection {connection.id} not found")
        self.subscribers.discard(connection)

    def create_connection(
        self, address: str, writer: asyncio.StreamWriter
    ) -> Connection:
        conn = Connection(
            id=next(self.connection_ids),
            address=address,
            writer=writer,
            output_queue=asyncio.Queue(),
        )
        self.connections[conn.id] = conn
        return conn

    def remove_connection(self, connection: Connection) -> None:
        if connection.id not in self.connections:
            raise ValueError(f"Connection {connection.id} not found")
        self.subscribers.discard(connection)
        self.connections.pop(connection.id)
