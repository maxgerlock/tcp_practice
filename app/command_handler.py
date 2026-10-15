from app.connection_manager import ConnectionManager
from app.errors import CommandError
from app.models.connection import Connection
from app.repository.base import MessageRepository


class CommandHandler:
    def __init__(
        self,
        message_repository: MessageRepository,
        connection_manager: ConnectionManager,
        connection: Connection,
    ):
        self.message_repository = message_repository
        self.connection_manager = connection_manager
        self.connection = connection

    async def echo(self, message: str) -> None:
        if not message:
            raise CommandError("message cannot be empty")
        self.connection.send_line(f"OK {message}")

    async def exit(self) -> None:
        self.connection.send_line("Goodbye!")

    async def send(self, message: str) -> None:
        if not message:
            raise CommandError("message cannot be empty")
        created_message = await self.message_repository.create_message(message)
        # TODO consider outbox pattern to tightly couple message creation and broadcasting
        # await self._write_line(f"OK {created.message_id}")
        self.connection.send_line(f"OK {created_message.message_id}")
        await self.connection_manager.broadcast(created_message)

    async def history(self) -> None:
        for message in await self.message_repository.get_all_messages():
            self.connection.send_line(f"{message.message_id} {message.message}")
        self.connection.send_line("END")

    async def get(self, message_id: str) -> None:
        if not message_id:
            raise CommandError("message_id cannot be empty")
        if not message_id.isdigit():
            raise CommandError("invalid message id")

        message = await self.message_repository.get_message(int(message_id))
        if message:
            self.connection.send_line(f"{message.message_id} {message.message}")
        else:
            raise CommandError("message not found")

    async def subscribe(self) -> None:
        await self.connection_manager.subscribe(self.connection)
        self.connection.send_line("OK")

    async def unsubscribe(self) -> None:
        await self.connection_manager.unsubscribe(self.connection)
        self.connection.send_line("OK")

    async def ack(self, message_id: str) -> None:
        if not message_id:
            raise CommandError("message_id cannot be empty")
        if not message_id.isdigit():
            raise CommandError("invalid message id")

        if not self.connection.has_pending_ack(int(message_id)):
            raise CommandError("message not found")

        self.connection.remove_pending_ack(int(message_id))
        self.connection.send_line("OK")
