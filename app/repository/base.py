from typing import Protocol

from app.models.message import Message


class MessageRepository(Protocol):
    async def create_message(self, content: str) -> Message: ...

    async def get_all_messages(self) -> list[Message]:
        """Return every stored message in ascending ID order."""
        ...

    async def get_message(self, message_id: int) -> Message | None: ...

    async def delete_message(self, message_id: int) -> None: ...
