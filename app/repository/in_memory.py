from app.models.message import Message


class InMemoryMessageRepository:
    def __init__(self):
        self.messages: dict[int, Message] = {}
        self.next_message_id = 1

    # These methods must not await: with no suspension points, each one runs
    # atomically on the event loop, which keeps ID assignment race-free.
    async def create_message(self, content: str) -> Message:
        message = Message(message_id=self.next_message_id, message=content)
        self.messages[message.message_id] = message
        self.next_message_id += 1
        return message

    async def get_all_messages(self) -> list[Message]:
        return list(self.messages.values())

    async def get_message(self, message_id: int) -> Message | None:
        return self.messages.get(message_id)

    async def delete_message(self, message_id: int) -> None:
        self.messages.pop(message_id, None)
