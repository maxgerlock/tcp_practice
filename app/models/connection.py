import asyncio


class Connection:
    def __init__(
        self,
        id: int,
        address: str,
        writer: asyncio.StreamWriter,
        output_queue: asyncio.Queue[str | None],
    ):
        self.id: int = id
        self.address: str = address
        self.writer: asyncio.StreamWriter = writer
        self.output_queue: asyncio.Queue[str | None] = output_queue
        self.pending_acks = set[int]()

    def send_line(self, line: str) -> None:
        self.output_queue.put_nowait(line)

    def close(self) -> None:
        """Stop the writer once every line queued before this call has been sent."""
        self.output_queue.put_nowait(None)

    async def run_writer(self) -> None:
        try:
            while (line := await self.output_queue.get()) is not None:
                self.writer.write(f"{line}\n".encode())
                await self.writer.drain()
        except ConnectionError:
            pass

    def add_pending_ack(self, message_id: int) -> None:
        self.pending_acks.add(message_id)

    def remove_pending_ack(self, message_id: int) -> None:
        self.pending_acks.discard(message_id)

    def has_pending_ack(self, message_id: int) -> bool:
        return message_id in self.pending_acks

    def get_pending_acks(self) -> set[int]:
        return self.pending_acks
