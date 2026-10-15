import asyncio
import functools
import traceback

from app.command_handler import CommandHandler
from app.connection_manager import ConnectionManager
from app.errors import CommandError
from app.protocol import run_command
from app.repository.base import MessageRepository
from app.repository.sqlite import SqliteMessageRepository
from app.repository.sqlite_schema import DEFAULT_DB_PATH

DEFAULT_PORT = 9000


async def handle_tcp(
    repository: MessageRepository,
    connection_manager: ConnectionManager,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
): 
    addr = writer.get_extra_info("peername")
    connection = connection_manager.create_connection(addr, writer)
    writer_task = asyncio.create_task(connection.run_writer())

    print(f"Connection from {addr}")
    handler = CommandHandler(
        message_repository=repository,
        connection_manager=connection_manager,
        connection=connection,
    )
    try:
        while True:
            data = await reader.readline()
            if not data:
                break
            try:
                command = await run_command(handler, data.decode())
            except CommandError as e:
                connection.send_line(f"ERROR {e}")
                continue

            if command == "EXIT":
                break
    except Exception as e:
        traceback.print_exc()
    finally:
        connection_manager.remove_connection(connection)
        connection.close()
        try:
            await asyncio.wait_for(writer_task, timeout=5)
        except TimeoutError:
            pass  # wait_for cancels a writer stuck on a client that stopped reading
        writer.close()
        await writer.wait_closed()
        print("Connection closed")


async def serve(port: int = DEFAULT_PORT, db_path: str = DEFAULT_DB_PATH) -> None:
    repository = await SqliteMessageRepository.open(db_path)
    connection_manager = ConnectionManager()
    try:
        server = await asyncio.start_server(
            functools.partial(handle_tcp, repository, connection_manager),
            "127.0.0.1",
            port,
        )

        addrs = ", ".join(str(sock.getsockname()) for sock in server.sockets)
        print(f"Serving on {addrs} using {db_path}")

        async with server:
            await server.serve_forever()
    finally:
        await repository.close()
