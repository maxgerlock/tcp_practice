from collections.abc import Awaitable, Callable

from app.command_handler import CommandHandler
from app.errors import CommandError

COMMANDS_WITH_ARGUMENT: dict[str, Callable[[CommandHandler, str], Awaitable[None]]] = {
    "ECHO": CommandHandler.echo,
    "SEND": CommandHandler.send,
    "GET": CommandHandler.get,
    "ACK": CommandHandler.ack,
}
COMMANDS_WITHOUT_ARGUMENT: dict[str, Callable[[CommandHandler], Awaitable[None]]] = {
    "EXIT": CommandHandler.exit,
    "HISTORY": CommandHandler.history,
    "SUBSCRIBE": CommandHandler.subscribe,
    "UNSUBSCRIBE": CommandHandler.unsubscribe,
}


async def run_command(handler: CommandHandler, line: str) -> str:
    """Parse and execute one command line, returning the command name."""
    parts = line.split(maxsplit=1)
    if not parts:
        raise CommandError("empty command")
    command = parts[0]
    argument = parts[1].strip() if len(parts) > 1 else ""

    if command in COMMANDS_WITH_ARGUMENT:
        await COMMANDS_WITH_ARGUMENT[command](handler, argument)
    elif command in COMMANDS_WITHOUT_ARGUMENT:
        if argument:
            raise CommandError("invalid arguments")
        await COMMANDS_WITHOUT_ARGUMENT[command](handler)
    else:
        print(f"Unknown command: {command!r}")
        raise CommandError("unknown command")
    return command
