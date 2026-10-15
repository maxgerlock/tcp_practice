from dataclasses import dataclass, field


@dataclass(frozen=True)
class Message:
    message_id: int = field(metadata={"primary_key": True})
    message: str
