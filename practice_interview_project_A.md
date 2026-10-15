# Project A

## Phase 1: TCP Echo Server

Build a TCP server that listens on a configurable port.

Clients will connect using Netcat:

```sh
nc localhost 9000
```

The server accepts newline-delimited commands.

Initially, support one command:

```text
ECHO <message>
```

The server should respond:

```text
OK <message>
```

For example:

```text
> ECHO hello world
< OK hello world
```

Any unrecognized command should receive:

```text
ERROR unknown command
```

The connection should remain open so that a client can send multiple commands.

### Requirements

- Use Python.
- Multiple commands may be sent over one connection.
- Commands are delimited by newlines.
- Don't use an HTTP framework for the TCP protocol.
- Include automated tests for the behavior you consider important.

There are additional phases, but they're hidden.

## Phase 2: Shared Message History

Your Phase 1 server now needs to support shared state across multiple connected clients.

Add two commands:

```text
SEND <message>
HISTORY
```



### `SEND`

`SEND` stores a message in the server's in-memory history.

Each successfully stored message receives a unique, monotonically increasing integer ID.

Example:

```text
> SEND hello world
< OK 1

> SEND another message
< OK 2
```

IDs begin at `1` when the server starts.

Messages sent by any client are visible to every other client.

For example:

```text
Client A:
> SEND first
< OK 1

Client B:
> SEND second
< OK 2
```



### `HISTORY`

`HISTORY` returns all stored messages in ascending ID order, followed by `END`.

```text
> HISTORY
< 1 first
< 2 second
< END
```

If there are no messages:

```text
> HISTORY
< END
```



### Requirements

Your server must now support multiple clients connected at the same time.

The following must hold:

- `ECHO` from Phase 1 continues to work.
- Unknown commands continue to return `ERROR unknown command`.
- Message state is shared between all connected clients.
- Message IDs are unique and monotonically increasing.
- Messages returned by `HISTORY` appear in ID order.
- A client may issue multiple commands over one connection.
- One client disconnecting must not disrupt other clients.
- Persistence across server restarts is **not** required.
- Continue using the existing newline-delimited TCP protocol.
- Add automated tests for the new behavior while preserving the Phase 1 tests.

You may refactor your Phase 1 implementation as needed.

### Additional Requirement

Your implementation should behave correctly if two connected clients submit messages at approximately the same time.

You are free to choose how shared state and concurrency are handled.

**Phase 3 remains hidden.**

---



## Notes on Phase 2

This version of Phase 2 specifies the externally observable contract clearly, but leaves enough architectural freedom that there are decisions to make. It deliberately doesn't say things like:

> "Use an `asyncio.Lock`."

or:

> "Create a `MessageService` and repository."

Those are exactly the kinds of architectural decisions to recognize and reason about during the interview.

A few things are also left unstated that would be reasonable to ask the interviewer about, for example:

- Are empty `SEND` messages valid?
- Do IDs need to be contiguous, or merely monotonically increasing?

In an actual interview, noticing those ambiguities is part of the exercise.

## Project A — Phase 3: Persistent History + Message Lookup

Your server now needs to preserve messages across restarts.

Replace the purely in-memory message history with **durable persistence**.

You may choose the storage technology, but it must run locally without requiring external infrastructure.

For this phase, SQLite is a reasonable choice.

### New command: `GET`

Add support for:

```text
GET <message_id>
```

If the message exists:

```text
> GET 2
< 2 second
```

If it does not exist:

```text
> GET 999
< ERROR message not found
```

If the ID is malformed:

```text
> GET abc
< ERROR invalid message id
```



### Persistence requirements

Messages created with `SEND` must survive a server restart.

For example:

```text
Server run #1

> SEND hello
< OK 1

> SEND world
< OK 2
```

After stopping and restarting the server using the same data store:

```text
Server run #2

> HISTORY
< 1 hello
< 2 world
< END
```

The next message ID must continue from the existing history:

```text
> SEND again
< OK 3
```



### Concurrency requirements

All Phase 2 concurrency guarantees still apply.

In particular:

- multiple clients may issue `SEND` concurrently,
- message IDs must remain unique,
- message ordering must remain well-defined,
- reads and writes must not corrupt persisted state.



### Existing behavior

These commands must continue to behave as before:

```text
ECHO <message>
SEND <message>
HISTORY
```

Unknown commands must still return:

```text
ERROR unknown command
```



### Engineering requirement

Your persistence mechanism should be isolated from the command-processing logic so that changing storage implementations would not require rewriting the protocol layer.

You are free to choose the abstraction and project structure.

Add automated tests for the new behavior while preserving the earlier tests.

Project A — Phase 4: Real-Time Message Delivery
Clients should now be able to receive new messages in real time.
Add a new command:
SUBSCRIBE

After a client successfully subscribes:

> SUBSCRIBE
> < OK

whenever any client successfully sends a new message, each subscribed client should receive:
MESSAGE  

For example:
Client A:

> SUBSCRIBE
> < OK

Client B:

> SEND hello
> < OK 4

Client A:
< MESSAGE 4 hello

A subscribed client may continue issuing normal commands such as SEND, GET, HISTORY, and ECHO over the same connection.
Requirements

- Multiple clients may be subscribed simultaneously.
- A sender that is subscribed should also receive the MESSAGE notification for its own message.
- Each successfully stored message should generate exactly one notification per subscribed client.
- Notifications must use the same message IDs as the persisted messages.
- Messages received by a given subscriber must appear in increasing message-ID order.
- Disconnecting clients must be cleaned up correctly.
- A disconnected or failing client must not prevent other clients from sending or receiving messages.
- All behavior from Phases 1–3 must continue to work.
- Add automated tests for the new behavior.
You may refactor the implementation as needed.

Project A — Phase 5: Delivery Acknowledgements
Subscribed clients now need to acknowledge that they have received each real-time message notification.
Add a new command:
ACK 

A successful acknowledgement should return:

> ACK 4
> < OK

New delivery behavior
When a subscribed client receives:
MESSAGE 4 hello

the server should consider that message unacknowledged for that connection until the client sends:
ACK 4

If a client sends an acknowledgement for a message it has not received, return:
ERROR invalid acknowledgement

If the acknowledgement contains a malformed or invalid ID, return:
ERROR invalid message id

Reconnection behavior
If a subscribed client disconnects with unacknowledged messages, those acknowledgements do not persist across connections.
A new connection starts with no delivery or acknowledgement state.
Requirements

- Acknowledgement state is tracked independently for each subscribed connection.
- A message may be acknowledged at most once per connection.
- Repeated acknowledgement of the same message should return:
ERROR invalid acknowledgement
- Acknowledging one message must not affect any other message.
- Normal commands must continue to work while messages are awaiting acknowledgement.

```
A failing or disconnected client must not affect other clients.
```

- All behavior from Phases 1–4 must continue to work.
- Add automated tests for the new behavior.
You may refactor as necessary.

