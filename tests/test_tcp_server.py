import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from typing import BinaryIO

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HOST = "127.0.0.1"

EMPTY_COMMAND = "ERROR empty command"
UNKNOWN_COMMAND = "ERROR unknown command"
INVALID_ARGUMENTS = "ERROR invalid arguments"
EMPTY_MESSAGE = "ERROR message cannot be empty"
MESSAGE_NOT_FOUND = "ERROR message not found"
INVALID_MESSAGE_ID = "ERROR invalid message id"
INVALID_ACK = "ERROR invalid acknowledgement"


def _free_port():
    with socket.socket() as s:
        s.bind((HOST, 0))
        return s.getsockname()[1]


def _start_server(db_path: str) -> tuple[subprocess.Popen[bytes], int]:
    """Start a server on `db_path`, creating the schema if it doesn't exist yet."""
    subprocess.run(
        [sys.executable, "-m", "app.scripts.create_sqlite_db", db_path],
        cwd=ROOT,
        check=True,
        stdout=subprocess.DEVNULL,
    )
    port = _free_port()
    env = dict(os.environ, PORT=str(port))
    proc = subprocess.Popen(
        [sys.executable, "-m", "app", str(port), db_path],
        cwd=ROOT,
        env=env,
    )
    deadline = time.time() + 5
    while time.time() < deadline:
        try:
            socket.create_connection((HOST, port), timeout=0.2).close()
            return proc, port
        except OSError:
            time.sleep(0.1)
    proc.kill()
    raise RuntimeError("server did not start")


def _stop_server(proc: subprocess.Popen[bytes]) -> None:
    if proc.poll() is None:
        proc.terminate()
    proc.wait(timeout=5)


def _temp_db_path(test: unittest.TestCase | type[unittest.TestCase]) -> str:
    db_dir = tempfile.TemporaryDirectory()
    if isinstance(test, unittest.TestCase):
        test.addCleanup(db_dir.cleanup)
    else:
        test.addClassCleanup(db_dir.cleanup)
    return os.path.join(db_dir.name, "test.db")


def _concurrent_sends(port: int, clients: int, per_client: int) -> dict[str, int]:
    """Have `clients` connections SEND at the same moment; map each message to its ID."""
    barrier = threading.Barrier(clients)
    returned: dict[str, int] = {}
    errors: list[BaseException] = []
    lock = threading.Lock()

    def worker(client: int) -> None:
        try:
            with socket.create_connection((HOST, port), timeout=5) as sock:
                reader = sock.makefile("rb")
                barrier.wait()
                for i in range(per_client):
                    message = f"client{client}-msg{i}"
                    sock.sendall(f"SEND {message}\n".encode())
                    response = reader.readline().decode().strip()
                    status, _, message_id = response.partition(" ")
                    if status != "OK":
                        raise AssertionError(f"unexpected response: {response!r}")
                    with lock:
                        returned[message] = int(message_id)
        except BaseException as e:
            with lock:
                errors.append(e)

    threads = [threading.Thread(target=worker, args=(c,)) for c in range(clients)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)
    if errors:
        raise errors[0]
    return returned


class TcpTestCase(unittest.TestCase):
    port: int

    def connect(self):
        sock = socket.create_connection((HOST, self.port), timeout=2)
        self.addCleanup(sock.close)
        return sock, sock.makefile("rb")

    def send_recv(self, sock: socket.socket, reader: BinaryIO, line: bytes) -> str:
        sock.sendall(line)
        return reader.readline().decode().rstrip("\r\n")

    def read_line(self, reader: BinaryIO) -> str:
        line = reader.readline()
        if not line:
            self.fail("connection closed while waiting for a line")
        return line.decode().rstrip("\r\n")

    def parse_notification(self, line: str) -> tuple[int, str]:
        kind, _, rest = line.partition(" ")
        self.assertEqual(kind, "MESSAGE", line)
        message_id, _, message = rest.partition(" ")
        self.assertTrue(message_id.isdigit(), line)
        return int(message_id), message

    def read_history(self, reader: BinaryIO) -> list[str]:
        lines = []
        while True:
            line = reader.readline().decode().rstrip("\r\n")
            if line == "END":
                return lines
            if not line:
                self.fail("connection closed before END")
            lines.append(line)

    def history(self, sock: socket.socket, reader: BinaryIO) -> list[tuple[int, str]]:
        sock.sendall(b"HISTORY\n")
        entries = []
        for line in self.read_history(reader):
            message_id, _, message = line.partition(" ")
            entries.append((int(message_id), message))
        return entries

    def send_message(self, sock: socket.socket, reader: BinaryIO, message: str) -> int:
        response = self.send_recv(sock, reader, f"SEND {message}\n".encode())
        status, _, message_id = response.partition(" ")
        self.assertEqual(status, "OK", response)
        return int(message_id)

    def assertResponses(
        self, sock: socket.socket, reader: BinaryIO, lines: list[bytes], expected: str
    ) -> None:
        for line in lines:
            with self.subTest(line=line):
                self.assertEqual(self.send_recv(sock, reader, line), expected)


class TcpServerTests(TcpTestCase):
    proc: subprocess.Popen[bytes]

    @classmethod
    def setUpClass(cls):
        cls.proc, cls.port = _start_server(_temp_db_path(cls))
        cls.addClassCleanup(_stop_server, cls.proc)

    def test_echo(self):
        sock, r = self.connect()
        self.assertEqual(
            self.send_recv(sock, r, b"ECHO hello world\n"), "OK hello world"
        )

    def test_echo_strips_surrounding_whitespace(self):
        sock, r = self.connect()
        self.assertEqual(
            self.send_recv(sock, r, b"ECHO    hello world    \n"), "OK hello world"
        )

    def test_echo_without_message_is_error(self):
        sock, r = self.connect()
        self.assertResponses(
            sock, r, [b"ECHO\n", b"ECHO \n", b"ECHO     \n"], EMPTY_MESSAGE
        )

    def test_unknown_command(self):
        sock, r = self.connect()
        self.assertEqual(self.send_recv(sock, r, b"FOO bar\n"), UNKNOWN_COMMAND)

    def test_empty_line_is_empty_command(self):
        sock, r = self.connect()
        self.assertResponses(sock, r, [b"\n", b"   \n", b"\t \n"], EMPTY_COMMAND)

    def test_exit_with_arguments_is_invalid(self):
        sock, r = self.connect()
        self.assertEqual(self.send_recv(sock, r, b"EXIT now\n"), INVALID_ARGUMENTS)
        self.assertEqual(self.send_recv(sock, r, b"ECHO still open\n"), "OK still open")

    def test_exit_closes_connection(self):
        sock, r = self.connect()
        self.assertEqual(self.send_recv(sock, r, b"EXIT\n"), "Goodbye!")
        self.assertEqual(r.readline(), b"")

    def test_multiple_commands_same_connection(self):
        sock, r = self.connect()
        self.assertEqual(self.send_recv(sock, r, b"ECHO one\n"), "OK one")
        self.assertEqual(self.send_recv(sock, r, b"BAD\n"), UNKNOWN_COMMAND)
        self.assertEqual(self.send_recv(sock, r, b"ECHO two\n"), "OK two")

    def test_pipelined_commands(self):
        sock, r = self.connect()
        sock.sendall(b"ECHO a\nECHO b\nNOPE\n")
        self.assertEqual(r.readline().decode().strip(), "OK a")
        self.assertEqual(r.readline().decode().strip(), "OK b")
        self.assertEqual(r.readline().decode().strip(), UNKNOWN_COMMAND)

    def test_fragmented_command(self):
        sock, r = self.connect()
        sock.sendall(b"ECHO hel")
        time.sleep(0.2)
        sock.sendall(b"lo\n")
        self.assertEqual(r.readline().decode().strip(), "OK hello")

    def test_multiple_clients(self):
        s1, r1 = self.connect()
        s2, r2 = self.connect()
        self.assertEqual(self.send_recv(s1, r1, b"ECHO one\n"), "OK one")
        self.assertEqual(self.send_recv(s2, r2, b"ECHO two\n"), "OK two")
        self.assertEqual(self.send_recv(s1, r1, b"ECHO again\n"), "OK again")


class FreshServerTestCase(TcpTestCase):
    """Each test gets a fresh server and database, so IDs and history start empty."""

    def setUp(self):
        proc, self.port = _start_server(_temp_db_path(self))
        self.addCleanup(_stop_server, proc)


class MessageHistoryTests(FreshServerTestCase):
    def test_history_empty(self):
        sock, r = self.connect()
        self.assertEqual(self.send_recv(sock, r, b"HISTORY\n"), "END")

    def test_send_returns_ids_starting_at_one(self):
        sock, r = self.connect()
        self.assertEqual(self.send_recv(sock, r, b"SEND hello world\n"), "OK 1")
        self.assertEqual(self.send_recv(sock, r, b"SEND another message\n"), "OK 2")

    def test_send_ids_increase(self):
        sock, r = self.connect()
        ids = [self.send_message(sock, r, f"message {i}") for i in range(10)]
        self.assertEqual(ids, sorted(set(ids)))

    def test_history_returns_messages_in_id_order(self):
        sock, r = self.connect()
        self.send_message(sock, r, "first")
        self.send_message(sock, r, "second")
        self.send_message(sock, r, "third")
        sock.sendall(b"HISTORY\n")
        self.assertEqual(self.read_history(r), ["1 first", "2 second", "3 third"])

    def test_history_preserves_multi_word_messages(self):
        sock, r = self.connect()
        message_id = self.send_message(sock, r, "hello there world")
        self.assertEqual(self.history(sock, r), [(message_id, "hello there world")])

    def test_send_strips_surrounding_whitespace(self):
        sock, r = self.connect()
        self.assertEqual(self.send_recv(sock, r, b"SEND    hello world    \n"), "OK 1")
        self.assertEqual(self.history(sock, r), [(1, "hello world")])

    def test_send_without_message_is_error(self):
        sock, r = self.connect()
        self.assertResponses(
            sock, r, [b"SEND\n", b"SEND \n", b"SEND     \n"], EMPTY_MESSAGE
        )
        self.assertEqual(self.history(sock, r), [])

    def test_rejected_send_does_not_consume_id(self):
        sock, r = self.connect()
        self.assertEqual(self.send_recv(sock, r, b"SEND\n"), EMPTY_MESSAGE)
        self.assertEqual(self.send_recv(sock, r, b"SEND real\n"), "OK 1")

    def test_history_with_arguments_is_invalid(self):
        sock, r = self.connect()
        self.send_message(sock, r, "stored")
        self.assertResponses(
            sock, r, [b"HISTORY xyz\n", b"HISTORY foo bar\n"], INVALID_ARGUMENTS
        )
        self.assertEqual(self.history(sock, r), [(1, "stored")])

    def test_history_with_trailing_whitespace_is_valid(self):
        sock, r = self.connect()
        self.assertEqual(self.send_recv(sock, r, b"HISTORY   \n"), "END")

    def test_history_shared_between_clients(self):
        s1, r1 = self.connect()
        s2, r2 = self.connect()
        self.assertEqual(self.send_recv(s1, r1, b"SEND first\n"), "OK 1")
        self.assertEqual(self.send_recv(s2, r2, b"SEND second\n"), "OK 2")
        expected = [(1, "first"), (2, "second")]
        self.assertEqual(self.history(s1, r1), expected)
        self.assertEqual(self.history(s2, r2), expected)

    def test_history_visible_to_client_connecting_later(self):
        s1, r1 = self.connect()
        self.send_message(s1, r1, "early bird")
        s2, r2 = self.connect()
        self.assertEqual(self.history(s2, r2), [(1, "early bird")])

    def test_echo_and_unknown_still_work_alongside_messages(self):
        sock, r = self.connect()
        self.assertEqual(self.send_recv(sock, r, b"SEND stored\n"), "OK 1")
        self.assertEqual(self.send_recv(sock, r, b"ECHO hi\n"), "OK hi")
        self.assertEqual(self.send_recv(sock, r, b"NOPE\n"), UNKNOWN_COMMAND)
        self.assertEqual(self.history(sock, r), [(1, "stored")])

    def test_pipelined_send_and_history(self):
        sock, r = self.connect()
        sock.sendall(b"SEND a\nSEND b\nHISTORY\nECHO done\n")
        self.assertEqual(r.readline().decode().strip(), "OK 1")
        self.assertEqual(r.readline().decode().strip(), "OK 2")
        self.assertEqual(self.read_history(r), ["1 a", "2 b"])
        self.assertEqual(r.readline().decode().strip(), "OK done")

    def test_client_disconnect_does_not_disrupt_others(self):
        s1, r1 = self.connect()
        s2, r2 = self.connect()
        self.send_message(s1, r1, "from one")
        s1.close()
        self.assertEqual(self.send_recv(s2, r2, b"SEND from two\n"), "OK 2")
        self.assertEqual(self.history(s2, r2), [(1, "from one"), (2, "from two")])

    def test_disconnect_mid_command_does_not_disrupt_others(self):
        s1, _ = self.connect()
        s2, r2 = self.connect()
        s1.sendall(b"SEND half a comm")
        s1.close()
        self.assertEqual(self.send_recv(s2, r2, b"SEND whole\n"), "OK 1")
        self.assertEqual(self.history(s2, r2), [(1, "whole")])

    def test_concurrent_sends(self):
        clients = 10
        per_client = 20
        returned = _concurrent_sends(self.port, clients, per_client)

        self.assertEqual(len(returned), clients * per_client)
        ids = list(returned.values())
        self.assertEqual(len(set(ids)), len(ids), "message IDs must be unique")

        sock, r = self.connect()
        history = self.history(sock, r)
        history_ids = [message_id for message_id, _ in history]
        self.assertEqual(history_ids, sorted(history_ids))
        self.assertEqual({m: i for i, m in history}, returned)


class MessageLookupTests(FreshServerTestCase):
    def test_get_existing_message(self):
        sock, r = self.connect()
        self.send_message(sock, r, "first")
        self.send_message(sock, r, "second")
        self.assertEqual(self.send_recv(sock, r, b"GET 2\n"), "2 second")
        self.assertEqual(self.send_recv(sock, r, b"GET 1\n"), "1 first")

    def test_get_preserves_multi_word_message(self):
        sock, r = self.connect()
        message_id = self.send_message(sock, r, "hello there world")
        self.assertEqual(
            self.send_recv(sock, r, f"GET {message_id}\n".encode()),
            f"{message_id} hello there world",
        )

    def test_get_ignores_surrounding_whitespace(self):
        sock, r = self.connect()
        self.send_message(sock, r, "padded")
        self.assertEqual(self.send_recv(sock, r, b"GET    1    \n"), "1 padded")

    def test_get_message_from_another_client(self):
        s1, r1 = self.connect()
        s2, r2 = self.connect()
        self.send_message(s1, r1, "shared")
        self.assertEqual(self.send_recv(s2, r2, b"GET 1\n"), "1 shared")

    def test_get_missing_message(self):
        sock, r = self.connect()
        self.send_message(sock, r, "only one")
        self.assertResponses(
            sock, r, [b"GET 999\n", b"GET 2\n", b"GET 0\n"], MESSAGE_NOT_FOUND
        )

    def test_get_on_empty_history(self):
        sock, r = self.connect()
        self.assertEqual(self.send_recv(sock, r, b"GET 1\n"), MESSAGE_NOT_FOUND)

    def test_get_malformed_id(self):
        sock, r = self.connect()
        self.send_message(sock, r, "exists")
        self.assertResponses(
            sock,
            r,
            [b"GET abc\n", b"GET -1\n", b"GET 1.5\n", b"GET 1abc\n", b"GET 0x1\n"],
            INVALID_MESSAGE_ID,
        )

    def test_get_with_extra_arguments_is_rejected(self):
        sock, r = self.connect()
        self.send_message(sock, r, "exists")
        self.assertIn(
            self.send_recv(sock, r, b"GET 1 2\n"),
            {INVALID_MESSAGE_ID, INVALID_ARGUMENTS},
        )

    def test_get_without_id_is_error(self):
        sock, r = self.connect()
        for line in [b"GET\n", b"GET    \n"]:
            with self.subTest(line=line):
                self.assertTrue(self.send_recv(sock, r, line).startswith("ERROR "))
        self.assertEqual(self.send_recv(sock, r, b"ECHO still open\n"), "OK still open")

    def test_get_does_not_modify_history(self):
        sock, r = self.connect()
        self.send_message(sock, r, "first")
        self.send_recv(sock, r, b"GET 1\n")
        self.send_recv(sock, r, b"GET 999\n")
        self.send_recv(sock, r, b"GET abc\n")
        self.assertEqual(self.history(sock, r), [(1, "first")])
        self.assertEqual(self.send_recv(sock, r, b"SEND second\n"), "OK 2")


class PersistenceTests(TcpTestCase):
    """Tests that restart the server against the same database file."""

    def setUp(self):
        self.db_path = _temp_db_path(self)

    def start(self) -> subprocess.Popen[bytes]:
        proc, self.port = _start_server(self.db_path)
        self.addCleanup(_stop_server, proc)
        return proc

    def restart(self, proc: subprocess.Popen[bytes]) -> subprocess.Popen[bytes]:
        _stop_server(proc)
        return self.start()

    def test_history_survives_restart(self):
        proc = self.start()
        sock, r = self.connect()
        self.assertEqual(self.send_recv(sock, r, b"SEND hello\n"), "OK 1")
        self.assertEqual(self.send_recv(sock, r, b"SEND world\n"), "OK 2")
        sock.close()

        self.restart(proc)
        sock, r = self.connect()
        self.assertEqual(self.history(sock, r), [(1, "hello"), (2, "world")])

    def test_ids_continue_after_restart(self):
        proc = self.start()
        sock, r = self.connect()
        self.send_message(sock, r, "hello")
        self.send_message(sock, r, "world")
        sock.close()

        self.restart(proc)
        sock, r = self.connect()
        self.assertEqual(self.send_recv(sock, r, b"SEND again\n"), "OK 3")
        self.assertEqual(
            self.history(sock, r), [(1, "hello"), (2, "world"), (3, "again")]
        )

    def test_get_after_restart(self):
        proc = self.start()
        sock, r = self.connect()
        self.send_message(sock, r, "first")
        self.send_message(sock, r, "second")
        sock.close()

        self.restart(proc)
        sock, r = self.connect()
        self.assertEqual(self.send_recv(sock, r, b"GET 2\n"), "2 second")
        self.assertEqual(self.send_recv(sock, r, b"GET 3\n"), MESSAGE_NOT_FOUND)

    def test_acknowledged_send_survives_abrupt_kill(self):
        proc = self.start()
        sock, r = self.connect()
        self.assertEqual(self.send_recv(sock, r, b"SEND durable\n"), "OK 1")
        proc.kill()
        proc.wait(timeout=5)
        sock.close()

        self.start()
        sock, r = self.connect()
        self.assertEqual(self.history(sock, r), [(1, "durable")])

    def test_survives_multiple_restarts(self):
        proc = self.start()
        for i in range(1, 4):
            sock, r = self.connect()
            self.assertEqual(
                self.send_recv(sock, r, f"SEND run {i}\n".encode()), f"OK {i}"
            )
            sock.close()
            proc = self.restart(proc)
        sock, r = self.connect()
        self.assertEqual(
            self.history(sock, r), [(1, "run 1"), (2, "run 2"), (3, "run 3")]
        )

    def test_concurrent_sends_survive_restart(self):
        proc = self.start()
        returned = _concurrent_sends(self.port, clients=10, per_client=10)
        sock, r = self.connect()
        before = self.history(sock, r)
        sock.close()

        self.restart(proc)
        sock, r = self.connect()
        after = self.history(sock, r)
        self.assertEqual(after, before)
        self.assertEqual({m: i for i, m in after}, returned)
        self.assertEqual(self.send_message(sock, r, "next"), max(returned.values()) + 1)

    def test_concurrent_reads_and_writes_stay_consistent(self):
        self.start()
        stop = threading.Event()
        snapshots: list[list[tuple[int, str]]] = []
        errors: list[BaseException] = []

        def reader() -> None:
            try:
                with socket.create_connection((HOST, self.port), timeout=5) as s:
                    f = s.makefile("rb")
                    while not stop.is_set():
                        s.sendall(b"HISTORY\n")
                        snapshot = []
                        while (line := f.readline().decode().rstrip("\r\n")) != "END":
                            if not line:
                                raise AssertionError("connection closed before END")
                            message_id, _, message = line.partition(" ")
                            snapshot.append((int(message_id), message))
                        snapshots.append(snapshot)
            except BaseException as e:
                errors.append(e)

        readers = [threading.Thread(target=reader) for _ in range(3)]
        for t in readers:
            t.start()
        try:
            returned = _concurrent_sends(self.port, clients=5, per_client=20)
        finally:
            stop.set()
            for t in readers:
                t.join(timeout=10)

        self.assertEqual(errors, [])
        self.assertTrue(snapshots, "readers never completed a HISTORY")
        by_id = {i: m for m, i in returned.items()}
        for snapshot in snapshots:
            ids = [i for i, _ in snapshot]
            self.assertEqual(
                ids, sorted(set(ids)), "HISTORY must be in strictly increasing ID order"
            )
            for message_id, message in snapshot:
                self.assertEqual(
                    by_id.get(message_id), message, "HISTORY returned a corrupted entry"
                )


class SubscriptionTests(FreshServerTestCase):
    def subscribe(self) -> tuple[socket.socket, BinaryIO]:
        sock, reader = self.connect()
        self.assertEqual(self.send_recv(sock, reader, b"SUBSCRIBE\n"), "OK")
        return sock, reader

    def test_subscribe_acknowledged(self):
        sock, r = self.connect()
        self.assertResponses(sock, r, [b"SUBSCRIBE\n", b"SUBSCRIBE   \n"], "OK")

    def test_subscribe_with_arguments_is_invalid(self):
        sock, r = self.connect()
        self.assertEqual(self.send_recv(sock, r, b"SUBSCRIBE now\n"), INVALID_ARGUMENTS)
        self.assertEqual(self.send_recv(sock, r, b"ECHO still open\n"), "OK still open")

    def test_unsubscribed_client_receives_nothing(self):
        bystander, br = self.connect()
        sender, sr = self.connect()
        self.assertEqual(self.send_recv(sender, sr, b"SEND hello\n"), "OK 1")
        self.assertEqual(self.send_recv(bystander, br, b"ECHO ping\n"), "OK ping")

    def test_subscriber_receives_other_clients_message(self):
        _, subscriber = self.subscribe()
        sender, sr = self.connect()
        self.assertEqual(self.send_recv(sender, sr, b"SEND hello\n"), "OK 1")
        self.assertEqual(self.read_line(subscriber), "MESSAGE 1 hello")

    def test_notification_preserves_multi_word_message(self):
        _, subscriber = self.subscribe()
        sender, sr = self.connect()
        self.assertEqual(
            self.send_recv(sender, sr, b"SEND hello there world\n"), "OK 1"
        )
        self.assertEqual(self.read_line(subscriber), "MESSAGE 1 hello there world")

    def test_multiple_subscribers_each_get_one_notification(self):
        subscribers = [self.subscribe()[1] for _ in range(3)]
        sender, sr = self.connect()
        self.assertEqual(self.send_recv(sender, sr, b"SEND shared\n"), "OK 1")
        for subscriber in subscribers:
            self.assertEqual(self.read_line(subscriber), "MESSAGE 1 shared")

    def test_subscribed_sender_is_notified_of_its_own_message(self):
        sock, r = self.subscribe()
        sock.sendall(b"SEND hello\n")
        lines = {self.read_line(r), self.read_line(r)}
        self.assertEqual(lines, {"OK 1", "MESSAGE 1 hello"})

    def test_subscribe_is_idempotent(self):
        sock, r = self.connect()
        for _ in range(3):
            self.assertEqual(self.send_recv(sock, r, b"SUBSCRIBE\n"), "OK")
        sender, sr = self.connect()
        self.assertEqual(self.send_recv(sender, sr, b"SEND once\n"), "OK 1")
        self.assertEqual(self.read_line(r), "MESSAGE 1 once")
        self.assertEqual(self.send_recv(sock, r, b"UNSUBSCRIBE\n"), "OK")
        self.assertEqual(self.send_recv(sender, sr, b"SEND after\n"), "OK 2")
        self.assertEqual(self.send_recv(sock, r, b"ECHO ping\n"), "OK ping")

    def test_subscribing_twice_still_notifies_once(self):
        sock, r = self.subscribe()
        self.assertEqual(self.send_recv(sock, r, b"SUBSCRIBE\n"), "OK")
        sender, sr = self.connect()
        self.assertEqual(self.send_recv(sender, sr, b"SEND once\n"), "OK 1")
        self.assertEqual(self.read_line(r), "MESSAGE 1 once")
        self.assertEqual(self.send_recv(sock, r, b"ECHO ping\n"), "OK ping")

    def test_subscribe_does_not_replay_existing_history(self):
        sender, sr = self.connect()
        self.send_message(sender, sr, "already stored")
        _, subscriber = self.subscribe()
        sender.sendall(b"SEND fresh\n")
        self.assertEqual(self.read_line(subscriber), "MESSAGE 2 fresh")

    def test_notification_ids_match_persisted_messages(self):
        _, subscriber = self.subscribe()
        sender, sr = self.connect()
        self.assertEqual(self.send_recv(sender, sr, b"SEND hello world\n"), "OK 1")
        message_id, message = self.parse_notification(self.read_line(subscriber))
        self.assertEqual((message_id, message), (1, "hello world"))
        self.assertEqual(self.send_recv(sender, sr, b"GET 1\n"), "1 hello world")
        self.assertEqual(self.history(sender, sr), [(1, "hello world")])

    def test_notifications_arrive_in_increasing_id_order(self):
        _, subscriber = self.subscribe()
        sender, sr = self.connect()
        for word in ("one", "two", "three"):
            self.send_message(sender, sr, word)
        notes = [self.parse_notification(self.read_line(subscriber)) for _ in range(3)]
        self.assertEqual(notes, [(1, "one"), (2, "two"), (3, "three")])

    def test_concurrent_notifications_arrive_in_increasing_id_order(self):
        _, subscriber = self.subscribe()
        returned = _concurrent_sends(self.port, clients=5, per_client=10)
        notes = [
            self.parse_notification(self.read_line(subscriber))
            for _ in range(len(returned))
        ]
        ids = [message_id for message_id, _ in notes]
        self.assertEqual(ids, sorted(ids))
        self.assertEqual(len(set(ids)), len(ids))
        self.assertEqual(
            {message: message_id for message_id, message in notes}, returned
        )

    def test_subscribed_client_can_keep_using_commands(self):
        sock, r = self.subscribe()
        sock.sendall(b"SEND stored\n")
        self.assertEqual(
            {self.read_line(r), self.read_line(r)}, {"OK 1", "MESSAGE 1 stored"}
        )
        self.assertEqual(self.send_recv(sock, r, b"ECHO hi\n"), "OK hi")
        self.assertEqual(self.send_recv(sock, r, b"GET 1\n"), "1 stored")
        sock.sendall(b"HISTORY\n")
        self.assertEqual(self.read_history(r), ["1 stored"])
        self.assertEqual(self.send_recv(sock, r, b"NOPE\n"), UNKNOWN_COMMAND)

    def test_unsubscribe_stops_notifications(self):
        sock, r = self.subscribe()
        self.assertEqual(self.send_recv(sock, r, b"UNSUBSCRIBE\n"), "OK")
        sender, sr = self.connect()
        self.assertEqual(self.send_recv(sender, sr, b"SEND later\n"), "OK 1")
        self.assertEqual(self.send_recv(sock, r, b"ECHO ping\n"), "OK ping")

    def test_unsubscribe_with_arguments_is_invalid(self):
        sock, r = self.subscribe()
        self.assertEqual(
            self.send_recv(sock, r, b"UNSUBSCRIBE now\n"), INVALID_ARGUMENTS
        )
        sender, sr = self.connect()
        self.assertEqual(self.send_recv(sender, sr, b"SEND still\n"), "OK 1")
        self.assertEqual(self.read_line(r), "MESSAGE 1 still")

    def test_rejected_send_notifies_nobody(self):
        _, subscriber = self.subscribe()
        sender, sr = self.connect()
        self.assertEqual(self.send_recv(sender, sr, b"SEND\n"), EMPTY_MESSAGE)
        self.assertEqual(self.send_recv(sender, sr, b"SEND real\n"), "OK 1")
        self.assertEqual(self.read_line(subscriber), "MESSAGE 1 real")

    def test_disconnected_subscriber_does_not_block_anyone(self):
        gone, _ = self.subscribe()
        _, survivor = self.subscribe()
        gone.close()
        time.sleep(0.2)
        sender, sr = self.connect()
        sender.settimeout(2)
        self.assertEqual(self.send_recv(sender, sr, b"SEND after\n"), "OK 1")
        self.assertEqual(self.read_line(survivor), "MESSAGE 1 after")

    def test_subscriber_that_stops_reading_does_not_block_anyone(self):
        stuck = socket.create_connection((HOST, self.port), timeout=2)
        self.addCleanup(stuck.close)
        stuck.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
        stuck.sendall(b"SUBSCRIBE\n")
        time.sleep(0.2)
        _, survivor = self.subscribe()
        sender, sr = self.connect()
        sender.settimeout(5)
        count = 100
        sender.sendall(b"".join(f"SEND m{i}\n".encode() for i in range(count)))
        for i in range(count):
            self.assertEqual(self.read_line(sr), f"OK {i + 1}")
        notes = [
            self.parse_notification(self.read_line(survivor)) for _ in range(count)
        ]
        self.assertEqual(notes, [(i + 1, f"m{i}") for i in range(count)])


class SubscriptionPersistenceTests(TcpTestCase):
    """Notifications must use the IDs of messages that survive a restart."""

    def setUp(self):
        self.db_path = _temp_db_path(self)

    def start(self) -> subprocess.Popen[bytes]:
        proc, self.port = _start_server(self.db_path)
        self.addCleanup(_stop_server, proc)
        return proc

    def test_notification_ids_continue_after_restart(self):
        proc = self.start()
        sock, r = self.connect()
        self.send_message(sock, r, "before")
        sock.close()
        _stop_server(proc)

        restarted, self.port = _start_server(self.db_path)
        self.addCleanup(_stop_server, restarted)
        subscriber, sr = self.connect()
        self.assertEqual(self.send_recv(subscriber, sr, b"SUBSCRIBE\n"), "OK")
        sender, sender_r = self.connect()
        self.assertEqual(self.send_recv(sender, sender_r, b"SEND after\n"), "OK 2")
        self.assertEqual(self.read_line(sr), "MESSAGE 2 after")


class AcknowledgementTests(FreshServerTestCase):
    def subscribe(self) -> tuple[socket.socket, BinaryIO]:
        sock, reader = self.connect()
        self.assertEqual(self.send_recv(sock, reader, b"SUBSCRIBE\n"), "OK")
        return sock, reader

    def deliver(self, subscriber: BinaryIO, message: str) -> int:
        """Have a separate client store `message` and return the ID the subscriber was notified of."""
        sender, reader = self.connect()
        response = self.send_recv(sender, reader, f"SEND {message}\n".encode())
        status, _, message_id = response.partition(" ")
        self.assertEqual(status, "OK", response)
        self.assertEqual(self.read_line(subscriber), f"MESSAGE {message_id} {message}")
        return int(message_id)

    def test_ack_returns_ok(self):
        sock, r = self.subscribe()
        message_id = self.deliver(r, "hello")
        self.assertEqual(self.send_recv(sock, r, f"ACK {message_id}\n".encode()), "OK")

    def test_ack_strips_surrounding_whitespace(self):
        sock, r = self.subscribe()
        self.deliver(r, "padded")
        self.assertEqual(self.send_recv(sock, r, b"ACK    1    \n"), "OK")

    def test_ack_message_never_delivered_is_invalid(self):
        sock, r = self.subscribe()
        self.assertEqual(self.send_recv(sock, r, b"ACK 1\n"), INVALID_ACK)
        self.deliver(r, "hello")
        self.assertEqual(self.send_recv(sock, r, b"ACK 999\n"), INVALID_ACK)
        self.assertEqual(self.send_recv(sock, r, b"ACK 0\n"), INVALID_ACK)

    def test_ack_history_message_not_delivered_to_this_connection_is_invalid(self):
        sender, sr = self.connect()
        self.send_message(sender, sr, "before subscribe")
        sock, r = self.subscribe()
        self.assertEqual(self.send_recv(sock, r, b"ACK 1\n"), INVALID_ACK)
        self.assertEqual(self.send_recv(sock, r, b"GET 1\n"), "1 before subscribe")

    def test_ack_by_client_that_is_not_subscribed_is_invalid(self):
        sock, r = self.connect()
        sender, sr = self.connect()
        self.assertEqual(self.send_recv(sender, sr, b"SEND hello\n"), "OK 1")
        self.assertEqual(self.send_recv(sock, r, b"ACK 1\n"), INVALID_ACK)

    def test_ack_malformed_id(self):
        sock, r = self.subscribe()
        self.deliver(r, "exists")
        self.assertResponses(
            sock,
            r,
            [
                b"ACK abc\n",
                b"ACK -1\n",
                b"ACK 1.5\n",
                b"ACK 1abc\n",
                b"ACK 0x1\n",
                b"ACK 1 2\n",
            ],
            INVALID_MESSAGE_ID,
        )

    def test_ack_without_id(self):
        sock, r = self.subscribe()
        self.deliver(r, "exists")
        self.assertResponses(sock, r, [b"ACK\n", b"ACK    \n"], INVALID_MESSAGE_ID)

    def test_repeated_ack_is_invalid(self):
        sock, r = self.subscribe()
        self.deliver(r, "once")
        self.assertEqual(self.send_recv(sock, r, b"ACK 1\n"), "OK")
        self.assertEqual(self.send_recv(sock, r, b"ACK 1\n"), INVALID_ACK)

    def test_ack_one_message_leaves_the_others_pending(self):
        sock, r = self.subscribe()
        self.deliver(r, "first")
        self.deliver(r, "second")
        self.assertEqual(self.send_recv(sock, r, b"ACK 2\n"), "OK")
        self.assertEqual(self.send_recv(sock, r, b"ACK 1\n"), "OK")
        self.assertEqual(self.send_recv(sock, r, b"ACK 2\n"), INVALID_ACK)

    def test_acks_are_independent_per_connection(self):
        first, fr = self.subscribe()
        second, sr = self.subscribe()
        sender, sender_r = self.connect()
        self.assertEqual(self.send_recv(sender, sender_r, b"SEND shared\n"), "OK 1")
        self.assertEqual(self.read_line(fr), "MESSAGE 1 shared")
        self.assertEqual(self.read_line(sr), "MESSAGE 1 shared")
        self.assertEqual(self.send_recv(first, fr, b"ACK 1\n"), "OK")
        self.assertEqual(self.send_recv(second, sr, b"ACK 1\n"), "OK")
        self.assertEqual(self.send_recv(first, fr, b"ACK 1\n"), INVALID_ACK)

    def test_commands_work_while_acknowledgement_is_pending(self):
        sock, r = self.subscribe()
        self.deliver(r, "pending")
        self.assertEqual(self.send_recv(sock, r, b"ECHO hi\n"), "OK hi")
        self.assertEqual(self.send_recv(sock, r, b"GET 1\n"), "1 pending")
        sock.sendall(b"HISTORY\n")
        self.assertEqual(self.read_history(r), ["1 pending"])
        self.assertEqual(self.send_recv(sock, r, b"ACK 1\n"), "OK")

    def test_subscribed_sender_must_ack_its_own_notification(self):
        sock, r = self.subscribe()
        sock.sendall(b"SEND mine\n")
        self.assertEqual(
            {self.read_line(r), self.read_line(r)}, {"OK 1", "MESSAGE 1 mine"}
        )
        self.assertEqual(self.send_recv(sock, r, b"ACK 1\n"), "OK")

    def test_disconnect_does_not_affect_other_clients_acks(self):
        gone, gr = self.subscribe()
        survivor, sr = self.subscribe()
        sender, sender_r = self.connect()
        self.assertEqual(self.send_recv(sender, sender_r, b"SEND shared\n"), "OK 1")
        self.assertEqual(self.read_line(gr), "MESSAGE 1 shared")
        self.assertEqual(self.read_line(sr), "MESSAGE 1 shared")
        gone.close()
        time.sleep(0.2)
        self.assertEqual(self.send_recv(survivor, sr, b"ACK 1\n"), "OK")
        self.assertEqual(self.send_recv(sender, sender_r, b"SEND next\n"), "OK 2")
        self.assertEqual(self.read_line(sr), "MESSAGE 2 next")

    def test_new_connection_starts_with_no_acknowledgement_state(self):
        sock, r = self.subscribe()
        self.deliver(r, "old")
        sock.close()
        time.sleep(0.2)

        reconnected, rr = self.connect()
        self.assertEqual(self.send_recv(reconnected, rr, b"ACK 1\n"), INVALID_ACK)
        self.assertEqual(self.send_recv(reconnected, rr, b"SUBSCRIBE\n"), "OK")
        message_id = self.deliver(rr, "new")
        self.assertEqual(message_id, 2)
        self.assertEqual(self.send_recv(reconnected, rr, b"ACK 1\n"), INVALID_ACK)
        self.assertEqual(self.send_recv(reconnected, rr, b"ACK 2\n"), "OK")


if __name__ == "__main__":
    unittest.main()
