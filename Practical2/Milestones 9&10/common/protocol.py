"""
TeleRM wire protocol.

Every message is a JSON object preceded by a 4-byte big-endian length
prefix (design doc section 6: "4-byte length plus JSON over TCP. Each
exchange is one request and one reply.").

Additions for Milestones 9/10 (the framing is unchanged):

* request correlation: ``request()`` stamps every message with ``corr_id`` and
  every reply echoes it; a mismatching reply is rejected;
* a frame-size ceiling so a corrupt or hostile length prefix cannot make a
  peer allocate gigabytes;
* the server answers malformed frames with an ``ERROR`` reply instead of
  killing the connection thread, and converts handler exceptions to ``ERROR``.
"""
import json
import socket
import struct
import sys
import uuid

_HEADER = struct.Struct(">I")
MAX_MESSAGE_BYTES = 16 * 1024 * 1024


class ProtocolError(ConnectionError):
    """The peer sent something that is not a valid TeleRM frame."""


def send_msg(sock: socket.socket, obj: dict) -> int:
    """Send one length-prefixed JSON message. Returns bytes written
    (header + payload), for message/byte counters."""
    payload = json.dumps(obj).encode("utf-8")
    if len(payload) > MAX_MESSAGE_BYTES:
        raise ProtocolError(f"message too large: {len(payload)} bytes")
    sock.sendall(_HEADER.pack(len(payload)) + payload)
    return len(payload) + _HEADER.size


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("peer closed connection mid-message")
        buf.extend(chunk)
    return bytes(buf)


def recv_msg(sock: socket.socket):
    """Read one length-prefixed JSON message. Returns (obj, bytes_read)."""
    header = _recv_exact(sock, _HEADER.size)
    (length,) = _HEADER.unpack(header)
    if length > MAX_MESSAGE_BYTES:
        raise ProtocolError(f"frame length {length} exceeds limit")
    payload = _recv_exact(sock, length)
    try:
        return json.loads(payload.decode("utf-8")), length + _HEADER.size
    except (UnicodeDecodeError, ValueError) as exc:
        raise ProtocolError(f"malformed JSON frame: {exc}") from exc


def request(host: str, port: int, obj: dict, timeout: float = 5.0, counters=None):
    """Open a connection, send one message, read the one reply, close.
    This is the request/reply pattern used for every exchange in the
    protocol table (REGISTER, HEARTBEAT, REQUEST, RESIZE, ALLOCATE, ...).

    A ``corr_id`` is attached to the request (unless the caller supplied one)
    and verified on the reply.
    """
    obj = dict(obj)
    corr = obj.setdefault("corr_id", uuid.uuid4().hex[:12])
    with socket.create_connection((host, port), timeout=timeout) as sock:
        sock.settimeout(timeout)
        sent = send_msg(sock, obj)
        if counters is not None:
            counters.sent(sent)
        reply, recvd = recv_msg(sock)
        if counters is not None:
            counters.recv(recvd)
        if not isinstance(reply, dict):
            raise ProtocolError("reply is not a JSON object")
        if "corr_id" in reply and reply["corr_id"] != corr:
            raise ProtocolError("reply correlation id mismatch")
        return reply


def serve_forever(host: str, port: int, handler, counters=None, stop_flag=None):
    """Generic request/reply TCP server. `handler(msg) -> reply_dict`.
    Each accepted connection handles exactly one request/reply exchange,
    matching the protocol's request/reply semantics. Returns the bound
    server socket so the caller can close it to stop serving.

    If the handler raises ``ConnectionError`` the connection is closed without
    a reply (used by the fault injector to model a network partition).
    """
    import threading

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((host, port))
    srv.listen(128)

    def _client(conn):
        try:
            with conn:
                conn.settimeout(30)
                try:
                    msg, recvd = recv_msg(conn)
                except ProtocolError as exc:
                    send_msg(conn, {"type": "ERROR", "error": f"malformed_message: {exc}"})
                    return
                if counters is not None:
                    counters.recv(recvd)
                if not isinstance(msg, dict):
                    reply = {"type": "ERROR", "error": "message must be a JSON object"}
                else:
                    try:
                        reply = handler(msg)
                    except ConnectionError:
                        return
                    except Exception as exc:  # noqa: BLE001 - never kill the server thread
                        print(f"[protocol] handler error for {msg.get('type')!r}: {exc!r}",
                              file=sys.stderr, flush=True)
                        reply = {"type": "ERROR", "error": f"internal_error: {exc}"}
                    if not isinstance(reply, dict):
                        reply = {"type": "ERROR", "error": "handler returned no reply"}
                    if "corr_id" in msg:
                        reply.setdefault("corr_id", msg["corr_id"])
                sent = send_msg(conn, reply)
                if counters is not None:
                    counters.sent(sent)
        except (ConnectionError, socket.timeout, OSError):
            pass

    def _accept_loop():
        while True:
            try:
                conn, _addr = srv.accept()
            except OSError:
                break  # socket was closed -> shut down
            threading.Thread(target=_client, args=(conn,), daemon=True).start()

    t = threading.Thread(target=_accept_loop, daemon=True)
    t.start()
    return srv


def close_server(srv):
    """Stop a server returned by serve_forever. ``shutdown`` first: closing a listening
    socket does not wake a thread blocked in accept(), so without it the port would stay
    open (a "crashed" site would keep answering and a restarted manager could not rebind)."""
    if srv is None:
        return
    try:
        srv.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass
    try:
        srv.close()
    except OSError:
        pass
