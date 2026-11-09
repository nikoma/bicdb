"""Minimal pgwire (protocol v3) client, stdlib only.

Written from the protocol specification for this evidence harness so the
reconciliation does not depend on any BicDB code or any client library.
Supports: startup without a password (trust) or with cleartext password,
the simple query protocol, ErrorResponse fields, and NoticeResponse.
"""

import socket
import struct


class ConnectionLost(Exception):
    pass


class PgError(Exception):
    def __init__(self, fields):
        self.fields = fields
        self.code = fields.get("C", "")
        self.message = fields.get("M", "")
        self.detail = fields.get("D", "")
        self.hint = fields.get("H", "")
        super().__init__(f"{self.code}: {self.message} [{self.detail}]")


class Result:
    def __init__(self):
        self.columns = []
        self.rows = []
        self.tag = None


class Conn:
    def __init__(self, host, port, user="bicdb", database="bicdb", timeout=60.0, password=None):
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.buf = b""
        params = b"user\0" + user.encode() + b"\0database\0" + database.encode() + b"\0\0"
        body = struct.pack("!I", 196608) + params
        self._send(struct.pack("!I", len(body) + 4) + body)
        while True:
            kind, payload = self._recv_msg()
            if kind == b"R":
                (code,) = struct.unpack("!I", payload[:4])
                if code == 0:
                    continue
                if code == 3 and password is not None:
                    pw = password.encode() + b"\0"
                    self._send(b"p" + struct.pack("!I", len(pw) + 4) + pw)
                    continue
                raise PgError({"C": "28000", "M": f"unsupported auth request {code}"})
            if kind == b"E":
                raise PgError(_fields(payload))
            if kind == b"Z":
                return
            # S (ParameterStatus), K (BackendKeyData), N (Notice): ignore

    def _send(self, data):
        try:
            self.sock.sendall(data)
        except OSError as error:
            raise ConnectionLost(str(error))

    def _read(self, n):
        while len(self.buf) < n:
            try:
                chunk = self.sock.recv(65536)
            except OSError as error:
                raise ConnectionLost(str(error))
            if not chunk:
                raise ConnectionLost("connection closed by server")
            self.buf += chunk
        data, self.buf = self.buf[:n], self.buf[n:]
        return data

    def _recv_msg(self):
        header = self._read(5)
        kind = header[:1]
        (length,) = struct.unpack("!I", header[1:])
        return kind, self._read(length - 4)

    def query(self, sql):
        """Run one simple query. Returns the last statement's Result; raises
        PgError for the first ErrorResponse (after draining to ReadyForQuery)."""
        body = sql.encode() + b"\0"
        self._send(b"Q" + struct.pack("!I", len(body) + 4) + body)
        result = Result()
        error = None
        while True:
            kind, payload = self._recv_msg()
            if kind == b"T":
                (count,) = struct.unpack("!H", payload[:2])
                pos = 2
                cols = []
                for _ in range(count):
                    end = payload.index(b"\0", pos)
                    cols.append(payload[pos:end].decode())
                    pos = end + 1 + 18
                result = Result()
                result.columns = cols
            elif kind == b"D":
                (count,) = struct.unpack("!H", payload[:2])
                pos = 2
                row = []
                for _ in range(count):
                    (length,) = struct.unpack("!i", payload[pos:pos + 4])
                    pos += 4
                    if length < 0:
                        row.append(None)
                    else:
                        row.append(payload[pos:pos + length].decode())
                        pos += length
                result.rows.append(row)
            elif kind == b"C":
                result.tag = payload.rstrip(b"\0").decode()
            elif kind == b"E":
                if error is None:
                    error = PgError(_fields(payload))
            elif kind == b"Z":
                if error is not None:
                    raise error
                return result
            # I (EmptyQuery), N (Notice), S, A: ignore

    def close(self):
        try:
            self.sock.sendall(b"X\0\0\0\4")
        except OSError:
            pass
        try:
            self.sock.close()
        except OSError:
            pass


def _fields(payload):
    fields = {}
    pos = 0
    while pos < len(payload) and payload[pos] != 0:
        code = chr(payload[pos])
        end = payload.index(b"\0", pos + 1)
        fields[code] = payload[pos + 1:end].decode(errors="replace")
        pos = end + 1
    return fields
