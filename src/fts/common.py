"""
Shared types, dataclasses, msgpack helpers, sha1 utilities, and exceptions.
"""
from __future__ import annotations

import hashlib
import time
import uuid
from dataclasses import dataclass, asdict
from typing import Any, Dict, Set

import msgpack


# ---------- Utilities ----------
def now_ts() -> float:
    return time.time()


def gen_code() -> str:
    # 4-digit zero-padded numeric code
    return f"{uuid.uuid4().int % 10000:04d}"


def sha1_hex(data: bytes) -> str:
    h = hashlib.sha1()
    h.update(data)
    return h.hexdigest()


# ---------- MsgPack helpers ----------
def pack(msg: Dict[str, Any]) -> bytes:
    # Use use_bin_type True for bytes support
    return msgpack.packb(msg, use_bin_type=True)


def unpack(b: bytes) -> Dict[str, Any]:
    return msgpack.unpackb(b, raw=False)


# ---------- Dataclasses ----------
@dataclass
class SessionInfo:
    code: str
    receiver_ws: Any  # WebSocketServerProtocol or client protocol
    sender_ws: Any | None
    last_active: float
    meta: Dict[str, Any] | None = None  # optional small in-memory metadata
    lock: Any | None = None  # placeholder for per-session lock if needed


@dataclass
class FileMeta:
    file_id: str
    filename: str
    size: int
    chunk_size: int
    chunks: int
    received: Set[int]  # indexes received

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["received"] = sorted(list(self.received))
        return d

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "FileMeta":
        return cls(
            file_id=d["file_id"],
            filename=d["filename"],
            size=int(d["size"]),
            chunk_size=int(d["chunk_size"]),
            chunks=int(d["chunks"]),
            received=set(int(x) for x in d.get("received", [])),
        )


# ---------- Exceptions ----------
class TransferError(Exception):
    """Generic transfer error."""


class SessionNotFound(TransferError):
    pass


class ProtocolError(TransferError):
    pass
