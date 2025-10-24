"""
Receiver CLI.

- Connects to server as receiver.
- Receives 'meta' from sender (forwarded via server), responds with meta_response with missing_chunks.
- Receives 'chunk' messages, verifies sha1, writes into <filename>.tmp at correct offset, persists <filename>.meta.
- When all chunks received, do final verification, rename .tmp -> filename and delete .meta, send finish success.
- All messages are msgpack (binary frames).
"""

from __future__ import annotations

import asyncio
import os
import pathlib
import sys

import aiofiles
import msgpack
import websockets
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential
from tqdm.asyncio import tqdm

import config
from common import (
    FileMeta,
    pack,
    sha1_hex,
    unpack,
    set_terminal_title
)

# configure logger
logger.remove()
logger.add(sys.stdout, level=config.LOG_LEVEL)

META_SUFFIX = ".meta"
TMP_SUFFIX = ".tmp"


async def save_meta(meta_path: pathlib.Path, meta: FileMeta) -> None:
    # persist meta as msgpack
    data = meta.to_dict()
    async with aiofiles.open(meta_path, "wb") as f:
        await f.write(msgpack.packb(data, use_bin_type=True))  # type: ignore


async def load_meta(meta_path: pathlib.Path) -> FileMeta:
    async with aiofiles.open(meta_path, "rb") as f:
        b = await f.read()
    d = msgpack.unpackb(b, raw=False)
    return FileMeta.from_dict(d)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=config.TENACITY_BACKOFF_BASE),
)
async def aio_write_at(path: pathlib.Path, offset: int, data: bytes) -> None:
    # Retry wrapper for file IO
    # Open r+b if exists else wb+
    async with aiofiles.open(path, "r+b" if path.exists() else "wb+") as f:
        await f.seek(offset)
        await f.write(data)


async def handle_session(server_uri: str) -> None:
    logger.info("Connecting to server {}", server_uri)
    async with websockets.connect(server_uri, max_size=None) as ws:
        # handshake as receiver
        await ws.send(pack({"type": "role", "role": "receiver"}))
        # wait for session assignment
        raw = await ws.recv()
        msg = unpack(raw if isinstance(raw, (bytes, bytearray)) else raw.encode())  # type: ignore
        if msg.get("type") != "session" or "code" not in msg:
            logger.error("Didn't receive session assignment: {}", msg)
            return
        code = msg["code"]
        set_terminal_title(f"FTS Receiver - {code}")
        logger.info("Assigned session code: {}. Waiting for sender...", code)

        # state for active file(s). We only support single file per session as spec.
        file_meta: FileMeta | None = None
        file_lock = asyncio.Lock()
        file_pbar: dict[str, tqdm] = {}

        async for raw in ws:
            if not isinstance(raw, (bytes, bytearray)):
                continue
            try:
                msg = unpack(bytes(raw))
            except Exception as e:
                logger.exception("Invalid msgpack received: {}", e)
                continue

            t = msg.get("type")
            if t == "meta":
                # Sender intends to send a file
                fm = FileMeta(
                    file_id=msg["file_id"],
                    filename=msg["filename"],
                    size=int(msg["size"]),
                    chunk_size=int(msg["chunk_size"]),
                    chunks=int(msg["chunks"]),
                    received=set(),
                )
                logger.info("File '{}' meta received", fm.filename)

                target_path = pathlib.Path(config.RECEIVE_DIR, fm.filename)
                target_path.parent.mkdir(parents=True, exist_ok=True)
                meta_path = target_path.with_suffix(target_path.suffix + META_SUFFIX)
                tmp_path = target_path.with_suffix(target_path.suffix + TMP_SUFFIX)

                # Check if final file exists
                if target_path.exists():
                    await ws.send(
                        pack(
                            {
                                "type": "meta_response",
                                "status": "error",
                                "message": "file exists",
                            }
                        )
                    )
                    logger.info("File exists: {}. Informed sender.", target_path)
                    continue
                # if meta exists, load it
                if meta_path.exists():
                    # noinspection PyBroadException
                    try:
                        existing = await load_meta(meta_path)
                        # ensure file_id matches
                        if existing.file_id == fm.file_id:
                            fm = existing
                            missing = [
                                i for i in range(fm.chunks) if i not in fm.received
                            ]
                        else:
                            # different file id -> start fresh
                            fm.received = set()
                            missing = list(range(fm.chunks))
                    except Exception:
                        fm.received = set()
                        missing = list(range(fm.chunks))
                else:
                    # persist initial meta
                    await save_meta(meta_path, fm)
                    missing = list(range(fm.chunks))

                file_meta = fm
                # progress bar using tqdm.asyncio
                file_pbar[fm.file_id] = tqdm(
                    total=fm.chunks,
                    initial=len(fm.received),
                    ascii=True,
                    desc=f"Receiving {fm.filename}",
                    unit="chunk",
                )

                await ws.send(
                    pack(
                        {
                            "type": "meta_response",
                            "file_id": fm.file_id,
                            "status": "ok",
                            "missing_chunks": missing,
                        }
                    )
                )
            elif t == "chunk":
                if not file_meta:
                    logger.warning("Received chunk before meta. Ignoring.")
                    continue
                idx = int(msg["index"])
                payload: bytes = msg["payload"]
                expected_sha1: str = msg["sha1"]
                offset = idx * file_meta.chunk_size
                target_path = pathlib.Path(config.RECEIVE_DIR, file_meta.filename)
                target_path.parent.mkdir(parents=True, exist_ok=True)
                meta_path = target_path.with_suffix(target_path.suffix + META_SUFFIX)
                tmp_path = target_path.with_suffix(target_path.suffix + TMP_SUFFIX)

                # Verify sha1 for the payload
                actual = sha1_hex(payload)
                if actual != expected_sha1:
                    # send nack
                    await ws.send(
                        pack(
                            {
                                "type": "nack",
                                "file_id": file_meta.file_id,
                                "index": idx,
                                "reason": "sha1 mismatch",
                            }
                        )
                    )
                    logger.warning(
                        "Chunk {} sha1 mismatch (got {} expected {}). NACK sent.",
                        idx,
                        actual,
                        expected_sha1,
                    )
                    continue

                # write to tmp at offset
                async with file_lock:
                    try:
                        await aio_write_at(tmp_path, offset, payload)
                    except Exception as e:
                        logger.exception("Failed to write chunk {}: {}", idx, e)
                        await ws.send(
                            pack(
                                {
                                    "type": "nack",
                                    "file_id": file_meta.file_id,
                                    "index": idx,
                                    "reason": "io error",
                                }
                            )
                        )
                        continue

                    # mark received and persist meta
                    file_meta.received.add(idx)
                    try:
                        await save_meta(meta_path, file_meta)
                    except Exception as e:
                        logger.exception(
                            "Failed to persist meta for chunk {}: {}", idx, e
                        )
                        # still ack so sender will not repeatedly send? We'll nack to force retry.
                        await ws.send(
                            pack(
                                {
                                    "type": "nack",
                                    "file_id": file_meta.file_id,
                                    "index": idx,
                                    "reason": "meta persist error",
                                }
                            )
                        )
                        file_meta.received.remove(idx)
                        continue

                # send ack
                await ws.send(
                    pack({"type": "ack", "file_id": file_meta.file_id, "index": idx})
                )

                received_size = len(file_meta.received)
                pbar = file_pbar.get(fm.file_id)
                if pbar is not None:
                    pbar.n = received_size
                    pbar.refresh()

                # check completion
                if received_size >= file_meta.chunks:
                    # final verification
                    size_ok = (
                            tmp_path.exists() and tmp_path.stat().st_size == file_meta.size
                    )
                    if not size_ok:
                        await ws.send(
                            pack({"type": "error", "message": "final size mismatch"})
                        )
                        logger.error(
                            "Final size mismatch: expected {} actual {}",
                            file_meta.size,
                            tmp_path.stat().st_size if tmp_path.exists() else -1,
                        )
                        continue

                    pbar = file_pbar.get(fm.file_id)
                    if pbar is not None:
                        pbar.close()

                    # atomically rename tmp -> filename
                    try:
                        os.replace(tmp_path, target_path)  # atomic on most OS
                        # remove meta
                        try:
                            os.remove(meta_path)
                        except Exception as e:
                            logger.warning(
                                "Failed to remove meta file {}: {}", meta_path, e
                            )
                        await ws.send(
                            pack(
                                {
                                    "type": "finish",
                                    "file_id": file_meta.file_id,
                                    "status": "success",
                                }
                            )
                        )
                        logger.info("Transfer complete, saved {}", target_path)
                        # reset file_meta for next transfer
                        file_meta = None
                    except Exception as e:
                        logger.exception("Failed to finalize file move: {}", e)
                        await ws.send(
                            pack({"type": "error", "message": "finalize error"})
                        )

            elif t == "attach" or t == "detach":
                sender_address: str | None = msg.get("sender_address")
                if sender_address is None:
                    logger.error("Sender address not exists!")
                else:
                    if t == "attach":
                        logger.info("Sender connected: {}", sender_address)
                    elif t == "detach":
                        logger.info("Sender disconnected: {}", sender_address)

            elif t in ("ack", "nack", "finish", "error", "meta_response"):
                # Normally receiver doesn't expect these from sender forwarded via server,
                # but if it does, just log
                logger.debug("Received control message: {}", msg)
            else:
                logger.warning("Unknown message type: {}", t)


def main() -> None:
    set_terminal_title("FTS Receiver")
    asyncio.run(handle_session(config.SERVER_URI))


if __name__ == "__main__":
    main()
