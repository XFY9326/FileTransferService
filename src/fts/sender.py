"""
Sender CLI.

- Connects to server as sender with a 4-digit code (provided by receiver).
- Prompts user for a single file path (strip whitespace and optional surrounding quotes).
- Sends 'meta', processes 'meta_response'.
  - If meta_response.status == "error" and message == "file exists": prompt user to re-enter path or 'cancel'.
- Sends only missing chunks (based on meta_response.missing_chunks) using a bounded asyncio.Queue and workers.
- Uses semaphore to bound in-flight chunks.
- Waits for ack/nack for each chunk (listener sets Events).
- Retries per-chunk using tenacity on network or nack.
- If user enters empty line as first prompt -> exit and close, server releases code.
"""

from __future__ import annotations

import asyncio
import pathlib
import sys
import uuid
from typing import Dict, Set

import aiofiles
import websockets
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential
from tqdm.asyncio import tqdm

import config
from common import pack, sha1_hex, unpack, set_terminal_title

# configure logger
logger.remove()
logger.add(sys.stdout, level=config.LOG_LEVEL)


class SenderClient:
    def __init__(self, server_uri: str, code: str):
        self.server_uri = server_uri
        self.code = code
        self.ack_events: Dict[int, asyncio.Event] = {}
        self.ack_status: Dict[int, bool] = {}
        self.lock = asyncio.Lock()
        self.send_queue: asyncio.Queue[int] = asyncio.Queue(
            maxsize=config.QUEUE_MAXSIZE
        )
        self.in_flight_sem = asyncio.Semaphore(config.MAX_IN_FLIGHT)
        self.total_chunks = 0
        self.chunk_size = config.CHUNK_SIZE
        self.file_path: pathlib.Path | None = None
        self.file_id: str | None = None
        self.chunks_to_send: Set[int] = set()

        # tenacity parameters
        self.tenacity_kwargs = dict(
            stop=stop_after_attempt(config.TENACITY_MAX_ATTEMPTS),
            wait=wait_exponential(multiplier=config.TENACITY_BACKOFF_BASE),
        )

    async def run(self):
        async with websockets.connect(self.server_uri, max_size=None) as ws:
            # handshake as sender
            await ws.send(pack({"type": "role", "role": "sender", "code": self.code}))

            # primary loop: prompt for file path, handle sending
            while True:
                path_input = await self._prompt_file_path()
                if path_input == "":
                    logger.info("Empty input, exiting session.")
                    await ws.close()
                    return

                p = pathlib.Path(path_input)
                if not p.exists() or not p.is_file():
                    logger.error("File not found or not a regular file: {}", p)
                    continue

                # create file_id
                fid = str(
                    uuid.uuid5(
                        uuid.NAMESPACE_URL,
                        f"{p}_{p.stat().st_mtime_ns}_{p.stat().st_size}",
                    )
                )
                self.file_id = fid
                self.file_path = p
                size = p.stat().st_size
                chunk_size = config.CHUNK_SIZE
                chunks = (size + chunk_size - 1) // chunk_size
                self.total_chunks = chunks
                self.chunk_size = chunk_size

                # send meta
                meta_msg = {
                    "type": "meta",
                    "file_id": fid,
                    "filename": p.name,
                    "size": int(size),
                    "chunk_size": int(chunk_size),
                    "chunks": int(chunks),
                }
                await ws.send(pack(meta_msg))

                # await meta_response
                meta_resp_raw = await ws.recv()
                meta_resp = unpack(
                    meta_resp_raw
                    if isinstance(meta_resp_raw, (bytes, bytearray))
                    else meta_resp_raw.encode()  # type: ignore
                )
                if meta_resp.get("type") != "meta_response":
                    logger.error("Unexpected response to meta: {}", meta_resp)
                    continue

                if meta_resp.get("status") == "error":
                    msg = meta_resp.get("message", "")
                    if msg == "file exists":
                        # must prompt user to re-enter or cancel
                        logger.warning(
                            "Receiver reports the file already exists on the destination."
                        )
                        # force reinput
                        continue
                    else:
                        logger.error("Meta error: {}", msg)
                        continue

                missing = meta_resp.get("missing_chunks", list(range(chunks)))
                if not missing:
                    logger.info(
                        "No missing chunks reported; file may already be present. Session finished."
                    )
                    continue

                self.chunks_to_send = set(int(x) for x in missing)
                # prepare ack_events
                for idx in self.chunks_to_send:
                    self.ack_events[idx] = asyncio.Event()
                    self.ack_status[idx] = False

                # create tasks: listener for ACKs, workers, and producer to enqueue missing indexes
                listener_task = asyncio.create_task(self._listener(ws))
                producer_task = asyncio.create_task(self._producer())
                workers = [
                    asyncio.create_task(self._worker(ws))
                    for _ in range(config.MAX_IN_FLIGHT)
                ]

                # progress bar using tqdm.asyncio
                pbar = tqdm(
                    total=len(self.chunks_to_send),
                    ascii=True,
                    desc=f"Sending {p.name}",
                    unit="chunk",
                )
                try:
                    # wait until all ack_events are set to True (success)
                    while True:
                        # check completed ACKs
                        completed = sum(
                            1
                            for idx in self.chunks_to_send
                            if self.ack_status.get(idx, False)
                        )
                        pbar.n = completed
                        pbar.refresh()
                        if completed >= len(self.chunks_to_send):
                            break
                        await asyncio.sleep(0.2)
                    # done, wait for possible finish message
                    # allow listener to process finish
                    await asyncio.sleep(0.2)
                finally:
                    pbar.close()
                    listener_task.cancel()
                    producer_task.cancel()
                    for w in workers:
                        w.cancel()
                    # clear state for next file
                    self.ack_events.clear()
                    self.ack_status.clear()
                    self.chunks_to_send.clear()

                logger.info(
                    "File transfer (attempt) finished. You may send another file or press Enter on an empty line to exit."
                )

    async def test_connection(self) -> bool:
        """Test connection to the server."""
        # noinspection PyBroadException
        try:
            async with websockets.connect(self.server_uri, max_size=None) as ws:
                await ws.ping()
            return True
        except Exception:
            return False

    async def _prompt_file_path(self) -> str:
        # Async input isn't standard; use thread to get input
        return await self._sync_input()

    @staticmethod
    async def _sync_input() -> str:
        prompt = "Enter path to file to send (empty line to exit): "
        try:
            loop = asyncio.get_event_loop()
            s = await loop.run_in_executor(None, input, prompt)
        except EOFError:
            s = ""
        s = s.strip()
        # strip surrounding quotes if both ends have them
        if len(s) >= 2 and ((s[0] == s[-1] == '"') or (s[0] == s[-1] == "'")):
            s = s[1:-1]
        return s

    async def _producer(self):
        # enqueue chunk indexes to send
        for idx in sorted(self.chunks_to_send):
            await self.send_queue.put(idx)

    async def _listener(self, ws: websockets.ClientConnection):
        # listens for ack/nack/finish/error messages from receiver (forwarded by server)
        try:
            async for raw in ws:
                if not isinstance(raw, (bytes, bytearray)):
                    continue
                msg = unpack(bytes(raw))
                t = msg.get("type")
                if t == "ack":
                    idx = int(msg["index"])
                    if idx in self.ack_events:
                        self.ack_status[idx] = True
                        self.ack_events[idx].set()
                elif t == "nack":
                    idx = int(msg["index"])
                    reason = msg.get("reason", "")
                    logger.warning("Received NACK for chunk {}: {}", idx, reason)
                    # clear event so worker will retry (worker relies on event not being set)
                    if idx in self.ack_events:
                        # ensure it is not set and set status false
                        self.ack_status[idx] = False
                        ev = self.ack_events[idx]
                        if ev.is_set():
                            # recreate event
                            self.ack_events[idx] = asyncio.Event()
                elif t == "finish":
                    logger.info("Received finish: {}", msg)
                elif t == "error":
                    logger.error("Received error: {}", msg)
                elif t == "terminate":
                    logger.error("Terminate due to: {}", msg["reason"])
                    exit(1)
                elif t == "meta_response":
                    # already handled upstream, ignore
                    pass
                else:
                    logger.debug("Listener got control msg: {}", msg)
        except asyncio.CancelledError:
            return
        except Exception as e:
            logger.exception("Listener exception: {}", e)

    async def _worker(self, ws: websockets.ClientConnection):
        try:
            while True:
                idx = await self.send_queue.get()
                if idx is None:
                    return
                await self.in_flight_sem.acquire()
                try:
                    await self._send_chunk_with_retries(ws, idx)
                finally:
                    self.in_flight_sem.release()
                    self.send_queue.task_done()
        except asyncio.CancelledError:
            return

    @retry(
        **{
            "stop": stop_after_attempt(config.TENACITY_MAX_ATTEMPTS),
            "wait": wait_exponential(multiplier=config.TENACITY_BACKOFF_BASE),
        }
    )
    async def _send_chunk_with_retries(self, ws, idx: int):
        # read chunk from file and attempt to send; wait for ack or raise error to trigger retry
        if self.file_path is None or self.file_id is None:
            raise RuntimeError("file_path or file_id missing")
        offset = idx * self.chunk_size
        size = min(self.chunk_size, self.file_path.stat().st_size - offset)
        # read data
        data = await self._read_chunk(self.file_path, offset, size)
        sha1 = sha1_hex(data)
        chunk_msg = {
            "type": "chunk",
            "file_id": self.file_id,
            "index": idx,
            "sha1": sha1,
            "payload": data,
        }
        # send
        await ws.send(pack(chunk_msg))
        # wait for ack or nack
        ev = self.ack_events.get(idx)
        if ev is None:
            # receiver might have accepted immediately; but ensure we create the event
            self.ack_events[idx] = asyncio.Event()
            ev = self.ack_events[idx]
        try:
            await asyncio.wait_for(ev.wait(), timeout=10.0)
        except asyncio.TimeoutError:
            # no ack -> raise to cause retry by tenacity decorator
            logger.warning("Timeout waiting for ACK for chunk {}; will retry", idx)
            raise
        # check status
        if not self.ack_status.get(idx, False):
            logger.warning("ACK status false for chunk {} -> retry", idx)
            raise RuntimeError("nack or other failure")
        # else success

    @staticmethod
    async def _read_chunk(path: pathlib.Path, offset: int, size: int) -> bytes:
        async with aiofiles.open(path, "r+b") as f:
            await f.seek(offset)
            return await f.read(size)


async def main_async(code: str):
    client = SenderClient(config.SERVER_URI, code)
    if not await client.test_connection():
        logger.error("Connection failed: {}", config.SERVER_URI)
        return
    await client.run()


def main():
    set_terminal_title("FTS Sender")
    code = input("Enter 4 digit code: ").strip()
    set_terminal_title(f"FTS Sender - {code}")
    asyncio.run(main_async(code))


if __name__ == "__main__":
    main()
