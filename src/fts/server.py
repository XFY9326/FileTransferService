"""
In-memory relay server.

- No file writes.
- Keeps minimal session metadata in memory.
- Generates 4-digit session codes for receivers.
- Matches sender to receiver by code and forwards msgpack binary frames.
- Reclaims sessions after SESSION_TTL_SECONDS inactivity.
"""

from __future__ import annotations

import asyncio
import sys

import websockets
from loguru import logger

import config
from common import (
    SessionInfo,
    gen_code,
    now_ts,
    pack,
    unpack,
)

# Configure loguru to respect config.LOG_LEVEL
logger.remove()
logger.add(sys.stdout, level=config.LOG_LEVEL)


class RelayServer:
    def __init__(self, host: str, port: int):
        self.host: str = host
        self.port: int = port
        self.sessions: dict[str, SessionInfo] = {}
        self.sessions_lock: asyncio.Lock = asyncio.Lock()
        self._stop: asyncio.Event = asyncio.Event()
        self.server: websockets.Server | None = None
        self._sweeper_task: asyncio.Task | None = None

    async def start(self) -> None:
        logger.info("Starting relay server on {}:{}", self.host, self.port)
        self.server = await websockets.serve(
            self.handler, self.host, self.port, max_size=None
        )
        self._sweeper_task = asyncio.create_task(self._sweep_sessions_loop())
        await self._stop.wait()

    async def stop(self) -> None:
        self._stop.set()
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        if self._sweeper_task:
            self._sweeper_task.cancel()

    async def _sweep_sessions_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(10)
                await self._sweep_sessions()
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.exception("sweeper failed: {}", e)

    # noinspection PyBroadException
    async def _sweep_sessions(self) -> None:
        now = now_ts()
        async with self.sessions_lock:
            to_remove = []
            for code, s in list(self.sessions.items()):
                if now - s.last_active > config.SESSION_TTL_SECONDS:
                    to_remove.append(code)
            for code in to_remove:
                logger.info("Session {} expired by TTL, closing connections", code)
                s = self.sessions.pop(code)
                try:
                    if s.receiver_ws:
                        await s.receiver_ws.close()
                except Exception:
                    pass
                try:
                    if s.sender_ws:
                        await s.sender_ws.close()
                except Exception:
                    pass

    async def handler(self, websocket: websockets.ServerConnection):
        """
        First message from client MUST be role handshake: { "type":"role", "role":"receiver" } or role:"sender", optionally code for sender.
        For receiver: server assigns code and returns {"type":"session","code":"1234"}.
        Then server simply forwards binary msgpack frames between sender and receiver.
        """
        peer = websocket.remote_address
        logger.info("Connection from {}", peer)
        try:
            raw = await websocket.recv()
            if not isinstance(raw, (bytes, bytearray)):
                # Expect msgpack binary
                logger.warning("Handshake not binary from {}", peer)
                await websocket.close(
                    code=1002, reason="expected binary msgpack handshake"
                )
                return
            try:
                msg = unpack(bytes(raw))
            except Exception as e:
                logger.exception("Invalid handshake msgpack: {}", e)
                await websocket.close(code=1002, reason="invalid msgpack")
                return

            if msg.get("type") != "role" or "role" not in msg:
                logger.warning("Invalid role handshake: {}", msg)
                await websocket.close(code=1002, reason="invalid handshake")
                return

            role = msg["role"]
            if role == "receiver":
                await self._handle_receiver(websocket)
            elif role == "sender":
                code = msg.get("code")
                await self._handle_sender(websocket, code)
            else:
                logger.warning("Unknown role: {}", role)
                await websocket.close(code=1002, reason="unknown role")
        except websockets.ConnectionClosed:
            logger.info("Connection closed during handshake: {}", peer)
        except Exception as e:
            logger.exception("Handler exception for {}: {}", peer, e)
            # noinspection PyBroadException
            try:
                await websocket.close()
            except Exception:
                pass

    async def _handle_receiver(self, websocket: websockets.ServerConnection):
        # assign code and register session
        async with self.sessions_lock:
            if len(self.sessions) >= config.MAX_SESSIONS:
                logger.warning("Max sessions reached")
                await websocket.send(pack({"type": "error", "message": "server busy"}))
                await websocket.close()
                return
            # ensure code unique
            code = gen_code()
            while code in self.sessions:
                code = gen_code()
            session = SessionInfo(
                code=code,
                receiver_ws=websocket,
                sender_ws=None,
                last_active=now_ts(),
                meta=None,
                lock=asyncio.Lock(),
            )
            self.sessions[code] = session

        # send session assignment
        await websocket.send(pack({"type": "session", "code": code}))
        logger.info("Assigned code {} to receiver {}", code, websocket.remote_address)

        try:
            # wait for messages from receiver and forward to sender when present.
            async for raw in websocket:
                # raw should be bytes
                if not isinstance(raw, (bytes, bytearray)):
                    continue
                msg = raw  # forward raw binary
                session.last_active = now_ts()
                # forward if sender present
                async with self.sessions_lock:
                    s = self.sessions.get(code)
                    target = s.sender_ws if s else None
                if target:
                    try:
                        await target.send(msg)
                    except Exception as e:
                        logger.exception(
                            "Failed to forward receiver->sender for {}: {}", code, e
                        )
                # else: keep alive; no sender yet
        except websockets.ConnectionClosed:
            logger.info("Receiver disconnected for code {}", code)
        finally:
            # remove session
            async with self.sessions_lock:
                if code in self.sessions:
                    s = self.sessions.pop(code)
                    # close sender if exists
                    if s.sender_ws:
                        # noinspection PyBroadException
                        try:
                            await s.sender_ws.close()
                        except Exception:
                            pass
            logger.info("Session {} removed (receiver connection ended)", code)

    async def _handle_sender(
            self, websocket: websockets.ServerConnection, code: str | None
    ):
        if not code:
            await websocket.send(pack({"type": "error", "message": "missing code"}))
            await websocket.close()
            return
        async with self.sessions_lock:
            session = self.sessions.get(code)
            if not session:
                await websocket.send(
                    pack({"type": "error", "message": "session not found"})
                )
                await websocket.close()
                return
            if session.sender_ws is not None:
                await websocket.send(
                    pack({"type": "error", "message": "session is already in use"})
                )
                await websocket.close()
                return
            # attach sender
            session.sender_ws = websocket
            session.last_active = now_ts()
            logger.info(
                "Sender {} attached to session {}", websocket.remote_address, code
            )

        receiver = session.receiver_ws
        await receiver.send(
            pack({"type": "connection", "sender_address": websocket.remote_address})
        )

        # notify both sides we have matched? optional
        try:
            # Forward loop: read from sender and forward to receiver.
            async for raw in websocket:
                if not isinstance(raw, (bytes, bytearray)):
                    continue
                session.last_active = now_ts()
                try:
                    await receiver.send(raw)
                except Exception as e:
                    logger.exception(
                        "Failed to forward sender->receiver for {}: {}", code, e
                    )
        except websockets.ConnectionClosed:
            logger.info("Sender disconnected for {}", code)
        finally:
            # cleanup
            async with self.sessions_lock:
                s = self.sessions.get(code)
                if s:
                    s.sender_ws = None
                    # if receiver still connected we leave session for TTL
            logger.info("Sender detached from {}", code)


async def main():
    rs = RelayServer(config.SERVER_LISTEN_HOST, config.SERVER_LISTEN_PORT)

    try:
        await rs.start()
    except KeyboardInterrupt:
        pass
    finally:
        await rs.stop()


if __name__ == "__main__":
    asyncio.run(main())
