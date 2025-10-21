"""
Configuration for in-memory relay file transfer.

All values can be overridden with environment variables (see below).
"""
from __future__ import annotations

import os

# Network
SERVER_LISTEN_HOST: str = os.getenv("SERVER_HOST", "0.0.0.0")
SERVER_LISTEN_PORT: int = int(os.getenv("SERVER_PORT", "8765"))
SERVER_URI: str = f"ws://localhost:{SERVER_LISTEN_PORT}"

# Chunking / concurrency / flow-control
CHUNK_SIZE: int = int(os.getenv("CHUNK_SIZE", str(2 << 20)))  # 2 MiB
MAX_IN_FLIGHT: int = int(os.getenv("MAX_IN_FLIGHT", "16"))
QUEUE_MAXSIZE: int = int(os.getenv("QUEUE_MAXSIZE", "32"))

# Session management
SESSION_TTL_SECONDS: int = int(os.getenv("SESSION_TTL_SECONDS", str(60 * 60)))  # 60 minutes
MAX_SESSIONS: int = int(os.getenv("MAX_SESSIONS", "32"))

# Tenacity (retry) defaults
TENACITY_MAX_ATTEMPTS: int = int(os.getenv("TENACITY_MAX_ATTEMPTS", "5"))
TENACITY_BACKOFF_BASE: float = float(os.getenv("TENACITY_BACKOFF_BASE", "0.5"))

# Logging
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

# Receive
RECEIVE_DIR: str = os.getenv("RECEIVE_DIR", "./downloads")
