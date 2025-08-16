# File Transfer Service

Simple in-memory relay server + CLI sender & receiver for transferring **single files** across an intranet via a central
server. The server never writes files to disk — all file data is forwarded in memory. Resume, parallel chunking,
per-chunk SHA-1, and robust retries are supported.

## Requirements

- Python >= 3.12
- Install dependencies:
    ```bash
    uv sync
    ```

## Files

- config.py — all configuration (env overrides supported)
- common.py — shared dataclasses, helpers
- server.py — in-memory relay server
- receiver.py — receiver CLI (runs on the machine that will receive the file)
- sender.py — sender CLI (runs on the machine that will send the file)
- README.md — this file

## Quick start

1. Start server:

  ```bash
  uv run src/fts/server.py
  # Server listens on 0.0.0.0:8765 by default (see config.py or env vars)
  ```

2. On receiver machine, start receiver:

  ```bash
  uv run src/fts/receiver.py
  # On connect it prints assigned 4-digit code, e.g. "Assigned session code: 1234. Waiting for sender..."
  # Keep this running. It writes <filename>.tmp and <filename>.meta in the current directory.
  ```

3. On sender machine, run sender with the receiver's 4-digit code:

  ```bash
  uv run src/fts/sender.py
  Enter 4 digit code: 1234
  Enter path to file to send (empty line to exit): /path/to/myfile.bin
  # Sender will send meta, then only missing chunks.
  # Progress is shown with tqdm. On completion, receiver renames .tmp to the final filename.
  ```

## Example interactions

**Receiver logs**

```
Assigned session code: 1234. Waiting for sender...
meta_response sent. missing_chunks=1024
ACK sent for chunk 0
...
Transfer complete, saved myfile.bin
```

**Sender logs**

```
Enter 4 digit code: 
Enter path to file to send (empty line to exit): "/home/user/myfile.bin"
sending myfile.bin:  12%|##        | 123/1000 [00:12<01:30, 7.00chunk/s]
...
File transfer (attempt) finished. You may send another file or press Enter on an empty line to exit.
```

If the receiver already has the full file, the receiver responds with:

```json
{
  "type": "meta_response",
  "status": "error",
  "message": "file exists"
}
```

The sender will print a warning; the user must re-enter a different path or type an empty line to exit.

## Notes / Guarantees

- **No disk writes on server** — server only forwards bytes and keeps minimal session metadata (in memory).
- **Resume** — receiver persists `<filename>.meta` (msgpack) with received chunk indices so transfers can resume
  precisely.
- **Chunk SHA-1** — every chunk carries a SHA-1 hex; receiver verifies per-chunk and NACKs mismatches.
- **Bounded memory** — sender uses a bounded queue (`QUEUE_MAXSIZE`) and a semaphore (`MAX_IN_FLIGHT`) to avoid huge
  in-memory buffering.
- **Retries** — network and nack/timeouts are retried with exponential backoff via `tenacity`. Configurable in
  `config.py`.
- **Single-file-per-send** — sender supports one file per send session; after successful send you can send another or
  press Enter to exit.
- **Cross-platform** — file writes use `aiofiles` and atomic `os.replace` to finalize files.

## Configuration via environment variables

All config values can be overridden using environment variables. Example:

```bash
export CHUNK_SIZE=$((2<<20))   # 2 MiB
export SERVER_PORT=9000
python server.py
```
