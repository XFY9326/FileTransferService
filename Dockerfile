FROM python:3.12-slim

ENV SERVER_LISTEN_HOST=0.0.0.0
ENV SERVER_LISTEN_PORT=8765
ENV LOG_LEVEL=INFO
ENV SESSION_TTL_SECONDS=3600
ENV MAX_SESSIONS=32

COPY --from=docker.io/astral/uv:latest /uv /uvx /bin/

WORKDIR /app

COPY pyproject.toml uv.lock ./

RUN uv sync \
 && rm -rf ~/.cache/uv \
 && rm -rf /root/.cache/uv \
 && rm -rf /tmp/uv-cache

COPY . .

EXPOSE 8765/tcp

CMD ["uv", "run", "src/fts/server.py"]
