目标（Goal）：
实现一个内存中继（relay）服务与两个 CLI 客户端，用于在内网中通过服务器 C 中转，使 A 与 B（A↔C、B↔C 可通信，A↔B 不通）能相互传输文件。中继服务器
C 不可将文件写入磁盘：所有文件数据必须仅在服务器内存中转发。支持断点续传、并行分块、每块 SHA-1 校验、自动重试与多用户并发会话。

基本环境与依赖：

- Python >= 3.12。请使用 3.12 的现代类型提示（例如内置泛型 list[int] 等）。
- 必要依赖（确保用最新 API）：
    - loguru
    - msgpack
    - tenacity
    - tqdm
    - websockets (使用最新 async ClientConnection API)
    - aiofiles
- 配置必须集中在 `config.py`（server 地址/端口、chunk_size、并发数、队列大小、TTL、重试策略、最大会话等）。

文件划分：

- config.py：所有可调配置（包含默认值和环境变量覆盖说明）。
- common.py：共享数据结构（dataclass）、消息打包/解包、sha1 工具、类型定义、常用异常等。
- server.py：中继服务（HTTP/WebSocket），仅内存保留最小元数据（允许短期内存元数据），绝不写入磁盘。负责生成 4
  位会话码、维护会话映射、转发消息、心跳与 TTL 回收。
- receiver.py：接收端 CLI。连接 server，接收并展示进度条，写入 `<filename>.tmp` 同时维护 `<filename>.meta` 以支持
  resume；传输完成后删除 `.meta` 并把 `.tmp` 重命名为真实文件名。所有用户提示与日志均为英文。
- sender.py：发送端 CLI。用户通过 4 位码连接目标接收端，输入单文件路径（只支持单文件一次性传送）。路径输入需 strip
  两端空白，并在双引号或单引号两侧同时存在时去除它们。若输入为空行则结束会话并退出。
- README.md：使用说明和示例启动命令（Server / Receiver / Sender）。

主要行为与协议（使用 msgpack，以二进制 websocket 帧发送）：

- 会话建立：
    1. Receiver 连接到 Server，Server 下发唯一 4 位 code（字符串形式，如 "1234"）给 Receiver。
    2. Server 在内存保存 session 信息（receiver websocket、last_active timestamp 等），若双方在 30 分钟内无消息交互则释放
       session。
    3. Sender 使用该 4 位 code 与 Server 建立连接，Server 将 Sender 与对应 Receiver 匹配并做转发桥接。
- 消息类型（msgpack 映射到 dict）示例：
    - Session assignment: `{ "type": "session", "code": "1234" }`
    - Meta（发送方先发）：
      `{ "type":"meta", "file_id":"<uuid>", "filename":"foo.bin", "size": int, "chunk_size": int, "chunks": int }`
    - Meta response（接收端返回 resume info 或 error）：  
      `{ "type":"meta_response", "file_id":"<uuid>", "status":"ok", "missing_chunks":[int,...] }`  
      或 `{ "type":"meta_response", "status":"error", "message":"file exists" }`
    - Chunk：`{ "type":"chunk", "file_id":"<uuid>", "index": int, "sha1": str, "payload": bytes }`
    - Ack / Nack：`{ "type":"ack","file_id":..., "index": ... }` 或
      `{ "type":"nack","file_id":..., "index":..., "reason":"..." }`
    - Finish：`{ "type":"finish", "file_id":..., "status":"success" }`
    - Error：`{ "type":"error","message":"..." }`
- 传输流程：
    1. Sender 发 `meta`。Receiver 检查本地是否已存在同名完整文件或 `.tmp`/`.meta` 部分文件。
        - 如果完整文件已存在：Receiver 回复 `meta_response` 带 `status:"error"` 和 `message:"file exists"`。Server 转发到
          Sender。Sender 必须提示用户（英文）并强制要求用户重新输入新路径或取消（不得自动重命名）。
        - 如果存在 `.tmp` 与 `.meta`：Receiver 使用本地 `<filename>.meta`（持久化小文件，记录每个 chunk 是否已接收/校验通过的
          bitmap 或索引集合）计算 `missing_chunks` 并在 `meta_response` 返回；这样实现精确 resume（不是仅基于文件大小）。
        - 如果不存在：Receiver 返回 `missing_chunks` 为全量索引（0..chunks-1）。
    2. Sender 根据 `missing_chunks` 只发送缺失的 chunk。Sender 读取文件分块并将每个 chunk 放入受限大小的 `asyncio.Queue`
       （避免大量内存占用），并由并发 worker（数量由 config 决定）从队列取出发送。
    3. 每个 chunk 发送时附带 `sha1`（hex）。Receiver 写入 `<filename>.tmp` 指定偏移，并对写入的 chunk 计算 sha1，与收到的
       sha1 对比。若通过，Receiver 更新 `<filename>.meta` 标记该 index 已接收，发送 `ack`；若不匹配，发送 `nack`，并 Receiver
       删除/覆盖该 chunk（以保证下次接收时能写入正确内容）。
    4. Sender 对每个 chunk 的发送/重试使用 tenacity（指数回退 + 最大重试次数，可在 config 中配置）。对网络发送失败、nack 或
       sha1 不匹配要重试有限次数后报错给用户。
    5. 传输完成：所有 chunk 都被 ack，Receiver 做整体校验（仅校验文件字节长度等于 meta 中声明的 size）；若校验通过，Receiver
       删除 `<filename>.meta` 并把 `<filename>.tmp` 原子重命名为 `<filename>`，并向 Sender 发送 `finish` 成功消息。若校验失败，发送
       error。
- 文件与元数据格式：
    - 临时文件：`<filename>.tmp`（用于写入分块数据，支持并发按偏移写入）
    - 元数据：`<filename>.meta`（存放每个 chunk 的接收状态 bitmap 或已接收索引列表、声明的 chunk_size、total
      chunks、文件总大小、file_id。该文件允许写入磁盘，Receiver 必须持久化它以支持断点续传。）
    - 不允许 Server 写任何文件到磁盘；Server 仅在内存中保持 session 与转发相关的最小元数据（file_id -> chunks count 等临时
      info）。
- 内存与流控（backpressure）：
    - 使用有界 `asyncio.Queue`（默认 `queue_maxsize=64`）在 Sender 端限制未发送 chunk 缓存，避免一次性读入过多数据。
    - 使用 `asyncio.Semaphore` 控制并发 in-flight chunk 数量（默认 `max_in_flight=4`）。
    - Receiver 对写文件操作使用 file-level `asyncio.Lock` 以避免并发写冲突（跨平台注意 Windows 的文件写入行为）。
- 配置默认值（place in config.py，可被环境变量覆盖）：
    - CHUNK_SIZE = 1 << 20 # 1 MiB
    - MAX_IN_FLIGHT = 4
    - QUEUE_MAXSIZE = 64
    - SESSION_TTL_SECONDS = 30 * 60 # 30 minutes inactivity (sender & receiver both quiet)
    - MAX_SESSIONS = 50
    - TENACITY_MAX_ATTEMPTS = 5
    - TENACITY_BACKOFF_BASE = 0.5
    - LOG_LEVEL = "INFO"
    - SERVER_HOST = "0.0.0.0"
    - SERVER_PORT = 8765
- Tenacity 使用场景：
    - websocket send/recv wrapper（短暂错误重试）
    - chunk 网络发送函数（遇到 nack 或网络异常时）
    - 可选的 file IO 重试（写入 meta/tmp 时用以应对文件锁/IO 短暂失败）
- 日志与 CLI 文案：
    - 所有运行时提示、进度、错误、警告必须为英文（方便多语言环境下一致），日志使用 loguru。CLI 显示进度条使用 tqdm。
    - 需要在 README 中给出示例英文交互文本（send/receive 的提示样例）。
- 平台兼容性：
    - 需在 Windows 与 Linux 下兼容测试（文件路径处理、文件写入偏移与锁、子进程/线程模型差异）。
- 安全与网络：
    - 仅在内网，不启用 TLS
- 测试建议：
    - 编写单元测试覆盖：chunk 切分、sha1 计算/校验、meta 文件读写/解析、resume bitmap 逻辑、基本的 sender-receiver
      小规模集成测试（可在同一进程中运行 server、fake receiver 和 fake sender）。
- 代码质量：
    - 全部函数需有精确 Type Hints；使用 dataclasses 描述消息/元数据；遵循最佳工程实践（清晰错误码、异常处理、文档字符串）。
- 失败与用户交互约定：
    - 当 Receiver 返回 `file exists`：Sender 必须提示用户（英文），并强制用户重新输入文件路径或输入 `cancel` 退出该会话。
    - 如果 Sender 在中途输入空行（第一次回车即空）：当前连接结束，Sender 与 Receiver 关闭连接，Server 释放该 4 位 code。
