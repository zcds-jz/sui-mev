# 仓库指南

## 1. 项目概览
本仓库是一个 Rust Workspace，实现 Sui MEV 套利系统，核心包括套利机器人（`arb`）与交易中继（`relay`）。  
工作区入口见 `Cargo.toml`，Rust 工具链固定为 `1.81`（`rust-toolchain.toml`）。

## 2. 目录说明（按文件夹）
- `bin/`：可执行程序目录。
- `bin/arb/`：套利主程序。
- `bin/arb/src/main.rs`：CLI 入口，包含 `start-bot`、`run`、`pool-ids` 三个子命令。
- `bin/arb/src/start_bot.rs`：生产模式主流程，挂载 Collectors / Strategies / Executors。
- `bin/arb/src/collector.rs`：公共交易、本地私有交易（relay WS）采集器。
- `bin/arb/src/strategy/`：机会识别、缓存与多 worker 调度。
- `bin/arb/src/defi/`：各 DEX 协议适配与交易构建逻辑。
- `bin/relay/`：中继服务，接收 gRPC 交易并通过 WS 广播。
- `crates/`：共享库目录。
- `crates/dex-indexer/`：池子索引、协议池缓存、按币种检索池子。
- `crates/simulator/`：交易模拟抽象与实现（DB/HTTP/Replay）。
- `crates/shio/`：Shio feed 连接、竞价提交执行器。
- `crates/logger/`：日志初始化与模块白名单过滤。
- `crates/utils/`：通用工具（panic hook、coin/telegram/link 等）。
- `crates/object-pool/`：对象池封装，用于模拟器池复用。
- `crates/version/`：构建版本号宏（基于 git 信息）。
- `crates/arb-common/`：预留公共库（当前内容较少）。
- `scripts/`：运维脚本（如 `restart_bot.py`、`monitor_profit.py`）。
- `target/`：编译产物目录，禁止提交。
- `.idea/`：IDE 配置目录，避免放业务配置。

## 3. 常用命令
- `cargo check --workspace`：全量编译检查（推荐提交前必跑）。
- `cargo build --workspace --release`：构建发布版本。
- `cargo test --workspace`：运行工作区测试。
- `cargo fmt --all`：统一格式化。
- `cargo run -r --bin arb start-bot -- --private-key <KEY>`：启动套利机器人。
- `cargo run -r --bin relay`：启动中继服务。

## 4. 配置与环境变量
- 核心：`SUI_PRIVATE_KEY`、`SUI_RPC_URL`。
- 数据源：`SUI_TX_SOCKET_PATH`、`--relay-ws-url`、`--shio-ws-url`。
- 模拟器：`--use-db-simulator`、`SUI_DB_PATH`、`SUI_CONFIG_PATH`、`SUI_PRELOAD_PATH`。
- 建议本地用 `export` 或启动脚本注入，不要把敏感值写入仓库。

## 5. 开发与提交流程
- 代码风格遵循 `rustfmt.toml`（120 列、导入整理等）。
- 命名：模块/函数 `snake_case`，类型 `PascalCase`，常量 `UPPER_SNAKE_CASE`。
- 提交信息建议：`<scope>: <action>`，示例：`arb: fix relay collector retry`。
- PR 至少包含：变更目的、影响范围、验证命令与结果、回滚方式。

## 6. 安全与运维注意
- 严禁提交私钥、RPC 凭据、Telegram token。
- `logs/`、`target/`、`venv/` 等运行产物不应入库。
- 涉及端口（如 9000/9001）先确认未被本机其他进程占用。
