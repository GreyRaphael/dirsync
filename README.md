# dirsync

基于共享内存的目录同步 CLI 工具。两个进程通过共享内存环形缓冲区交换文件变更事件，实现零网络开销的实时目录同步。

## 特性

- **共享内存传输** — 事件通过 SHM 环形缓冲区交换，无网络延迟
- **实时监控** — 使用操作系统原生文件监控（inotify / FSEvents / ReadDirectoryChangesW）
- **增量同步** — 仅传输变更文件，使用 blake3 内容哈希校验完整性
- **大文件分块** — 自动将大文件切分为多个块传输
- **冲突检测** — 检测同时修改；`--conflict last-write-wins` 按时间戳决定胜者，`--conflict keep-both` 保留两个版本
- **事件防抖** — 在可配置的时间窗口内合并连续的文件系统事件
- **心跳机制** — 每 5 秒发送心跳，15 秒无响应判定对端离线
- **沙箱兼容** — 支持 Windows Sandbox 等隔离环境（角色感知的 SHM 初始化）

## 安装

```bash
cargo install --path .
```

或从源码构建：

```bash
cargo build --release
```

## 使用方法

打开两个终端，分别指向要同步的目录：

```bash
# 终端 1 — 必须先启动 host
dirsync host -i /path/to/directory-a

# 终端 2 — 再启动 join（会自动等待 host 的共享内存段出现）
dirsync join -i /path/to/directory-b
```

> **重要**：host 必须先启动，因为它负责创建共享内存段。join 进程会重试等待 host 的 SHM 段就绪后再连接。这一设计确保了在 Windows Sandbox 等沙箱环境中正常工作（沙箱内可以看到 host 创建的内核对象，反之不行）。

两侧目录中的变更会自动双向同步。

### 命令行选项

```
用法: dirsync <COMMAND>

命令:
  host  启动同步对中的第一个进程（创建共享内存段）
  join  加入已有的同步对（连接到 host 创建的共享内存段）

选项:
  -i, --input <INPUT>              要监控和同步的目录
      --shm-name <SHM_NAME>        共享内存段名称 [默认: dirsync_shm]
      --shm-size <SHM_SIZE>        共享内存大小（字节） [默认: 67108864]
  -v, --verbose...                 详细输出（-v, -vv, -vvv）
      --conflict <CONFLICT>        冲突模式: last-write-wins | keep-both [默认: last-write-wins]
      --debounce-ms <DEBOUNCE_MS>  防抖间隔（毫秒） [默认: 100]
      --ignore <IGNORE>            忽略的目录（可重复指定）
  -h, --help                       打印帮助
  -V, --version                    打印版本
```

### 示例

```bash
# 开启详细日志
dirsync host -i ./project-a -v

# 自定义共享内存名称（用于多组同步）
dirsync host -i ./docs-a --shm-name docs_sync
dirsync join -i ./docs-b --shm-name docs_sync

# 忽略 node_modules 和 .git
dirsync host -i ./src-a --ignore node_modules --ignore .git
dirsync join -i ./src-b --ignore node_modules --ignore .git

# 使用 keep-both 冲突策略
dirsync host -i ./work-a --conflict keep-both
dirsync join -i ./work-b --conflict keep-both
```

### 冲突策略

| 策略 | 行为 |
|------|------|
| `last-write-wins` | 比较远端事件时间戳与本地文件修改时间，较新的一方胜出 |
| `keep-both` | 将本地副本重命名为 `<名称>.local.<扩展名>`，接受远端版本 |

## 架构

```
┌─────────────────┐     共享内存 (SHM)      ┌─────────────────┐
│   进程 A        │    ┌──────────────┐      │   进程 B        │
│                 │    │  环形缓冲区   │      │                 │
│  ┌───────────┐  │    │  ┌────────┐  │      │  ┌───────────┐  │
│  │ Watcher   │──┼───>│  │ Events │  │<─────┼──│ Applier   │  │
│  └───────────┘  │    │  └────────┘  │      │  └───────────┘  │
│       │         │    └──────────────┘      │       │         │
│  ┌───────────┐  │                          │  ┌───────────┐  │
│  │ Chunker   │  │                          │ │ Conflict   │  │
│  └───────────┘  │                          │ │ Detector   │  │
└─────────────────┘                          └─────────────────┘
```

### 模块结构

| 模块 | 职责 |
|------|------|
| `cli` | 命令行参数解析（clap） |
| `event` | SyncEvent 类型定义和 SHM 头部布局 |
| `shm` | 共享内存传输层，含环形缓冲区和自旋锁 |
| `watcher` | 文件系统监控，含防抖和状态跟踪 |
| `chunker` | 大文件分块传输 |
| `apply` | 将远端事件应用到本地文件系统 |
| `sync` | 主同步引擎（心跳、冲突检测、事件循环） |

### 共享内存布局

```
偏移     大小   字段
0x00     4      魔数 "DSYN"
0x04     4      协议版本
0x08     8      序列号 A / B
0x18     4      自旋锁 A / B
0x20     4      环形缓冲区写游标
0x24     4      环形缓冲区读游标 A
0x28     4      环形缓冲区读游标 B
0x2C     4      环形缓冲区容量
0x38     ...    环形缓冲区数据区（循环，带环绕哨兵）
```

### 同步流程

1. **初始扫描** — 扫描目录，推送 `FileCreated` + `FileContent` 事件
2. **监控循环** — 通过防抖机制监控文件系统变更
3. **推送** — 将事件序列化写入 SHM 环形缓冲区
4. **消费** — 读取远端事件，应用到本地文件系统
5. **心跳** — 每 5 秒发送一次，15 秒无响应判定对端离线

### SHM 角色机制

在沙箱环境（如 Windows Sandbox）中，内核命名对象存在隔离：

- **host 创建的对象 → join 可见**（继承 host 命名空间）
- **join 创建的对象 → host 不可见**（join 私有命名空间）

因此 dirsync 采用角色感知的 SHM 初始化：

- `host` 命令始终负责创建或打开 SHM 段
- `join` 命令只尝试打开，如果 host 尚未启动则持续重试

SHM 名称会自动按平台归一化：Linux/macOS 上添加 `/` 前缀（POSIX `shm_open` 要求），Windows 上去掉 `/` 前缀（避免成为名称的一部分）。

## 开发

```bash
# 检查编译
cargo clippy -- -D warnings

# 运行全部测试（单元 + 集成）
cargo test

# 带调试日志运行
RUST_LOG=debug cargo run -- host -i ./test-dir -v
```

## 许可证

MIT
