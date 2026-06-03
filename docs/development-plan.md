# DirSync - Directory Sync over Shared Memory

## 概述

`dirsync` 是一个基于共享内存（Shared Memory）的目录同步 CLI 工具。两个进程各自监控一个目录，通过共享内存交换文件变更事件，实现近实时的双向目录同步。

## 架构设计

### 整体架构

```mermaid
graph TB
    subgraph PA["进程 A (dirsync host -i dir1)"]
        WA[File Watcher - notify] --> CA[Change Detector]
        CA --> SWA[SHM Writer]
        SRA[SHM Reader] --> MA[Apply Changes to dir1]
    end

    subgraph SHM["共享内存 (Shared Memory)"]
        SHA[Ring Buffer A] --> SRB[SHM Reader B]
        SHB[Ring Buffer B] --> SRA2[SHM Reader A]
    end

    subgraph PB["进程 B (dirsync join -i dir2)"]
        WB[File Watcher - notify] --> CB[Change Detector]
        CB --> SWB[SHM Writer]
        SRB2[SHM Reader] --> MB[Apply Changes to dir2]
    end

    SWA --> SHA
    SWB --> SHB
    SRA2 --> SRA
    SRB --> SRB2

    style SHA fill:#f9f,stroke:#333,stroke-width:2px
    style SHB fill:#f9f,stroke:#333,stroke-width:2px
```

### 共享内存通信协议

```mermaid
graph LR
    subgraph SML["Shared Memory Layout"]
        H["Header<br/>magic · version · seq_a · seq_b<br/>lock_a · lock_b"]
        RB["Ring Buffer<br/>Event Queue"]
        DC["Data Chunk Pool<br/>File Content Blocks"]
    end

    H --> RB
    RB --> DC
```

### 模块结构

```mermaid
graph TD
    CLI[cli.rs<br/>命令行参数解析] --> MAIN[main.rs]
    MAIN --> SM[shm.rs<br/>共享内存管理]
    MAIN --> FW[watcher.rs<br/>文件系统监控]
    MAIN --> SYNC[sync.rs<br/>同步引擎]
    MAIN --> EVENT[event.rs<br/>事件定义与序列化]
    SM --> PROTO[protocol.rs<br/>通信协议]
    SYNC --> APPLY[apply.rs<br/>变更应用]
    SYNC --> HASH[hash.rs<br/>文件哈希计算]
    APPLY --> CONFLICT[conflict.rs<br/>冲突处理]

    style CLI fill:#e1f5fe
    style SM fill:#f3e5f5
    style FW fill:#e8f5e9
    style SYNC fill:#fff3e0
    style EVENT fill:#fce4ec
```

## 技术栈

| 组件 | Crate | 版本 | 用途 |
|------|-------|------|------|
| CLI 解析 | `clap` | 4.x | 命令行参数与子命令 |
| 文件监控 | `notify` | 8.x | 跨平台文件系统事件 |
| 共享内存 | `shared_memory` | 0.12 | 进程间共享内存 |
| 目录遍历 | `walkdir` | 2.x | 递归目录扫描 |
| 文件哈希 | `blake3` | 1.x | 快速文件内容哈希 |
| 序列化 | `bincode` + `serde` | 2.x / 1.x | 事件序列化 |
| 日志 | `tracing` | 0.1 | 结构化日志 |
| 异步运行时 | `tokio` | 1.x | 异步 IO 与定时器 |
| 错误处理 | `anyhow` | 1.x | 错误上下文 |

## 共享内存协议设计

### 内存布局

```
Offset  Size     Field
------  -------  ---------------------------
0x00    4        Magic Number (0x4453594E "DSYN")
0x04    4        Protocol Version
0x08    8        Sequence Number A (进程A写入计数)
0x10    8        Sequence Number B (进程B写入计数)
0x18    4        Lock A (原子操作)
0x1C    4        Lock B (原子操作)
0x20    4        Ring Buffer Write Offset
0x24    4        Ring Buffer Read Offset A
0x28    4        Ring Buffer Read Offset B
0x2C    4        Ring Buffer Capacity
0x30    4        Data Pool Write Offset
0x34    4        Data Pool Size
0x38    ...      Ring Buffer (事件队列)
...     ...      Data Pool (文件内容块)
```

### 事件类型

```rust
enum SyncEvent {
    FileCreated { path, content_hash, size },
    FileModified { path, content_hash, size },
    FileDeleted { path },
    DirCreated { path },
    DirDeleted { path },
    FileContent { path, offset, data },      // 大文件分块传输
    Heartbeat { timestamp },                  // 心跳检测
}
```

## 开发计划

### Phase 1: 基础框架 (Week 1)

```mermaid
gantt
    title Phase 1 - 基础框架
    dateFormat  YYYY-MM-DD
    section 依赖配置
    Cargo.toml 依赖配置           :a1, 2026-06-03, 1d
    section CLI
    clap 参数解析实现             :a2, after a1, 2d
    section 核心结构
    事件类型定义 (event.rs)       :a3, after a1, 1d
    错误处理框架                  :a4, after a1, 1d
    section 日志
    tracing 集成                  :a5, after a2, 1d
```

**任务清单:**
- [ ] 配置 `Cargo.toml` 所有依赖
- [ ] 实现 CLI 参数解析 (`host/join`, `-i <dir>`, `--shm-name`, `--shm-size`, `--verbose`)
- [ ] 定义 `SyncEvent` 枚举及序列化
- [ ] 集成 `anyhow` 错误处理
- [ ] 集成 `tracing` 日志

### Phase 2: 共享内存层 (Week 2)

```mermaid
gantt
    title Phase 2 - 共享内存层
    dateFormat  YYYY-MM-DD
    section 共享内存
    SHM 头部结构与原子操作        :b1, 2026-06-10, 2d
    Ring Buffer 实现              :b2, after b1, 3d
    section 序列化
    事件序列化/反序列化           :b3, 2026-06-10, 2d
    section 测试
    SHM 单元测试                  :b4, after b2, 2d
```

**任务清单:**
- [ ] 实现共享内存头部结构（magic, version, sequence, locks）
- [ ] 实现无锁 Ring Buffer（基于原子操作）
- [ ] 实现事件序列化到 Ring Buffer
- [ ] 实现事件反序列化从 Ring Buffer
- [ ] 大文件分块写入 Data Pool
- [ ] 单元测试：读写一致性

### Phase 3: 文件监控 (Week 3)

```mermaid
gantt
    title Phase 3 - 文件监控
    dateFormat  YYYY-MM-DD
    section 监控
    notify watcher 集成           :c1, 2026-06-17, 2d
    事件去抖动 (debounce)         :c2, after c1, 2d
    section 扫描
    初始目录扫描 (walkdir)        :c3, 2026-06-17, 2d
    增量变更检测                  :c4, after c3, 2d
    section 测试
    监控集成测试                  :c5, after c4, 1d
```

**任务清单:**
- [ ] 集成 `notify` crate 实现文件监控
- [ ] 实现事件去抖动（50-100ms 窗口）
- [ ] 实现初始目录全量扫描
- [ ] 实现增量变更检测（基于 mtime + blake3 hash）
- [ ] 过滤 `.git`, `node_modules` 等目录
- [ ] 集成测试

### Phase 4: 同步引擎 (Week 4)

```mermaid
gantt
    title Phase 4 - 同步引擎
    dateFormat  YYYY-MM-DD
    section 同步
    事件发送逻辑                  :d1, 2026-06-24, 2d
    事件接收与应用逻辑            :d2, after d1, 3d
    section 冲突
    冲突检测算法                  :d3, 2026-06-24, 2d
    冲突解决策略                  :d4, after d3, 2d
    section 测试
    双进程同步测试                :d5, after d4, 2d
```

**任务清单:**
- [ ] 实现事件发送（本地变更 → SHM）
- [ ] 实现事件接收（SHM → 应用到目标目录）
- [ ] 实现文件复制/创建/删除操作
- [ ] 实现冲突检测（双方同时修改同一文件）
- [ ] 实现冲突解决策略（last-write-wins / 保留双方副本）
- [ ] 双进程端到端测试

### Phase 5: 完善与发布 (Week 5)

```mermaid
gantt
    title Phase 5 - 完善与发布
    dateFormat  YYYY-MM-DD
    section 健壮性
    心跳与断线重连                :e1, 2026-07-01, 2d
    优雅退出与清理                :e2, after e1, 1d
    section 跨平台
    Windows 适配与测试            :e3, 2026-07-01, 2d
    Linux 适配与测试              :e4, after e3, 2d
    macOS 适配与测试              :e5, after e4, 1d
    section 文档
    README 编写                   :e6, after e2, 2d
    使用示例                      :e7, after e6, 1d
```

**任务清单:**
- [ ] 心跳机制（进程存活检测）
- [ ] 断线重连与状态恢复
- [ ] 优雅退出（SIGINT/SIGTERM 处理）
- [ ] 共享内存资源清理
- [ ] Windows / Linux / macOS 跨平台测试
- [ ] README 文档
- [ ] 使用示例与截图

## 核心流程

### 启动流程

```mermaid
sequenceDiagram
    participant User
    participant ProcessA as dirsync host -i dir1
    participant SHM as Shared Memory
    participant ProcessB as dirsync join -i dir2

    User->>ProcessA: dirsync host -i dir1
    ProcessA->>SHM: 创建/打开共享内存
    ProcessA->>ProcessA: 初始目录扫描
    ProcessA->>SHM: 写入初始快照事件
    ProcessA->>ProcessA: 启动文件监控

    User->>ProcessB: dirsync join -i dir2
    ProcessB->>SHM: 打开已有共享内存
    ProcessB->>ProcessB: 初始目录扫描
    ProcessB->>SHM: 读取 ProcessA 的事件
    ProcessB->>ProcessB: 应用变更到 dir2
    ProcessB->>SHM: 写入自己的快照事件
    ProcessB->>ProcessB: 启动文件监控

    loop 持续同步
        ProcessA->>SHM: 写入文件变更事件
        SHM->>ProcessB: 通知新事件
        ProcessB->>ProcessB: 应用变更
        ProcessB->>SHM: 写入确认/自己的事件
        SHM->>ProcessA: 通知新事件
        ProcessA->>ProcessA: 应用变更
    end
```

### 冲突处理流程

```mermaid
flowchart TD
    A[检测到同一文件双方修改] --> B{比较时间戳}
    B -->|A 更新| C[以 A 为准覆盖 B]
    B -->|B 更新| D[以 B 为准覆盖 A]
    B -->|几乎同时| E[比较 blake3 hash]
    E -->|内容相同| F[跳过, 无需同步]
    E -->|内容不同| G{冲突策略}
    G -->|last-write-wins| H[保留最后写入方]
    G -->|keep-both| I[保留两个副本<br/>file.txt.a<br/>file.txt.b]
    G -->|prompt| J[等待用户决策<br/>未来扩展]

    style A fill:#ffcdd2
    style C fill:#c8e6c9
    style D fill:#c8e6c9
    style F fill:#e1f5fe
    style I fill:#fff9c4
```

## 跨平台注意事项

| 平台 | 共享内存实现 | 文件监控 | 路径分隔符 |
|------|-------------|---------|-----------|
| Linux | `/dev/shm` (POSIX shm) | inotify | `/` |
| macOS | POSIX shm | FSEvents | `/` |
| Windows | Named File Mapping | ReadDirectoryChangesW | `\` |

## 性能目标

- 事件延迟: < 10ms（本地 SHM 读写）
- 大文件: 分块传输，支持 > 1GB 文件
- 目录规模: 支持 > 100,000 文件
- 内存占用: 默认 64MB 共享内存池

## 风险与对策

| 风险 | 影响 | 对策 |
|------|------|------|
| SHM 残留未清理 | 内存泄漏 | 注册退出钩子，强制清理 |
| 原子操作竞态 | 数据损坏 | 使用 `AtomicU64` + CAS |
| 符号链接循环 | 死循环 | 检测 symlink，限制深度 |
| 文件锁定 | 同步失败 | 重试机制 + 跳过策略 |
| 编码问题 | 路径乱码 | 统一使用 UTF-8 |
