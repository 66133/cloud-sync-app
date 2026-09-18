# 短篇拆文助手 · 云端同步版

原应用（`app_17e3ev426db`）是**纯前端应用**，全部数据存在浏览器本地的 IndexedDB 中，
因此手机和电脑各存一份、互不相通。本项目在不改动原应用任何一行业务代码的前提下，
在其存储层之下注入云端同步层，并配套一个带持久化存储的后端服务，
实现**同一个链接、多端实时同步、数据存云端**。

## 访问入口

- 应用主页：`/app/app_17e3ev426db/stats`（`/` 会自动跳转到这里）
- 手机、电脑打开同一链接即可，无需任何手动传文件

## 目录结构

```
cloud-sync-app/
├── server.js                  # 后端：静态托管 + 同步 API + SSE 推送 + 持久化 + 平台接口代理
├── package.json
├── data/                      # 数据目录（运行时生成，需持久化）
│   ├── snapshot.json          # 全量快照（原子写入）
│   └── oplog.jsonl            # 增量操作日志（快照后清空）
└── public/
    ├── app/index.html         # 改写后的应用入口（去除第三方监控、指向本地资源）
    ├── sync-shim.js           # 云端 IndexedDB 同步层（核心）
    ├── favicon.svg
    └── app_17e3ev426db/25/client/assets/   # 原应用静态资源（本地化）
```

## 工作原理

### 1. 云端 IndexedDB 同步层（`public/sync-shim.js`）

在应用加载前覆盖 `window.indexedDB`，实现原应用用到的 IndexedDB API 子集
（`open` / `transaction` / `objectStore` / `index`，以及 `put` `add` `get` `getAll`
`getAllKeys` `count` `delete` `clear` `createObjectStore` `createIndex` 等）：

- **读**：启动时从服务端 `bootstrap` 拉取全量状态到内存镜像，之后所有读操作走内存，速度与原版一致。
- **写**：本地立即生效并返回，同时把操作批量推送到服务端（约 150ms 合并，自动压缩同一记录的连续写入），
  服务端持久化后广播给其它设备。
- **实时**：SSE 长连接接收其它设备的写入；若 SSE 不可用自动降级为长轮询。
- **UI 更新**：远端变更后，在「页面可见 + 用户未在输入框内 + 本地改动已上送」时自动刷新；
  若正在写作编辑页，则改为右下角提示「云端有更新，点击查看」，绝不打断输入。
- **离线兜底**：服务端不可达时回退到本地真 IndexedDB 镜像，写入进入队列（localStorage 持久化），
  恢复联网后自动补传。

覆盖的原始数据库：
`story-analyzer-library`(records) · `story-creator-drafts`(drafts) ·
`story-creator-library-books`(books, chapters) · `story-creator-writing-stats`(logs)

### 2. 后端服务（`server.js`）

零 npm 依赖，仅用 Node 标准库。

| 接口 | 说明 |
| --- | --- |
| `GET /api/sync/bootstrap` | 返回全量状态与版本号（gzip 压缩） |
| `POST /api/sync/ops` | 接收一批写入操作，持久化并广播 |
| `GET /api/sync/events` | SSE 实时推送（新设备落后时直接补发快照） |
| `GET /api/sync/poll` | 长轮询降级方案 |
| `GET /api/sync/rev` | 仅返回当前版本号（用于可见性恢复时校验） |
| `GET /api/health` | 健康检查 |

**持久化策略**：写操作追加到 `oplog.jsonl`（保证不丢），每 400 次操作或每 30 秒
原子写入一次 `snapshot.json`；进程退出（SIGTERM/SIGINT）时落盘。

**AI 能力代理**：原应用的 AI 拆文（`story_analyzer_1`）、写作教练（`writing_coach_ai_generator_1`）
通过 `/app/<appId>/__runtime__/api/v1/plugin_server/capability/*` 调用原平台后端。
本服务原样反向代理这些请求（已验证原平台支持匿名调用），并做三层稳定性加固：

1. **立即写响应头 + SSE 保活**：上游 AI 首字延迟实测在 2~25s 之间随机波动（偶尔更久）。
   部署平台的入口网关对「超过约 60 秒无任何字节」的连接会直接掐断（表现为 `504 Gateway Time-out`）。
   因此对 SSE 类请求，代理**先向浏览器写 200 + SSE 响应头，并每 15 秒发一行 SSE 注释保活**，
   中间网关不会再因空闲而掐断连接；上游数据一到即原样透传。
   > 实测对照：静默 70s 的请求被网关掐断（连接中断），而带保活的 SSE 请求完整撑满 70.5s 正常返回。
2. **网关超时自动重试**：上游返回 502/504 或连接超时，自动重试一次（首字延迟随机，重试大概率落在快区间；
   AI 分析无副作用，重试安全，对浏览器完全透明）。
3. **流内错误反馈**：两次仍失败时，向 SSE 流内写一条 `type:error` 事件，由原前端自身的错误展示逻辑呈现，
   页面不会再出现网关级的 504 错误页。

非 SSE 的平台接口保持原样透传状态码。观测类上报接口本地静默响应，避免无效外呼。

## 本地运行

```bash
node server.js            # 默认 3000 端口
PORT=8080 node server.js  # 指定端口
```

## 环境变量

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `PORT` | `3000` | 监听端口（同时绑定 `0.0.0.0`） |
| `DATA_DIR` | `./data` | 数据目录，容器部署时建议挂载为持久卷 |
| `UPSTREAM_ORIGIN` | `https://4m1wmbcu3t32z.aiforce.cloud` | AI 能力上游平台地址 |

## 已知限制

- 多端**同时**编辑同一条草稿属于「后写覆盖」，未做字符级合并（与大多数在线文档工具的取舍一致）。
- 数据存在单个 `data` 目录，未做鉴权，**凡持有链接者共享同一份数据**。
- 上游 AI 服务的首字延迟本身波动较大（2~25s 常见），极慢时一次分析可能需要 1 分钟以上出结果；
  已通过保活 + 重试把「失败」降到最低，但**首字等待时间的长短由上游决定**，非本服务可控。
- 原应用（aiforce 链接）里的历史数据无法自动迁移到本服务：两个域名不同源，
  浏览器安全策略禁止跨站读取 IndexedDB。迁移方式见下。

## 数据迁移（把原应用里的历史数据搬过来）

在原应用（`https://4m1wmbcu3t32z.aiforce.cloud/app/app_17e3ev426db/...`）中：

1. 打开「拆文库」页面 → 点 **导出备份**，得到一个备份文件；
2. 在新链接（本服务）里打开「拆文库」页面 → 点 **导入备份**，选择该文件即可。

导入后数据即进入云端，多端自动同步。
