# 部署指南 · 短篇拆文助手（云端同步版）

本应用是**单端口 Node.js 服务**（零 npm 依赖）+ **本地文件持久化**（`data/` 目录）。
任何满足以下三点的平台都能部署：

1. 能跑 Node.js 18+（或 Docker）；
2. 能持久化磁盘（保住 `data/` 目录）；
3. 允许 HTTP 长连接（SSE 实时同步、AI 流式输出都依赖它）。

> 数据 = `data/snapshot.json`（全量快照）+ `data/oplog.jsonl`（增量日志）。
> **只要这个目录在，数据就在**；迁移 = 拷走这个目录。

---

## 方案一：Docker / docker-compose（任何有 Docker 的机器）

```bash
# 解压部署包后，在项目目录内：
docker compose up -d
# 验证：
curl http://localhost:3000/api/health
```

- 数据落在宿主机 `./data`，删容器不丢；
- 改端口：编辑 `docker-compose.yml` 里 `ports` 左边的数字；
- 单用 `docker run`：`docker build -t story-sync . && docker run -d -p 3000:3000 -v $(pwd)/data:/app/data story-sync`

---

## 方案二：Zeabur（PaaS，国内可访问）

1. 注册 [zeabur.com](https://zeabur.com)，新建项目（区域可选香港/东京等）；
2. 把本目录推到一个 **GitHub 仓库**（或用 Zeabur CLI `zeabur deploy`）；
3. 项目里「创建服务 → Git」选中该仓库。Zeabur 识别到 `Dockerfile` 会自动构建；
4. **存储 → 挂载卷（Volume），路径填 `/app/data`** ← 不挂卷重启会丢数据；
5. 「网络 → 生成域名」得到 `xxx.zeabur.app` 公网地址。

> Zeabur 免费额度有限，超出后按量计费；卷为付费功能，个人用量每月几元级别。

---

## 方案三：Railway（PaaS，海外）

1. GitHub 登录 [railway.app](https://railway.app)，`New Project → Deploy from GitHub repo`；
2. 推送本目录到仓库后自动构建（自动识别 Dockerfile / `npm start`）；
3. **Settings → Volumes → 挂载到 `/app/data`**；
4. **Settings → Networking → Generate Domain** 得到公网地址。

> Railway 有一次性试用额度，之后 Hobby 计划 $5/月起。国内直连速度一般。

---

## 方案四：腾讯云 CloudBase 云托管（免备案域名、微信扫码开通）

1. 微信扫码登录 [console.cloud.tencent.com](https://console.cloud.tencent.com/tcb)（云开发 CloudBase）；
2. 开通环境 → 「云托管」→ 新建服务，区域选较近的（如上海/广州）；
3. 部署方式选「本地代码」，上传本目录的 zip 包（构建命令留空，监听端口 3000）；
4. **服务设置 → 挂载 CFS 文件存储，路径 `/app/data`**；
5. 服务详情 → 「访问服务」得到 `xxx.tcloudbaseapp.com` 域名（腾讯已备案，国内直接访问）。

---

## 方案五：自己的 Linux 服务器（完全可控）

```bash
# 解压部署包，进入目录：
sudo bash deploy.sh
```

脚本自动完成：Node 版本检查 → 安装到 `/opt/story-sync` → systemd 服务
（开机自启、崩溃自动拉起）→ 健康检查。历史数据完整保留。

手机/电脑公网访问还需：

1. 云控制台**安全组放行 TCP 3000**；
2. **配 HTTPS**（强烈建议）：
   - 最简单：装 [Caddy](https://caddyserver.com/docs/install)，改 `deploy/Caddyfile` 里的域名后覆盖 `/etc/caddy/Caddyfile`，`systemctl reload caddy` —— 证书全自动；
   - 已有 nginx：用 `deploy/nginx-story-sync.conf`，里面的 `proxy_buffering off` 和长超时是 SSE 的命门，别删。

---

## 环境变量

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `PORT` | `3000` | 监听端口（多数 PaaS 会自动注入） |
| `DATA_DIR` | `./data` | 数据目录，容器部署务必指向持久卷 |
| `UPSTREAM_ORIGIN` | `https://4m1wmbcu3t32z.aiforce.cloud` | 原平台地址（AI 能力上游）。仅当原应用换了域名才需要改 |

---

## 数据备份 / 迁移 / 恢复

| 场景 | 操作 |
| --- | --- |
| 日常备份 | 应用内「拆文库 → 导出备份」得到 JSON；或整目录 `tar czf backup.tgz data` |
| 迁移到新环境 | 停旧服务 → 拷 `data/` 目录到新环境对应位置 → 启动 |
| 从备份 JSON 恢复 | 任意实例打开应用 → 「拆文库 → 导入备份」 |
| 查看数据版本 | `curl http://<地址>/api/health`（返回 `rev` 与 `build`） |

---

## 健康检查清单（部署后逐项确认）

```bash
BASE=http://localhost:3000   # 换成实际地址
curl $BASE/api/health                      # {"ok":true,...}
curl -s -o /dev/null -w "%{http_code}\n" $BASE/app/app_17e3ev426db/stats   # 200
curl -s -o /dev/null -w "%{http_code}\n" $BASE/sync-shim.js                # 200
curl -m 4 -N "$BASE/api/sync/events?clientId=check"                        # 应打印 event: hello
```

手机、电脑打开 `https://你的域名/`（会自动跳转到应用），一端保存拆文，
另一端 10 秒内自动刷新，即部署成功。
