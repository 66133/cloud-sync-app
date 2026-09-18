#!/usr/bin/env bash
# ============================================================
# 短篇拆文助手 · 云端同步版 —— 裸机 Linux 一键部署
#
# 用法：把部署包解压到任意目录，在目录内执行：
#     sudo bash deploy.sh
#
# 做了什么：
#   1. 检查 Node.js >= 18（缺失时给出安装指引）
#   2. 代码安装到 /opt/story-sync（已有 data/ 数据完整保留）
#   3. 注册 systemd 服务（开机自启、崩溃 3 秒自动拉起）
#   4. 健康检查 + 输出后续 HTTPS 配置指引
# ============================================================
set -euo pipefail

APP_DIR="/opt/story-sync"
SERVICE_NAME="story-sync"
PORT="${PORT:-3000}"

c_green=$'\033[32m'; c_yellow=$'\033[33m'; c_red=$'\033[31m'; c_off=$'\033[0m'
info()  { echo "${c_green}[部署]${c_off} $*"; }
warn()  { echo "${c_yellow}[注意]${c_off} $*"; }
fail()  { echo "${c_red}[失败]${c_off} $*" >&2; exit 1; }

# ---------- 1. Node.js 检查 ----------
if ! command -v node >/dev/null 2>&1; then
  warn "未检测到 Node.js。安装方式任选其一："
  echo "  · Debian/Ubuntu:  curl -fsSL https://deb.nodesource.com/setup_22.x | sudo bash - && sudo apt-get install -y nodejs"
  echo "  · RHEL/CentOS:    curl -fsSL https://rpm.nodesource.com/setup_22.x | sudo bash - && sudo yum install -y nodejs"
  echo "  · 通用(nvm):      curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.1/install.sh | bash && nvm install 22"
  fail "请先安装 Node.js 18 及以上版本，再重跑本脚本"
fi

NODE_MAJOR="$(node -e 'console.log(Number(process.versions.node.split(".")[0]))')"
if [ "$NODE_MAJOR" -lt 18 ]; then
  fail "当前 Node.js 版本为 $(node -v)，需要 18 及以上（内置 fetch）。请升级后重试。"
fi
info "Node.js $(node -v) ✓"

# ---------- 2. 安装代码（保留历史数据） ----------
[ "$(id -u)" -eq 0 ] || fail "请用 sudo 运行（需要写入 /opt 与 systemd）"

systemctl stop "$SERVICE_NAME" 2>/dev/null || true
mkdir -p "$APP_DIR"
cp -r server.js package.json package-lock.json public "$APP_DIR/"
mkdir -p "$APP_DIR/data"
if [ -d data ] && [ -n "$(ls -A data 2>/dev/null || true)" ]; then
  info "检测到包内自带 data/ 数据，随包安装到 $APP_DIR/data"
  cp -r data/. "$APP_DIR/data/"
fi
info "代码已安装到 $APP_DIR（历史数据不会被覆盖）"

# ---------- 3. systemd 服务 ----------
NODE_BIN="$(command -v node)"
cat > "/etc/systemd/system/${SERVICE_NAME}.service" <<EOF
[Unit]
Description=短篇拆文助手·云端同步版 (story-sync)
After=network.target

[Service]
Type=simple
WorkingDirectory=${APP_DIR}
ExecStart=${NODE_BIN} server.js
Environment=PORT=${PORT}
Environment=DATA_DIR=${APP_DIR}/data
# 按需修改上游平台地址：
# Environment=UPSTREAM_ORIGIN=https://4m1wmbcu3t32z.aiforce.cloud
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now "$SERVICE_NAME"
info "systemd 服务已启动并设为开机自启"

# ---------- 4. 健康检查 ----------
sleep 2
if curl -sf "http://127.0.0.1:${PORT}/api/health" >/dev/null 2>&1; then
  info "服务运行正常：http://<本机IP>:${PORT}/  → 自动跳转应用"
else
  journalctl -u "$SERVICE_NAME" -n 30 --no-pager || true
  fail "健康检查未通过，请查看上方日志"
fi

# ---------- 5. 后续指引 ----------
cat <<EOF

${c_green}部署完成！${c_off}

  服务管理：
    systemctl status ${SERVICE_NAME}      # 查看状态
    systemctl restart ${SERVICE_NAME}     # 重启
    journalctl -u ${SERVICE_NAME} -f      # 看实时日志

  数据备份：${APP_DIR}/data 目录就是全部数据
    tar czf story-sync-backup.tgz -C ${APP_DIR} data

  手机/电脑公网访问还需要两步：
    1) 云服务器安全组放行 TCP ${PORT} 端口
    2) 强烈建议配 HTTPS（应用含登录态与长连接）：
       最简单方式 —— 安装 Caddy（自动申请续期证书）：
         sudo apt install -y caddy            # 或参考 caddyserver.com
         然后把 deploy/Caddyfile 里的域名改成你的，放到 /etc/caddy/
         （详见 deploy/Caddyfile 与 DEPLOY.md）
EOF
