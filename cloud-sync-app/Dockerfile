# 短篇拆文助手 · 云端同步版 —— 通用容器镜像
# 适用于：Zeabur / Railway / 腾讯云 CloudBase / 任何自建 Docker 环境
#
# 唯一关键点：容器内数据写在 /app/data，部署平台务必把它挂载为持久卷（Volume），
# 否则容器重建后数据会丢失。
FROM node:22-alpine

WORKDIR /app

# 零 npm 依赖，只拷源码与静态资源（package.json 仅为声明 start 脚本）
COPY package.json package-lock.json ./
COPY server.js ./
COPY public ./public

ENV NODE_ENV=production \
    PORT=3000 \
    DATA_DIR=/app/data

# 运行时自动创建数据目录
RUN mkdir -p /app/data

EXPOSE 3000
VOLUME ["/app/data"]

CMD ["node", "server.js"]
