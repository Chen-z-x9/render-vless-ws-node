# Render / 本地 VLESS-WS 节点方案

这个目录当前保存的是一套可本地运行、也便于后续迁移到 `Render` 的 `VLESS + WebSocket + TLS` 节点代码。

它最初从 `Cloudflare Workers` 方案演进而来，所以仓库里仍保留了 `worker` 相关文件，方便回溯。

## 当前方案特点

- 平台：本地 `Node.js` 服务，可迁移到 `Render`
- 入站：`VLESS + WebSocket + TLS`
- 域名：本地测试可配任意反代入口；迁移后可使用 `onrender.com`
- 客户端：适合导入 `v2rayN`

## 当前参数

- `VLESS_UUID`: `30625464-78e5-4785-a466-5649b8a7b18f`
- `WS_PATH`: `/ws-tuug99w001ckb21l`

## 你需要准备（本地运行）

1. 本机可运行 `Node.js`
2. 一个可用的入口层（例如 `Tailscale Funnel` 或后续的 `Render`）
3. 如果你要把它当真正翻墙节点，服务所在环境本身必须能访问目标外网

## 项目结构

- `src/server.js`: 当前本地 / Render 版 Node 服务主逻辑
- `src/worker.js`: Worker 主逻辑
- `wrangler.toml`: Cloudflare Workers 配置
- `.dev.vars.example`: 本地环境变量示例
- `scripts/make-vless-link.sh`: 生成 `v2rayN` 导入链接
- `scripts/start-local-vless.sh`: 使用内置 Node 启动本地服务

## 本地启动

### 1. 安装依赖

```bash
cd /Users/chenzx/Downloads/cloudflare-vless-worker
npm install
```

### 2. 启动本地服务

```bash
bash /Users/chenzx/Downloads/cloudflare-vless-worker/scripts/start-local-vless.sh
```

启动成功后，根路径应返回：

```json
{
  "ok": true,
  "service": "tailscale-vless-server",
  "websocketPath": "/ws-tuug99w001ckb21l"
}
```

## Render 迁移入口

- 启动命令：

```bash
node src/server.js
```

- 关键环境变量：

- `VLESS_UUID`: `30625464-78e5-4785-a466-5649b8a7b18f`
- `WS_PATH`: `/ws-tuug99w001ckb21l`

## Koyeb / Render 流量统计

当前 `src/server.js` 已支持服务端流量统计，并暴露两个受保护的接口：

- `/stats`
- `/stats.json`

要启用持久化统计，需要额外配置以下环境变量：

- `UPSTASH_REDIS_REST_URL`
- `UPSTASH_REDIS_REST_TOKEN`
- `STATS_USER`
- `STATS_PASS`

可选环境变量：

- `STATS_ENABLED=true`
- `STATS_FLUSH_INTERVAL_MS=5000`
- `CONNECT_TIMEOUT_MS=10000`
- `FIRST_MESSAGE_TIMEOUT_MS=10000`
- `MAX_WEBSOCKET_PAYLOAD_BYTES=2097152`
- `MAX_PENDING_UPLINK_BYTES=4194304`

说明：

- `/stats` 返回浏览器可直接查看的 HTML 页面
- `/stats.json` 返回 JSON
- 统计的是节点转发的上下行字节数
- Redis 未配置时，仍可统计，但只保存在内存里，重启后会清零
- 建议给 Mac 和手机都使用同一个节点，这样总量能在服务端统一统计
- 服务端会限制 WebSocket 单帧和待发送队列，并对目标连接设置超时，避免低内存实例因异常或突发流量失去健康状态

更完整的迁移说明见：

`/Users/chenzx/Downloads/Render-海外节点迁移方案.md`

## v2rayN 参数

- 地址：部署后的公网域名
- 端口：`443`
- 用户 ID：`VLESS_UUID`
- 加密：`none`
- 传输协议：`ws`
- 路径：`WS_PATH`
- TLS：开启
- SNI：公网域名
- Host：公网域名
- Fingerprint：`chrome`

## 说明

- 访问 `https://你的域名/` 应返回一个 `ok: true` 的 JSON。
- 真正的节点入口是 `wss://你的域名<WS_PATH>`。
- 这个版本只实现 `TCP` 转发，不做 `UDP` 和 `Reality`。
- `src/server.js` 是当前主版本；`src/worker.js` 仅保留作历史方案参考。
