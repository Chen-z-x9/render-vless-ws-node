
import http from "node:http";
import { timingSafeEqual } from "node:crypto";
import net from "node:net";
import { WebSocketServer } from "ws";

const port = Number.parseInt(process.env.PORT || "3000", 10);
const host = process.env.HOST || "0.0.0.0";
const uuid = normalizeUuid(process.env.VLESS_UUID);
const wsPath = normalizePath(process.env.WS_PATH || "/ws");
const statsConfig = createStatsConfig(process.env);
const statsTracker = createStatsTracker(statsConfig);
const connectTimeoutMs = normalizePositiveInteger(process.env.CONNECT_TIMEOUT_MS, 10000);
const firstMessageTimeoutMs = normalizePositiveInteger(process.env.FIRST_MESSAGE_TIMEOUT_MS, 10000);
const maxWebSocketPayloadBytes = normalizePositiveInteger(
  process.env.MAX_WEBSOCKET_PAYLOAD_BYTES,
  2 * 1024 * 1024
);
const maxPendingUplinkBytes = normalizePositiveInteger(
  process.env.MAX_PENDING_UPLINK_BYTES,
  4 * 1024 * 1024
);

if (!uuid) {
  console.error("VLESS_UUID is missing");
  process.exit(1);
}

const server = http.createServer(async (request, response) => {
  const url = new URL(request.url || "/", `http://${request.headers.host || "localhost"}`);

  if (url.pathname === "/") {
    response.writeHead(200, {
      "content-type": "application/json; charset=utf-8"
    });
    response.end(
      JSON.stringify(
        {
          ok: true,
          service: "tailscale-vless-server",
          websocketPath: wsPath
        },
        null,
        2
      )
    );
    return;
  }

  if (url.pathname === "/stats" || url.pathname === "/stats.json") {
    if (!statsConfig.enabled) {
      response.writeHead(404, {
        "content-type": "text/plain; charset=utf-8"
      });
      response.end("Not found");
      return;
    }

    if (!statsConfig.authConfigured) {
      response.writeHead(503, {
        "content-type": "application/json; charset=utf-8"
      });
      response.end(JSON.stringify({ ok: false, error: "stats auth is not configured" }, null, 2));
      return;
    }

    if (!isAuthorizedStatsRequest(request, statsConfig)) {
      response.writeHead(401, {
        "content-type": "text/plain; charset=utf-8",
        "www-authenticate": 'Basic realm="traffic-stats"'
      });
      response.end("Authentication required");
      return;
    }

    try {
      const snapshot = await statsTracker.getSnapshot();

      if (url.pathname === "/stats.json") {
        response.writeHead(200, {
          "content-type": "application/json; charset=utf-8",
          "cache-control": "no-store"
        });
        response.end(JSON.stringify(snapshot, null, 2));
        return;
      }

      response.writeHead(200, {
        "content-type": "text/html; charset=utf-8",
        "cache-control": "no-store"
      });
      response.end(renderStatsPage(snapshot));
      return;
    } catch (error) {
      console.error("stats failed", error);
      response.writeHead(503, {
        "content-type": "application/json; charset=utf-8",
        "cache-control": "no-store"
      });
      response.end(JSON.stringify({ ok: false, error: "failed to read traffic stats" }, null, 2));
      return;
    }
  }

  response.writeHead(404, {
    "content-type": "text/plain; charset=utf-8"
  });
  response.end("Not found");
});

const wsServer = new WebSocketServer({
  noServer: true,
  perMessageDeflate: false,
  maxPayload: maxWebSocketPayloadBytes
});

server.on("upgrade", (request, socket, head) => {
  const url = new URL(request.url || "/", `http://${request.headers.host || "localhost"}`);

  if (url.pathname !== wsPath) {
    socket.destroy();
    return;
  }

  wsServer.handleUpgrade(request, socket, head, (webSocket) => {
    wsServer.emit("connection", webSocket, request);
  });
});

wsServer.on("connection", (webSocket) => {
  statsTracker.sessionOpened();
  webSocket.once("close", () => {
    statsTracker.sessionClosed();
  });

  handleVlessSession(webSocket, uuid, statsTracker).catch((error) => {
    console.error("session failed", error);
    safeCloseWebSocket(webSocket);
  });
});

server.listen(port, host, () => {
  console.log(`tailscale-vless-server listening on http://${host}:${port}`);
  console.log(`websocket path: ${wsPath}`);
  if (statsConfig.enabled) {
    console.log(`stats path: /stats`);
    console.log(`stats storage: ${statsConfig.redisConfigured ? "upstash-redis" : "memory-only"}`);
  }
});

function normalizeUuid(value) {
  if (!value) {
    return "";
  }
  return String(value).trim().toLowerCase();
}

function normalizePath(value) {
  if (!value) {
    return "/ws";
  }
  return value.startsWith("/") ? value : `/${value}`;
}

async function handleVlessSession(webSocket, expectedUuid, tracker) {
  const firstChunk = await readFirstMessage(webSocket, firstMessageTimeoutMs);
  const parsed = parseVlessHeader(firstChunk, expectedUuid);

  if (parsed.command !== 0x01) {
    throw new Error(`unsupported VLESS command: ${parsed.command}`);
  }

  const remoteSocket = net.connect({
    host: parsed.hostname,
    port: parsed.port
  });
  remoteSocket.setNoDelay(true);

  await onceConnected(remoteSocket, connectTimeoutMs);

  if (webSocket.readyState !== webSocket.OPEN) {
    remoteSocket.destroy();
    throw new Error("websocket closed before remote socket was ready");
  }

  let pendingUplinkBytes = 0;
  let remoteBackpressured = false;
  const pendingUplink = [];

  const flushPendingUplink = () => {
    remoteBackpressured = false;
    while (!remoteSocket.destroyed && pendingUplink.length > 0) {
      const chunk = pendingUplink.shift();
      pendingUplinkBytes -= chunk.length;
      const canContinue = remoteSocket.write(chunk);
      if (!canContinue) {
        remoteBackpressured = true;
        return;
      }
    }
  };

  const writeUplink = (chunk) => {
    if (!remoteBackpressured && pendingUplink.length === 0) {
      remoteBackpressured = !remoteSocket.write(chunk);
      return;
    }

    pendingUplink.push(chunk);
    pendingUplinkBytes += chunk.length;
    if (pendingUplinkBytes > maxPendingUplinkBytes) {
      remoteSocket.destroy(new Error("uplink buffer limit exceeded"));
      safeCloseWebSocket(webSocket);
    }
  };

  remoteSocket.on("drain", flushPendingUplink);

  webSocket.send(Buffer.from([parsed.version, 0]));

  if (parsed.initialPayload.byteLength > 0) {
    writeUplink(parsed.initialPayload);
    tracker.recordUplink(parsed.initialPayload.byteLength);
  }

  remoteSocket.on("data", (chunk) => {
    if (!Buffer.isBuffer(chunk) || chunk.length === 0) {
      return;
    }

    if (webSocket.readyState !== webSocket.OPEN) {
      remoteSocket.destroy();
      return;
    }

    remoteSocket.pause();
    tracker.recordDownlink(chunk.length);
    webSocket.send(chunk, { binary: true }, (error) => {
      if (error) {
        remoteSocket.destroy(error);
        return;
      }
      if (!remoteSocket.destroyed) {
        remoteSocket.resume();
      }
    });
  });

  remoteSocket.on("close", () => {
    safeCloseWebSocket(webSocket);
  });

  remoteSocket.on("error", (error) => {
    console.error("remote socket failed", error);
    safeCloseWebSocket(webSocket);
  });

  webSocket.on("message", (data, isBinary) => {
    if (!isBinary) {
      return;
    }

    const chunk = normalizeBinary(data);
    if (chunk.length > 0 && !remoteSocket.destroyed) {
      tracker.recordUplink(chunk.length);
      writeUplink(chunk);
    }
  });

  webSocket.on("close", () => {
    remoteSocket.destroy();
  });

  webSocket.on("error", (error) => {
    console.error("websocket failed", error);
    remoteSocket.destroy();
  });
}

function readFirstMessage(webSocket, timeoutMs) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      cleanup();
      reject(new Error("timed out waiting for VLESS header"));
    }, timeoutMs);

    const onMessage = (data, isBinary) => {
      cleanup();
      if (!isBinary) {
        reject(new Error("expected binary websocket frame"));
        return;
      }
      resolve(normalizeBinary(data));
    };

    const onClose = () => {
      cleanup();
      reject(new Error("websocket closed before VLESS header"));
    };

    const onError = (error) => {
      cleanup();
      reject(error);
    };

    const cleanup = () => {
      clearTimeout(timer);
      webSocket.off("message", onMessage);
      webSocket.off("close", onClose);
      webSocket.off("error", onError);
    };

    webSocket.once("message", onMessage);
    webSocket.once("close", onClose);
    webSocket.once("error", onError);
  });
}

function onceConnected(socket, timeoutMs) {
  return new Promise((resolve, reject) => {
    if (socket.readyState === "open") {
      resolve();
      return;
    }

    const timer = setTimeout(() => {
      cleanup();
      socket.destroy();
      reject(new Error("remote connection timed out"));
    }, timeoutMs);

    const onConnect = () => {
      cleanup();
      resolve();
    };

    const onError = (error) => {
      cleanup();
      reject(error);
    };

    const cleanup = () => {
      clearTimeout(timer);
      socket.off("connect", onConnect);
      socket.off("error", onError);
    };

    socket.once("connect", onConnect);
    socket.once("error", onError);
  });
}

function normalizeBinary(data) {
  if (Buffer.isBuffer(data)) {
    return data;
  }

  if (Array.isArray(data)) {
    return Buffer.concat(data.map((chunk) => Buffer.from(chunk)));
  }

  return Buffer.from(data);
}

function parseVlessHeader(chunk, expectedUuid) {
  const bytes = chunk instanceof Uint8Array ? chunk : new Uint8Array(chunk);
  if (bytes.byteLength < 24) {
    throw new Error("invalid VLESS header length");
  }

  const version = bytes[0];
  const receivedUuid = bytesToUuid(bytes.slice(1, 17));
  if (receivedUuid !== expectedUuid) {
    throw new Error("UUID mismatch");
  }

  const optionLength = bytes[17];
  const commandIndex = 18 + optionLength;
  ensureAvailable(bytes, commandIndex, 4, "VLESS command header");
  const command = bytes[commandIndex];
  const port = (bytes[commandIndex + 1] << 8) | bytes[commandIndex + 2];
  const addressType = bytes[commandIndex + 3];

  let addressIndex = commandIndex + 4;
  let hostname = "";

  if (addressType === 0x01) {
    ensureAvailable(bytes, addressIndex, 4, "IPv4 address");
    hostname = Array.from(bytes.slice(addressIndex, addressIndex + 4)).join(".");
    addressIndex += 4;
  } else if (addressType === 0x02) {
    ensureAvailable(bytes, addressIndex, 1, "domain length");
    const length = bytes[addressIndex];
    addressIndex += 1;
    ensureAvailable(bytes, addressIndex, length, "domain address");
    hostname = new TextDecoder().decode(bytes.slice(addressIndex, addressIndex + length));
    addressIndex += length;
  } else if (addressType === 0x03) {
    ensureAvailable(bytes, addressIndex, 16, "IPv6 address");
    const segments = [];
    for (let index = 0; index < 8; index += 1) {
      const left = bytes[addressIndex + index * 2].toString(16).padStart(2, "0");
      const right = bytes[addressIndex + index * 2 + 1].toString(16).padStart(2, "0");
      segments.push(`${left}${right}`);
    }
    hostname = segments.join(":");
    addressIndex += 16;
  } else {
    throw new Error(`unsupported address type: ${addressType}`);
  }

  if (!hostname || port === 0) {
    throw new Error("invalid VLESS target");
  }

  return {
    version,
    command,
    hostname,
    port,
    initialPayload: Buffer.from(bytes.slice(addressIndex))
  };
}

function ensureAvailable(bytes, start, length, label) {
  if (start < 0 || length < 0 || start + length > bytes.byteLength) {
    throw new Error(`invalid ${label}`);
  }
}

function bytesToUuid(bytes) {
  const hex = Array.from(bytes, (value) => value.toString(16).padStart(2, "0")).join("");
  return [
    hex.slice(0, 8),
    hex.slice(8, 12),
    hex.slice(12, 16),
    hex.slice(16, 20),
    hex.slice(20, 32)
  ].join("-");
}

function safeCloseWebSocket(webSocket) {
  try {
    if (webSocket.readyState === webSocket.OPEN || webSocket.readyState === webSocket.CLOSING) {
      webSocket.close();
    }
  } catch {
  }
}

function createStatsConfig(env) {
  const enabled = parseBoolean(env.STATS_ENABLED ?? "true");
  const restUrl = String(env.UPSTASH_REDIS_REST_URL || "").trim().replace(/\/+$/, "");
  const restToken = String(env.UPSTASH_REDIS_REST_TOKEN || "").trim();
  const authUser = String(env.STATS_USER || "").trim();
  const authPass = String(env.STATS_PASS || "").trim();
  const flushIntervalMs = normalizePositiveInteger(env.STATS_FLUSH_INTERVAL_MS, 5000);

  return {
    enabled,
    restUrl,
    restToken,
    authUser,
    authPass,
    authConfigured: authUser.length > 0 && authPass.length > 0,
    redisConfigured: restUrl.length > 0 && restToken.length > 0,
    flushIntervalMs
  };
}

function createStatsTracker(config) {
  const redis = createUpstashRedisClient(config);
  const state = {
    pendingUplink: 0,
    pendingDownlink: 0,
    inflightUplink: 0,
    inflightDownlink: 0,
    memoryTotalUplink: 0,
    memoryTotalDownlink: 0,
    memoryMonthId: getCurrentMonthId(),
    memoryMonthUplink: 0,
    memoryMonthDownlink: 0,
    activeSessions: 0,
    totalSessions: 0,
    lastFlushAt: null,
    lastFlushError: "",
    flushPromise: null
  };

  const timer =
    config.enabled && redis.enabled && config.flushIntervalMs > 0
      ? setInterval(() => {
          void flush();
        }, config.flushIntervalMs)
      : null;

  timer?.unref?.();

  function recordUplink(bytes) {
    const size = normalizeByteCount(bytes);
    if (size === 0) {
      return;
    }
    rotateMonthIfNeeded(state);
    state.pendingUplink += size;
    state.memoryTotalUplink += size;
    state.memoryMonthUplink += size;
  }

  function recordDownlink(bytes) {
    const size = normalizeByteCount(bytes);
    if (size === 0) {
      return;
    }
    rotateMonthIfNeeded(state);
    state.pendingDownlink += size;
    state.memoryTotalDownlink += size;
    state.memoryMonthDownlink += size;
  }

  function sessionOpened() {
    state.activeSessions += 1;
    state.totalSessions += 1;
  }

  function sessionClosed() {
    state.activeSessions = Math.max(0, state.activeSessions - 1);
  }

  async function getSnapshot() {
    rotateMonthIfNeeded(state);

    if (!config.enabled) {
      return {
        ok: false,
        enabled: false
      };
    }

    const monthId = getCurrentMonthId();
    const pendingUplink = state.pendingUplink + state.inflightUplink;
    const pendingDownlink = state.pendingDownlink + state.inflightDownlink;

    let totalUplink = state.memoryTotalUplink;
    let totalDownlink = state.memoryTotalDownlink;
    let monthUplink = monthId === state.memoryMonthId ? state.memoryMonthUplink : 0;
    let monthDownlink = monthId === state.memoryMonthId ? state.memoryMonthDownlink : 0;
    let storage = "memory";

    if (redis.enabled) {
      try {
        const values = await redis.mget([
          trafficKey("total", "uplink"),
          trafficKey("total", "downlink"),
          trafficKey(monthId, "uplink"),
          trafficKey(monthId, "downlink")
        ]);

        totalUplink = values[0] + pendingUplink;
        totalDownlink = values[1] + pendingDownlink;
        monthUplink = values[2] + pendingUplink;
        monthDownlink = values[3] + pendingDownlink;
        storage = "redis";
      } catch (error) {
        state.lastFlushError = error instanceof Error ? error.message : String(error);
        storage = "memory-fallback";
      }
    }

    return {
      ok: true,
      service: "tailscale-vless-server",
      websocketPath: wsPath,
      month: monthId,
      storage,
      redisConfigured: redis.enabled,
      activeSessions: state.activeSessions,
      totalSessionsSinceStart: state.totalSessions,
      pending: {
        uplink: pendingUplink,
        downlink: pendingDownlink,
        total: pendingUplink + pendingDownlink,
        uplinkHuman: formatBytes(pendingUplink),
        downlinkHuman: formatBytes(pendingDownlink),
        totalHuman: formatBytes(pendingUplink + pendingDownlink)
      },
      currentMonth: createTrafficBlock(monthUplink, monthDownlink),
      totals: createTrafficBlock(totalUplink, totalDownlink),
      lastFlushAt: state.lastFlushAt,
      lastFlushError: state.lastFlushError || null,
      updatedAt: new Date().toISOString()
    };
  }

  async function flush() {
    rotateMonthIfNeeded(state);

    if (!redis.enabled) {
      return;
    }

    if (state.flushPromise) {
      return state.flushPromise;
    }

    const monthId = getCurrentMonthId();
    const deltaUplink = state.pendingUplink;
    const deltaDownlink = state.pendingDownlink;

    if (deltaUplink === 0 && deltaDownlink === 0) {
      return;
    }

    state.pendingUplink = 0;
    state.pendingDownlink = 0;
    state.inflightUplink += deltaUplink;
    state.inflightDownlink += deltaDownlink;

    state.flushPromise = redis
      .pipeline(
        [
          ["INCRBY", trafficKey("total", "uplink"), deltaUplink],
          ["INCRBY", trafficKey("total", "downlink"), deltaDownlink],
          ["INCRBY", trafficKey(monthId, "uplink"), deltaUplink],
          ["INCRBY", trafficKey(monthId, "downlink"), deltaDownlink]
        ].filter((command) => Number(command[2]) > 0)
      )
      .then(() => {
        state.lastFlushAt = new Date().toISOString();
        state.lastFlushError = "";
      })
      .catch((error) => {
        state.pendingUplink += deltaUplink;
        state.pendingDownlink += deltaDownlink;
        state.lastFlushError = error instanceof Error ? error.message : String(error);
      })
      .finally(() => {
        state.inflightUplink = Math.max(0, state.inflightUplink - deltaUplink);
        state.inflightDownlink = Math.max(0, state.inflightDownlink - deltaDownlink);
        state.flushPromise = null;
      });

    return state.flushPromise;
  }

  async function close() {
    if (timer) {
      clearInterval(timer);
    }
    await flush();
  }

  return {
    recordUplink,
    recordDownlink,
    sessionOpened,
    sessionClosed,
    getSnapshot,
    flush,
    close
  };
}

function createUpstashRedisClient(config) {
  if (!config.redisConfigured) {
    return { enabled: false };
  }

  const headers = {
    authorization: `Bearer ${config.restToken}`,
    "content-type": "application/json"
  };

  async function call(pathname, body) {
    const response = await fetch(`${config.restUrl}${pathname}`, {
      method: "POST",
      headers,
      body: JSON.stringify(body)
    });

    const payload = await response.json();

    if (!response.ok) {
      throw new Error(payload?.error || `Upstash request failed with ${response.status}`);
    }

    return payload;
  }

  return {
    enabled: true,
    async mget(keys) {
      const payload = await call("", ["MGET", ...keys]);
      const values = Array.isArray(payload?.result) ? payload.result : [];
      return keys.map((_, index) => parseRedisInteger(values[index]));
    },
    async pipeline(commands) {
      if (!Array.isArray(commands) || commands.length === 0) {
        return [];
      }

      const payload = await call("/pipeline", commands);

      if (!Array.isArray(payload)) {
        throw new Error("Unexpected Upstash pipeline response");
      }

      const firstError = payload.find((item) => item?.error);
      if (firstError?.error) {
        throw new Error(firstError.error);
      }

      return payload;
    }
  };
}

function trafficKey(scope, direction) {
  if (scope === "total") {
    return `traffic:total:${direction}`;
  }
  return `traffic:month:${scope}:${direction}`;
}

function createTrafficBlock(uplink, downlink) {
  const total = uplink + downlink;
  return {
    uplink,
    downlink,
    total,
    uplinkHuman: formatBytes(uplink),
    downlinkHuman: formatBytes(downlink),
    totalHuman: formatBytes(total)
  };
}

function rotateMonthIfNeeded(state) {
  const monthId = getCurrentMonthId();
  if (state.memoryMonthId === monthId) {
    return;
  }

  state.memoryMonthId = monthId;
  state.memoryMonthUplink = 0;
  state.memoryMonthDownlink = 0;
}

function getCurrentMonthId() {
  return new Date().toISOString().slice(0, 7);
}

function normalizePositiveInteger(value, fallback) {
  const parsed = Number.parseInt(String(value || ""), 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function normalizeByteCount(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return 0;
  }
  return Math.floor(parsed);
}

function parseBoolean(value) {
  const normalized = String(value).trim().toLowerCase();
  return !["0", "false", "no", "off"].includes(normalized);
}

function parseRedisInteger(value) {
  if (typeof value === "number" && Number.isFinite(value)) {
    return Math.floor(value);
  }

  if (typeof value === "string" && value.trim().length > 0) {
    const parsed = Number.parseInt(value, 10);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }

  return 0;
}

function isAuthorizedStatsRequest(request, config) {
  const header = request.headers.authorization || "";
  if (!header.startsWith("Basic ")) {
    return false;
  }

  const encoded = header.slice("Basic ".length).trim();

  let decoded = "";
  try {
    decoded = Buffer.from(encoded, "base64").toString("utf8");
  } catch {
    return false;
  }

  const separatorIndex = decoded.indexOf(":");
  if (separatorIndex < 0) {
    return false;
  }

  const user = decoded.slice(0, separatorIndex);
  const pass = decoded.slice(separatorIndex + 1);

  return secureEquals(user, config.authUser) && secureEquals(pass, config.authPass);
}

function secureEquals(left, right) {
  const leftBuffer = Buffer.from(String(left));
  const rightBuffer = Buffer.from(String(right));

  if (leftBuffer.length !== rightBuffer.length) {
    return false;
  }

  return timingSafeEqual(leftBuffer, rightBuffer);
}

function renderStatsPage(snapshot) {
  const title = "VLESS Traffic Stats";
  const storageLabel =
    snapshot.storage === "redis"
      ? "Upstash Redis"
      : snapshot.storage === "memory-fallback"
        ? "Memory fallback"
        : "Memory only";

  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>${escapeHtml(title)}</title>
  <style>
    :root { color-scheme: dark; }
    body { margin: 0; background: #0b1020; color: #e5e7eb; font: 16px/1.5 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
    main { max-width: 900px; margin: 0 auto; padding: 32px 20px 48px; }
    h1 { margin: 0 0 8px; font-size: 32px; }
    p { margin: 0; color: #94a3b8; }
    .grid { display: grid; gap: 16px; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); margin-top: 24px; }
    .card { background: #111827; border: 1px solid #1f2937; border-radius: 16px; padding: 18px; }
    .label { font-size: 13px; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.08em; }
    .value { margin-top: 10px; font-size: 28px; font-weight: 700; color: #f8fafc; }
    .meta { margin-top: 8px; font-size: 14px; color: #94a3b8; }
    table { width: 100%; border-collapse: collapse; margin-top: 24px; background: #111827; border-radius: 16px; overflow: hidden; }
    th, td { padding: 14px 16px; border-bottom: 1px solid #1f2937; text-align: left; }
    th { width: 180px; color: #94a3b8; font-weight: 600; }
    tr:last-child th, tr:last-child td { border-bottom: 0; }
    .warn { color: #fca5a5; }
    code { color: #bfdbfe; }
  </style>
</head>
<body>
  <main>
    <h1>${escapeHtml(title)}</h1>
    <p>Month: ${escapeHtml(snapshot.month)} · Storage: ${escapeHtml(storageLabel)}</p>
    <div class="grid">
      <section class="card">
        <div class="label">This Month</div>
        <div class="value">${escapeHtml(snapshot.currentMonth.totalHuman)}</div>
        <div class="meta">Up ${escapeHtml(snapshot.currentMonth.uplinkHuman)} · Down ${escapeHtml(snapshot.currentMonth.downlinkHuman)}</div>
      </section>
      <section class="card">
        <div class="label">All Time</div>
        <div class="value">${escapeHtml(snapshot.totals.totalHuman)}</div>
        <div class="meta">Up ${escapeHtml(snapshot.totals.uplinkHuman)} · Down ${escapeHtml(snapshot.totals.downlinkHuman)}</div>
      </section>
      <section class="card">
        <div class="label">Pending Flush</div>
        <div class="value">${escapeHtml(snapshot.pending.totalHuman)}</div>
        <div class="meta">Up ${escapeHtml(snapshot.pending.uplinkHuman)} · Down ${escapeHtml(snapshot.pending.downlinkHuman)}</div>
      </section>
      <section class="card">
        <div class="label">Sessions</div>
        <div class="value">${escapeHtml(String(snapshot.activeSessions))}</div>
        <div class="meta">Started since boot: ${escapeHtml(String(snapshot.totalSessionsSinceStart))}</div>
      </section>
    </div>
    <table>
      <tr><th>Stats JSON</th><td><code>/stats.json</code></td></tr>
      <tr><th>WebSocket Path</th><td><code>${escapeHtml(snapshot.websocketPath)}</code></td></tr>
      <tr><th>Last Flush</th><td>${escapeHtml(snapshot.lastFlushAt || "not flushed yet")}</td></tr>
      <tr><th>Updated At</th><td>${escapeHtml(snapshot.updatedAt)}</td></tr>
      <tr><th>Flush Error</th><td class="${snapshot.lastFlushError ? "warn" : ""}">${escapeHtml(snapshot.lastFlushError || "none")}</td></tr>
    </table>
  </main>
</body>
</html>`;
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function formatBytes(bytes) {
  const size = normalizeByteCount(bytes);
  if (size === 0) {
    return "0 B";
  }

  const units = ["B", "KB", "MB", "GB", "TB"];
  const exponent = Math.min(Math.floor(Math.log(size) / Math.log(1024)), units.length - 1);
  const value = size / 1024 ** exponent;
  const digits = value >= 100 || exponent === 0 ? 0 : value >= 10 ? 1 : 2;
  return `${value.toFixed(digits)} ${units[exponent]}`;
}

for (const signal of ["SIGINT", "SIGTERM"]) {
  process.on(signal, () => {
    void statsTracker.close().finally(() => {
      server.close(() => {
        process.exit(0);
      });
      setTimeout(() => process.exit(0), 2000).unref();
    });
  });
}
