import { connect } from "cloudflare:sockets";

const WS_UPGRADE = "websocket";

export default {
  async fetch(request, env, ctx) {
    const uuid = normalizeUuid(env.VLESS_UUID);
    const wsPath = normalizePath(env.WS_PATH || "/ws");
    const url = new URL(request.url);

    if (!uuid) {
      return json(
        {
          ok: false,
          error: "VLESS_UUID is missing"
        },
        500
      );
    }

    if (request.headers.get("Upgrade") === WS_UPGRADE && url.pathname === wsPath) {
      return handleVlessOverWebSocket(request, uuid, ctx);
    }

    if (url.pathname === "/") {
      return json({
        ok: true,
        service: "cloudflare-vless-worker",
        websocketPath: wsPath
      });
    }

    return new Response("Not found", { status: 404 });
  }
};

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

function json(payload, status = 200) {
  return new Response(JSON.stringify(payload, null, 2), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8"
    }
  });
}

async function handleVlessOverWebSocket(request, uuid, ctx) {
  const webSocketPair = new WebSocketPair();
  const [client, server] = Object.values(webSocketPair);
  server.accept();

  const readable = webSocketToReadableStream(server);

  ctx.waitUntil(
    proxyVlessSession({
      readable,
      server,
      uuid
    }).catch((error) => {
      console.error("session failed", error);
      safeCloseWebSocket(server, 1011, "session failed");
    })
  );

  return new Response(null, {
    status: 101,
    webSocket: client
  });
}

async function proxyVlessSession({ readable, server, uuid }) {
  const reader = readable.getReader();
  const firstChunk = await reader.read();

  if (firstChunk.done || !firstChunk.value || firstChunk.value.byteLength === 0) {
    throw new Error("missing VLESS header");
  }

  const parsed = parseVlessHeader(firstChunk.value, uuid);
  if (parsed.command !== 0x01) {
    throw new Error(`unsupported VLESS command: ${parsed.command}`);
  }

  const socket = connect({
    hostname: parsed.hostname,
    port: parsed.port
  });

  const writer = socket.writable.getWriter();
  try {
    if (parsed.initialPayload.byteLength > 0) {
      await writer.write(parsed.initialPayload);
    }
  } finally {
    writer.releaseLock();
  }

  const serverToClient = pipeSocketToWebSocket(socket.readable, server, parsed.version);
  const clientToServer = pipeReaderToSocket(reader, socket.writable);

  await Promise.race([serverToClient, clientToServer]);
  safeCloseWebSocket(server, 1000, "done");
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
  const command = bytes[commandIndex];
  const port = (bytes[commandIndex + 1] << 8) | bytes[commandIndex + 2];
  const addressType = bytes[commandIndex + 3];

  let addressIndex = commandIndex + 4;
  let hostname = "";

  if (addressType === 0x01) {
    hostname = Array.from(bytes.slice(addressIndex, addressIndex + 4)).join(".");
    addressIndex += 4;
  } else if (addressType === 0x02) {
    const length = bytes[addressIndex];
    addressIndex += 1;
    hostname = new TextDecoder().decode(bytes.slice(addressIndex, addressIndex + length));
    addressIndex += length;
  } else if (addressType === 0x03) {
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

  return {
    version,
    command,
    hostname,
    port,
    initialPayload: bytes.slice(addressIndex)
  };
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

function webSocketToReadableStream(webSocket) {
  return new ReadableStream({
    start(controller) {
      webSocket.addEventListener("message", (event) => {
        const data = event.data;
        if (data instanceof ArrayBuffer) {
          controller.enqueue(new Uint8Array(data));
          return;
        }
        if (ArrayBuffer.isView(data)) {
          controller.enqueue(new Uint8Array(data.buffer, data.byteOffset, data.byteLength));
          return;
        }
        if (typeof data === "string") {
          controller.enqueue(new TextEncoder().encode(data));
        }
      });

      webSocket.addEventListener("close", () => {
        controller.close();
      });

      webSocket.addEventListener("error", (event) => {
        controller.error(event);
      });
    }
  });
}

async function pipeReaderToSocket(reader, writable) {
  const writer = writable.getWriter();
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) {
        break;
      }
      if (value && value.byteLength > 0) {
        await writer.write(value);
      }
    }
  } finally {
    try {
      await writer.close();
    } catch {
      // Ignore close errors when the remote side has already closed.
    }
    writer.releaseLock();
  }
}

async function pipeSocketToWebSocket(readable, webSocket, version) {
  const reader = readable.getReader();
  let sentHeader = false;

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) {
        break;
      }
      if (!value || value.byteLength === 0) {
        continue;
      }

      if (!sentHeader) {
        const chunk = new Uint8Array(2 + value.byteLength);
        chunk[0] = version;
        chunk[1] = 0;
        chunk.set(value, 2);
        webSocket.send(chunk);
        sentHeader = true;
      } else {
        webSocket.send(value);
      }
    }
  } finally {
    reader.releaseLock();
  }
}

function safeCloseWebSocket(webSocket, code, reason) {
  try {
    if (webSocket.readyState === WS_READY_OPEN || webSocket.readyState === WS_READY_CLOSING) {
      webSocket.close(code, reason);
    }
  } catch {
    // Ignore close errors.
  }
}

const WS_READY_OPEN = 1;
const WS_READY_CLOSING = 2;
