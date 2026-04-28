import http from "node:http";
import net from "node:net";
import { WebSocketServer } from "ws";

const port = Number.parseInt(process.env.PORT || "3000", 10);
const host = process.env.HOST || "0.0.0.0";
const uuid = normalizeUuid(process.env.VLESS_UUID);
const wsPath = normalizePath(process.env.WS_PATH || "/ws");

if (!uuid) {
  console.error("VLESS_UUID is missing");
  process.exit(1);
}

const server = http.createServer((request, response) => {
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

  response.writeHead(404, {
    "content-type": "text/plain; charset=utf-8"
  });
  response.end("Not found");
});

const wsServer = new WebSocketServer({ noServer: true });

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
  handleVlessSession(webSocket, uuid).catch((error) => {
    console.error("session failed", error);
    safeCloseWebSocket(webSocket);
  });
});

server.listen(port, host, () => {
  console.log(`tailscale-vless-server listening on http://${host}:${port}`);
  console.log(`websocket path: ${wsPath}`);
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

async function handleVlessSession(webSocket, expectedUuid) {
  const firstChunk = await readFirstMessage(webSocket);
  const parsed = parseVlessHeader(firstChunk, expectedUuid);

  if (parsed.command !== 0x01) {
    throw new Error(`unsupported VLESS command: ${parsed.command}`);
  }

  const remoteSocket = net.connect({
    host: parsed.hostname,
    port: parsed.port
  });

  await onceConnected(remoteSocket);

  if (parsed.initialPayload.byteLength > 0) {
    remoteSocket.write(parsed.initialPayload);
  }

  let sentHeader = false;

  remoteSocket.on("data", (chunk) => {
    if (!Buffer.isBuffer(chunk) || chunk.length === 0) {
      return;
    }

    if (webSocket.readyState !== webSocket.OPEN) {
      return;
    }

    if (!sentHeader) {
      webSocket.send(Buffer.concat([Buffer.from([parsed.version, 0]), chunk]));
      sentHeader = true;
      return;
    }

    webSocket.send(chunk);
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
      remoteSocket.write(chunk);
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

function readFirstMessage(webSocket) {
  return new Promise((resolve, reject) => {
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
      webSocket.off("message", onMessage);
      webSocket.off("close", onClose);
      webSocket.off("error", onError);
    };

    webSocket.once("message", onMessage);
    webSocket.once("close", onClose);
    webSocket.once("error", onError);
  });
}

function onceConnected(socket) {
  return new Promise((resolve, reject) => {
    if (socket.readyState === "open") {
      resolve();
      return;
    }

    const onConnect = () => {
      cleanup();
      resolve();
    };

    const onError = (error) => {
      cleanup();
      reject(error);
    };

    const cleanup = () => {
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
    initialPayload: Buffer.from(bytes.slice(addressIndex))
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

function safeCloseWebSocket(webSocket) {
  try {
    if (webSocket.readyState === webSocket.OPEN || webSocket.readyState === webSocket.CLOSING) {
      webSocket.close();
    }
  } catch {
    // Ignore close errors.
  }
}
