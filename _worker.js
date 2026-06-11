// 核心配置文件：轻量稳定版 VLESS 隧道（带一键导入/订阅功能）

export default {
  async fetch(request, env, ctx) {
    try {
      const upgradeHeader = request.headers.get('Upgrade');
      const url = new URL(request.url);
      
      // 1. 判断是否为客户端的 WebSocket 连接请求
      if (upgradeHeader === 'websocket') {
        return await vlessOverWebSocketHandler(request, env, ctx);
      }
      
      const userID = env.UUID || "d3b4a2e1-7c98-4b5a-9f12-e6d8c3a7b4f5";
      const hostName = request.headers.get('Host');

      // 2. 新增：一键导入与订阅路径 (例如：访问 你的域名/sub)
      if (url.pathname === '/sub' || url.pathname === '/sub/') {
        // 生成标准的 VLESS 分享链接
        const vlessLink = `vless://${userID}@${hostName}:443?encryption=none&security=tls&sni=${hostName}&type=ws&host=${hostName}&path=%2F#CF_Custom_Node`;
        
        // 将链接转换为 v2rayN 通用的 Base64 订阅格式
        const base64Sub = btoa(vlessLink);

        // 如果用户在浏览器里带了参数，比如 /sub?raw，直接吐出明文链接
        if (url.searchParams.has('raw')) {
          return new Response(vlessLink, {
            headers: { 'Content-Type': 'text/plain; charset=utf-8' }
          });
        }

        // 默认返回 Base64 编码，方便 v2rayN 直接作为订阅地址添加
        return new Response(base64Sub, {
          headers: {
            'Content-Type': 'text/plain; charset=utf-8',
            'Cache-Control': 'no-store, no-cache, must-revalidate',
          }
        });
      }
      
      // 3. 完美的网页伪装：如果不是连接也不是订阅，继续假装是 Bing 首页
      const mockUrl = env.PROXY_URL || 'https://www.bing.com';
      const targetUrl = new URL(url.pathname + url.search, mockUrl);
      const modifiedRequest = new Request(targetUrl, request);
      return await fetch(modifiedRequest);

    } catch (err) {
      return new Response(`边缘节点发生错误: ${err.message}`, { status: 500 });
    }
  }
};

/**
 * 处理 VLESS Over WebSocket 的核心流转发
 */
async function vlessOverWebSocketHandler(request, env, ctx) {
  const webSocketPair = new WebSocketPair();
  const [client, server] = Object.values(webSocketPair);
  server.accept();

  let remoteSocket = null;
  const userID = env.UUID || "d3b4a2e1-7c98-4b5a-9f12-e6d8c3a7b4f5";

  server.addEventListener('message', async (event) => {
    try {
      if (!remoteSocket) {
        const buffer = event.data;
        if (!(buffer instanceof ArrayBuffer)) {
          server.close(1003, " need ArrayBuffer");
          return;
        }

        const view = new DataView(buffer);
        if (buffer.byteLength < 24) {
          server.close(1002, "Header too short");
          return;
        }

        const version = view.getUint8(0);
        const uuidChunks = [];
        for (let i = 1; i <= 16; i++) {
          uuidChunks.push(view.getUint8(i).toString(16).padStart(2, '0'));
        }
        const clientUUID = `${uuidChunks.slice(0,4).join('')}-${uuidChunks.slice(4,6).join('')}-${uuidChunks.slice(6,8).join('')}-${uuidChunks.slice(8,10).join('')}-${uuidChunks.slice(10,16).join('')}`;

        if (clientUUID !== userID) {
          server.close(1002, "Auth failed");
          return;
        }

        const addonLength = view.getUint8(17);
        const targetPort = view.getUint16(19 + addonLength);
        const addressType = view.getUint8(21 + addonLength);

        let targetHost = "";
        let addressStart = 22 + addonLength;

        if (addressType === 1) {
          targetHost = [view.getUint8(addressStart), view.getUint8(addressStart + 1), view.getUint8(addressStart + 2), view.getUint8(addressStart + 3)].join('.');
          addressStart += 4;
        } else if (addressType === 2) {
          const domainLength = view.getUint8(addressStart);
          const decoder = new TextDecoder();
          targetHost = decoder.decode(new Uint8Array(buffer, addressStart + 1, domainLength));
          addressStart += 1 + domainLength;
        } else if (addressType === 3) {
          const ipv6Chunks = [];
          for (let i = 0; i < 8; i++) {
            ipv6Chunks.push(view.getUint16(addressStart + i * 2).toString(16));
          }
          targetHost = `[${ipv6Chunks.join(':')}]`;
          addressStart += 16;
        }

        const rawPayload = buffer.slice(addressStart);
        
        // @ts-ignore
        remoteSocket = connect({ hostname: targetHost, port: targetPort });
        ctx.waitUntil(ctxDotPipe(remoteSocket, server));

        const writer = remoteSocket.writable.getWriter();
        if (rawPayload.byteLength > 0) {
          await writer.write(rawPayload);
        }
        writer.releaseLock();

        const responseHeader = new Uint8Array([version, 0]);
        server.send(responseHeader);

      } else {
        const writer = remoteSocket.writable.getWriter();
        await writer.write(event.data);
        writer.releaseLock();
      }
    } catch (error) {
      server.close(1011, `Error: ${error.message}`);
      if (remoteSocket) remoteSocket.close();
    }
  });

  server.addEventListener('close', () => { if (remoteSocket) remoteSocket.close(); });
  server.addEventListener('error', () => { if (remoteSocket) remoteSocket.close(); });

  return new Response(null, { status: 101, webSocket: client });
}

async function ctxDotPipe(remoteSocket, server) {
  try {
    const reader = remoteSocket.readable.getReader();
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      server.send(value);
    }
  } catch (err) {
  } finally {
    try {
      server.close();
      remoteSocket.close();
    } catch (e) {}
  }
}
