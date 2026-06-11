// 核心配置文件：轻量级 VLESS 隧道
// 专注于高性能、低延迟的流式转发

export default {
  async fetch(request, env, ctx) {
    try {
      const upgradeHeader = request.headers.get('Upgrade');
      
      // 1. 判断是否为客户端的 WebSocket 连接请求
      if (upgradeHeader === 'websocket') {
        return await vlessOverWebSocketHandler(request, env);
      }
      
      // 2. 如果是普通 Web 访问，则执行网页伪装（静态反代）
      // 你可以在环境变量中设置 PROXY_URL，或者默认反代一个安全的静态页
      const mockUrl = env.PROXY_URL || 'https://www.bing.com';
      const url = new URL(request.url);
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
async function vlessOverWebSocketHandler(request, env) {
  // 创建 WebSocket 对
  const webSocketPair = new WebSocketPair();
  const [client, server] = Object.values(webSocketPair);

  // 接受连接
  server.accept();

  let remoteSocket = null;
  const userID = env.UUID || "88888888-8888-8888-8888-888888888888"; // 默认UUID

  // 监听客户端发送的第一个数据包（包含 VLESS 协议头）
  server.addEventListener('message', async (event) => {
    try {
      // 首次握手，处理协议头部
      if (!remoteSocket) {
        const buffer = event.data;
        if (!(buffer instanceof ArrayBuffer)) {
          server.close(1003, "需要二进制数据流");
          return;
        }

        const view = new DataView(buffer);
        if (buffer.byteLength < 24) {
          server.close(1002, "协议头部长度不足");
          return;
        }

        // 验证 VLESS 版本 (第一字节通常为 0)
        const version = view.getUint8(0);
        
        // 验证 UUID (第 2 到 17 字节)
        const uuidChunks = [];
        for (let i = 1; i <= 16; i++) {
          uuidChunks.push(view.getUint8(i).toString(16).padStart(2, '0'));
        }
        const clientUUID = `${uuidChunks.slice(0,4).join('')}-${uuidChunks.slice(4,6).join('')}-${uuidChunks.slice(6,8).join('')}-${uuidChunks.slice(8,10).join('')}-${uuidChunks.slice(10,16).join('')}`;

        if (clientUUID !== userID) {
          server.close(1002, "UUID 认证失败");
          return;
        }

        // 解析目标地址与端口
        const addonLength = view.getUint8(17);
        const command = view.getUint8(18 + addonLength); // 1: TCP, 2: UDP
        const targetPort = view.getUint16(19 + addonLength);
        const addressType = view.getUint8(21 + addonLength); // 1: IPv4, 2: 域名, 3: IPv6

        let targetHost = "";
        let addressStart = 22 + addonLength;

        if (addressType === 1) {
          // IPv4
          targetHost = [
            view.getUint8(addressStart),
            view.getUint8(addressStart + 1),
            view.getUint8(addressStart + 2),
            view.getUint8(addressStart + 3)
          ].join('.');
          addressStart += 4;
        } else if (addressType === 2) {
          // 域名
          const domainLength = view.getUint8(addressStart);
          const decoder = new TextDecoder();
          targetHost = decoder.decode(new Uint8Array(buffer, addressStart + 1, domainLength));
          addressStart += 1 + domainLength;
        } else if (addressType === 3) {
          // IPv6
          const ipv6Chunks = [];
          for (let i = 0; i < 8; i++) {
            ipv6Chunks.push(view.getUint16(addressStart + i * 2).toString(16));
          }
          targetHost = `[${ipv6Chunks.join(':')}]`;
          addressStart += 16;
        }

        // 提取除去 VLESS 头部后的纯原始网络负载数据
        const rawPayload = buffer.slice(addressStart);

        // 使用 Cloudflare 边缘 Socket 直连目标网站
        // @ts-ignore
        remoteSocket = connect({
          hostname: targetHost,
          port: targetPort
        });

        // 处理从目标网站返回的数据，原路写回 WebSocket 通道
        ctxDotPipe(remoteSocket, server);

        // 写入第一批脱壳后的原始数据
        if (rawPayload.byteLength > 0) {
          const writer = remoteSocket.writable.getWriter();
          await writer.write(rawPayload);
          writer.releaseLock();
        }

        // 发送 VLESS 握手成功响应给客户端 (1字节版本号 + 1字节附加信息长度)
        const responseHeader = new Uint8Array([version, 0]);
        server.send(responseHeader);

      } else {
        // 后续持续不断的数据流，直接脱壳写入目标 Socket
        const writer = remoteSocket.writable.getWriter();
        await writer.write(event.data);
        writer.releaseLock();
      }
    } catch (error) {
      server.close(1011, `建立上行链路失败: ${error.message}`);
    }
  });

  // 当客户端断开 WebSocket 时，同步断开远端套接字
  server.addEventListener('close', () => {
    if (remoteSocket) remoteSocket.close();
  });
  server.addEventListener('error', () => {
    if (remoteSocket) remoteSocket.close();
  });

  return new Response(null, { status: 101, webSocket: client });
}

/**
 * 管道对调：将目标网站传回的 TCP 流不间断地塞入 WebSocket 传回给用户
 */
async function ctxDotPipe(remoteSocket, server) {
  try {
    const reader = remoteSocket.readable.getReader();
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      // 只要远端有数据返回，立即通过 WebSocket 发送给客户端
      server.send(value);
    }
  } catch (err) {
    server.close(1011, "远端连接发生异常断开");
  } finally {
    server.close();
  }
}
