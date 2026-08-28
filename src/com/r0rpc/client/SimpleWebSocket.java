package com.r0rpc.client;

import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * WebSocket 连接的薄封装：底层用 org.java_websocket，对上仍是「阻塞读一条文本消息」的接口，
 * 让 {@link RelayClient} 的收发循环不用改。收到的完整文本消息进队列，{@link #readText()} 阻塞取；
 * 连接关闭/出错时投一个哨兵，让 readText 返回 null，循环自然退出去重连。
 * <p>
 * 握手失败（如 token 过期返回 401）时，{@code onClose} 的 reason 里带有状态码
 * （形如 {@code Invalid status code received: 401 ...}），{@link #connect} 把它塞进抛出的
 * {@link IOException} 消息，供 {@code RelayClient.isUnauthorized} 识别后清 token 重登。
 */
final class SimpleWebSocket implements Closeable {

    /** 关闭哨兵：读队列时用引用相等区分「真消息」与「连接已关」（队列不收 null）。 */
    private static final Object CLOSED = new Object();

    private final WebSocketClient client;
    private final LinkedBlockingQueue<Object> inbox = new LinkedBlockingQueue<Object>();
    private volatile String closeReason;
    private volatile boolean closed;

    private SimpleWebSocket(URI uri, Map<String, String> headers, int connectTimeoutMs, int readTimeoutMs) throws IOException {
        this.client = new WebSocketClient(uri, headers == null ? Collections.<String, String>emptyMap() : headers) {
            @Override
            public void onOpen(ServerHandshake handshake) {
            }

            @Override
            public void onMessage(String message) {
                inbox.offer(message);
            }

            @Override
            public void onClose(int code, String reason, boolean remote) {
                closeReason = "code=" + code + (reason == null || reason.isEmpty() ? "" : " " + reason);
                closed = true;
                inbox.offer(CLOSED);
            }

            @Override
            public void onError(Exception ex) {
                if (closeReason == null) {
                    closeReason = String.valueOf(ex);
                }
                // 兜底：某些出错路径未必跟一个 onClose，这里也标记关闭并投哨兵，
                // 免得 readText() 一直阻塞在 take()（重复投无害，第一次就返回 null）。
                closed = true;
                inbox.offer(CLOSED);
            }
        };
        // 保留库自带的 ping/pong 掉线检测，间隔用 readTimeout（秒）：定期发 WS ping、长时间无任何
        // 数据就判连接已死并关闭。这是传输层探活；SDK 自己的应用层 heartbeat/服务端静默检测是另一层。
        client.setConnectionLostTimeout((int) Math.max(3L, TimeUnit.MILLISECONDS.toSeconds(readTimeoutMs)));
        boolean ok;
        try {
            ok = client.connectBlocking(connectTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IOException("WebSocket connect interrupted", ex);
        }
        if (!ok || closed) {
            throw new IOException("WebSocket connect failed: "
                    + (closeReason != null ? closeReason : "timeout") + " (" + uri + ")");
        }
    }

    public static SimpleWebSocket connect(String wsUrl, Map<String, String> headers, int connectTimeoutMs, int readTimeoutMs) throws IOException {
        try {
            return new SimpleWebSocket(new URI(wsUrl), headers, connectTimeoutMs, readTimeoutMs);
        } catch (URISyntaxException ex) {
            throw new IOException("bad ws url: " + wsUrl, ex);
        }
    }

    public boolean isOpen() {
        return !closed && client.isOpen();
    }

    /** 阻塞取一条完整文本消息；连接已关返回 null。 */
    public String readText() throws IOException {
        try {
            Object message = inbox.take();
            return message == CLOSED ? null : (String) message;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IOException("WebSocket read interrupted", ex);
        }
    }

    public void sendText(String text) throws IOException {
        try {
            client.send(text == null ? "" : text);
        } catch (RuntimeException ex) {
            // WebsocketNotConnectedException 等：连接已断
            throw new IOException("WebSocket send failed", ex);
        }
    }

    public void sendHeartbeat() throws IOException {
        sendText("{\"type\":\"heartbeat\"}");
    }

    @Override
    public void close() {
        closed = true;
        try {
            client.close();
        } catch (RuntimeException ignore) {
        }
        inbox.offer(CLOSED);
    }
}
