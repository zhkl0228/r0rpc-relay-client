package com.r0rpc.client;

import com.r0rpc.relay.api.RelayHandler;
import com.r0rpc.relay.api.RelayRequest;
import com.r0rpc.relay.api.RelayResponse;
import com.r0rpc.relay.api.databind.AutoBind;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.zip.GZIPOutputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class RelayClient {

    private static final String TAG = "R0RPC";
    private static final RelayLogger DEFAULT_LOGGER = new RelayLogger() {
        @Override
        public void warn(String message) {
            System.out.println("[W/" + TAG + "] " + message);
        }
        @Override
        public void error(String message, Throwable error) {
            System.err.println("[E/" + TAG + "] " + message);
            if (error != null) {
                error.printStackTrace(System.err);
            }
        }
    };
    private static final long BASE_RETRY_DELAY_MS = 1000L;
    private static final long MAX_RETRY_DELAY_MS = 30000L;
    private static final long HEARTBEAT_INTERVAL_MS = 5000L;
    private static final long HEARTBEAT_JITTER_MS = 1500L;
    private static final long STABLE_CONNECTION_RESET_MS = 60000L;
    private static final long SERVER_SILENCE_TIMEOUT_MS = (HEARTBEAT_INTERVAL_MS + HEARTBEAT_JITTER_MS) * 3L;
    private static final String PAYLOAD_ENCODING_GZIP_BASE64_JSON = "gzip+base64+json";
    private static final int COMPRESS_PAYLOAD_THRESHOLD_BYTES = 32 * 1024;

    private final String baseUrl;
    private final String username;
    private final String password;
    private final String clientId;
    private final String group;
    private final String platform;
    private final int connectTimeoutMs;
    private final int readTimeoutMs;
    private final Map<String, RelayHandler> relayHandlers = new ConcurrentHashMap<String, RelayHandler>();
    private final Object lifecycleLock = new Object();
    private final Object executorLock = new Object();

    private volatile String token;
    private volatile String wsUrl;
    private volatile long lastServerActivityAt;
    private volatile int maxInFlight = 256;
    private volatile boolean running;
    private volatile Thread workerThread;
    private volatile RelayLogger logger = DEFAULT_LOGGER;
    private volatile ThreadPoolExecutor jobExecutor;

    public RelayClient(String baseUrl, String username, String password, String clientId, String group) {
        this(baseUrl, username, password, clientId, group, DEFAULT_PLATFORM, 5000, 30000);
    }

    public RelayClient(String baseUrl, String username, String password, String clientId, String group,
                       String platform) {
        this(baseUrl, username, password, clientId, group, platform, 5000, 30000);
    }

    public RelayClient(String baseUrl, String username, String password, String clientId, String group,
                       String platform, int connectTimeoutMs, int readTimeoutMs) {
        this.baseUrl = normalizeBaseUrl(baseUrl);
        this.username = username;
        this.password = password;
        this.clientId = clientId;
        this.group = group;
        this.platform = platform == null || platform.trim().isEmpty() ? DEFAULT_PLATFORM : platform.trim();
        this.connectTimeoutMs = connectTimeoutMs;
        this.readTimeoutMs = readTimeoutMs;
    }

    /**
     * 没显式传 platform 时的兜底值。platform 现在表示<b>接入种类</b>（如 {@code Android/xposed}、
     * {@code Apple/frida}、{@code J2SE}），该由各 client 显式传入；这里不再反射 android.os.Build
     * 去猜机型（那既区分不了客户端种类，又把「桌面」误报成 android）。
     */
    private static final String DEFAULT_PLATFORM = "unknown";

    public RelayClient registerHandler(String action, RelayHandler handler) {
        if (handler == null) {
            throw new IllegalArgumentException("handler can not be null");
        }
        if (action == null || action.trim().isEmpty()) {
            throw new IllegalArgumentException("handler action can not be empty");
        }
        relayHandlers.put(action.trim(), handler);
        return this;
    }

    /**
     * 换掉 SDK 的日志出口，比如在 Android 上接到 logcat 拿到自己的 tag。传 null 恢复默认。
     */
    public RelayClient logger(RelayLogger logger) {
        this.logger = logger == null ? DEFAULT_LOGGER : logger;
        return this;
    }

    public RelayClient maxInFlight(int value) {
        if (value <= 0) {
            return this;
        }
        if (value > 256) {
            value = 256;
        }
        this.maxInFlight = value;
        return this;
    }

    public RelayClient start() {
        return start("r0rpc-relay-client");
    }

    public RelayClient start(String threadName) {
        synchronized (lifecycleLock) {
            if (workerThread != null && workerThread.isAlive()) {
                return this;
            }
            final String finalThreadName = threadName == null || threadName.trim().isEmpty() ? "r0rpc-relay-client" : threadName.trim();
            running = true;
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        loopForever();
                    } catch (Throwable throwable) {
                        notifyError(throwable);
                    } finally {
                        synchronized (lifecycleLock) {
                            if (Thread.currentThread() == workerThread) {
                                workerThread = null;
                            }
                        }
                    }
                }
            }, finalThreadName);
            thread.setDaemon(true);
            workerThread = thread;
            thread.start();
            return this;
        }
    }

    public void login() throws IOException {
        JSONObject body = new JSONObject();
        body.put("username", username);
        body.put("password", password);
        body.put("clientId", clientId);
        body.put("group", group);
        body.put("platform", platform);
        body.put("maxInFlight", maxInFlight);

        JSONObject response = postJson("/api/client/login", null, body);
        String tokenValue = response.optString("token", "");
        if (tokenValue.isEmpty()) {
            throw new IOException("Login succeeded but token is missing");
        }
        token = tokenValue;
        int effective = response.optInt("maxInFlight", 0);
        if (effective > 0) {
            maxInFlight = effective;
        }
        String wsUrlValue = response.optString("wsUrl", "");
        wsUrl = wsUrlValue.isEmpty() ? buildWsUrl() : wsUrlValue;
        ensureJobExecutor();
    }

    public void loopForever() throws IOException {
        running = true;
        int retryAttempt = 0;

        while (running) {
            SimpleWebSocket socket = null;
            long connectedAt = 0L;
            try {
                ensureLoggedIn();
                socket = SimpleWebSocket.connect(currentWsUrl(), null, connectTimeoutMs, readTimeoutMs);
                connectedAt = System.currentTimeMillis();
                lastServerActivityAt = connectedAt;
                runSocketLoop(socket);

                if (!running) {
                    return;
                }

                if (System.currentTimeMillis() - connectedAt >= STABLE_CONNECTION_RESET_MS) {
                    retryAttempt = 0;
                }
                long delayMs = computeRetryDelayMs(retryAttempt++);
                logger.warn("relay connection closed, retry in " + delayMs + "ms, attempt=" + retryAttempt);
                sleepQuietly(delayMs);
            } catch (IOException ex) {
                if (!running) {
                    return;
                }
                if (isUnauthorized(ex)) {
                    clearSession();
                }
                if (connectedAt > 0L && System.currentTimeMillis() - connectedAt >= STABLE_CONNECTION_RESET_MS) {
                    retryAttempt = 0;
                }
                long delayMs = computeRetryDelayMs(retryAttempt++);
                logger.warn("relay reconnect scheduled in " + delayMs + "ms, attempt=" + retryAttempt + ", reason=" + safeMessage(ex));
                sleepQuietly(delayMs);
            } finally {
                closeQuietly(socket);
            }
        }
    }

    public void stop() {
        running = false;
        Thread thread = workerThread;
        if (thread != null) {
            thread.interrupt();
        }
        shutdownJobExecutor();
    }

    public Thread getWorkerThread() {
        return workerThread;
    }

    public void logoutQuietly() {
        if (token == null || token.isEmpty()) {
            return;
        }
        try {
            postJson("/api/client/logout", token, new JSONObject());
        } catch (Exception ignore) {
        }
    }

    @SuppressWarnings("unchecked")
    private void runSocketLoop(final SimpleWebSocket socket) throws IOException {
        Thread heartbeat = new Thread(new Runnable() {
            @Override
            public void run() {
                sleepQuietly(computeHeartbeatDelayMs());
                while (running && socket.isOpen()) {
                    if (isServerSilent()) {
                        logger.warn("relay server silent, closing socket for reconnect");
                        closeQuietly(socket);
                        return;
                    }
                    try {
                        socket.sendHeartbeat();
                    } catch (IOException ex) {
                        closeQuietly(socket);
                        return;
                    }
                    sleepQuietly(computeHeartbeatDelayMs());
                }
            }
        }, "r0rpc-heartbeat");
        heartbeat.setDaemon(true);
        heartbeat.start();

        while (running && socket.isOpen()) {
            String text = socket.readText();
            if (text == null || text.isEmpty()) {
                return;
            }
            lastServerActivityAt = System.currentTimeMillis();
            JSONObject message = new JSONObject(text);
            if (!"job".equals(message.optString("type"))) {
                continue;
            }
            JSONObject jobObject = message.optJSONObject("job");
            if (jobObject == null) {
                continue;
            }
            dispatchJob(socket, jobObject);
        }
    }

    private void dispatchJob(final SimpleWebSocket socket, final JSONObject job) throws IOException {
        ensureJobExecutor();
        // job 是每条消息新 parse 出来的 JSONObject，只交给一个 worker 处理，不跨消息共享，无需复制
        try {
            jobExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        handleJob(socket, job);
                    } catch (IOException ex) {
                        closeQuietly(socket);
                        notifyError(ex);
                    }
                }
            });
        } catch (RejectedExecutionException ex) {
            throw new IOException("client worker pool is full", ex);
        }
    }

    private void handleJob(final SimpleWebSocket socket, JSONObject job) throws IOException {
        String requestId = job.optString("requestId");
        String action = job.optString("action");
        String groupName = job.optString("group");
        String targetClientId = job.optString("clientId");
        JSONObject payload = job.optJSONObject("payload");
        if (payload == null) {
            payload = new JSONObject();
        }

        long startedAt = System.currentTimeMillis();
        RelayRequest request = new RelayRequest(requestId, groupName, action, targetClientId, payload);
        RelayResponse response = new RelayResponse(requestId, new RelayResponse.ResultSender() {
            @Override
            public void send(String respondedRequestId, String status, int httpCode, JSONObject respondedPayload, String error, long latencyMs) throws IOException {
                sendResult(socket, respondedRequestId, status, httpCode, respondedPayload, error, latencyMs);
            }
        }, startedAt);

        RelayHandler relayHandler = relayHandlers.get(action);
        if (relayHandler == null) {
            response.failed("No handler registered for action: " + action);
            return;
        }
        try {
            invokeRelayHandler(relayHandler, request, response);
        } catch (Throwable throwable) {
            response.failed(throwable);
        }
    }

    private void invokeRelayHandler(RelayHandler handler, RelayRequest request, RelayResponse response) throws Exception {
        RelayHandler effectiveHandler = createRequestScopedRelayHandler(handler);
        if (effectiveHandler != handler) {
            bindAutoFields(effectiveHandler, request);
            effectiveHandler.handleRequest(request, response);
            return;
        }
        if (requiresSerializedRelayHandler(handler)) {
            synchronized (handler) {
                bindAutoFields(handler, request);
                handler.handleRequest(request, response);
            }
            return;
        }
        bindAutoFields(handler, request);
        handler.handleRequest(request, response);
    }

    private void bindAutoFields(RelayHandler handler, RelayRequest request) throws IllegalAccessException {
        Class<?> current = handler.getClass();
        while (current != null && current != Object.class) {
            Field[] fields = current.getDeclaredFields();
            for (Field field : fields) {
                AutoBind autoBind = field.getAnnotation(AutoBind.class);
                if (autoBind == null || Modifier.isStatic(field.getModifiers())) {
                    continue;
                }
                String key = autoBind.key().trim().isEmpty() ? field.getName() : autoBind.key().trim();
                Object raw = request.getPayload().opt(key);
                if (raw == JSONObject.NULL) {
                    raw = null;
                }
                Object converted = convertValue(field.getType(), raw, autoBind);
                field.setAccessible(true);
                field.set(handler, converted);
            }
            current = current.getSuperclass();
        }
    }

    private Object convertValue(Class<?> type, Object raw, AutoBind autoBind) {
        if (type == String.class) {
            return raw == null ? autoBind.defaultStringValue() : String.valueOf(raw);
        }
        if (type == int.class || type == Integer.class) {
            if (raw instanceof Number) { return Integer.valueOf(((Number) raw).intValue()); }
            if (raw != null) {
                try { return Integer.valueOf(Integer.parseInt(String.valueOf(raw))); } catch (Exception ignore) {}
            }
            return Integer.valueOf(autoBind.defaultIntValue());
        }
        if (type == long.class || type == Long.class) {
            if (raw instanceof Number) { return Long.valueOf(((Number) raw).longValue()); }
            if (raw != null) {
                try { return Long.valueOf(Long.parseLong(String.valueOf(raw))); } catch (Exception ignore) {}
            }
            return Long.valueOf(autoBind.defaultLongValue());
        }
        if (type == boolean.class || type == Boolean.class) {
            if (raw instanceof Boolean) { return raw; }
            if (raw != null) { return Boolean.valueOf(Boolean.parseBoolean(String.valueOf(raw))); }
            return Boolean.valueOf(autoBind.defaultBooleanValue());
        }
        if (JSONObject.class.isAssignableFrom(type)) {
            return raw instanceof JSONObject ? raw : new JSONObject();
        }
        if (Map.class.isAssignableFrom(type)) {
            return raw instanceof JSONObject ? ((JSONObject) raw).toMap() : new java.util.LinkedHashMap<String, Object>();
        }
        return raw;
    }

    private void sendResult(SimpleWebSocket socket, String requestId, String status, int httpCode, JSONObject payload, String error, long latencyMs) throws IOException {
        EncodedPayload encodedPayload = encodePayload(payload == null ? new JSONObject() : payload);

        JSONObject resultBody = new JSONObject();
        resultBody.put("requestId", requestId);
        resultBody.put("status", status);
        resultBody.put("httpCode", httpCode);
        resultBody.put("payload", encodedPayload.payload);
        if (encodedPayload.encoding.length() > 0) {
            resultBody.put("payloadEncoding", encodedPayload.encoding);
            resultBody.put("payloadRawSize", encodedPayload.rawSize);
            resultBody.put("payloadCompressedSize", encodedPayload.compressedSize);
        }
        resultBody.put("error", error == null ? "" : error);
        resultBody.put("latencyMs", latencyMs);

        JSONObject envelope = new JSONObject();
        envelope.put("type", "result");
        envelope.put("result", resultBody);
        socket.sendText(envelope.toString());
    }

    @SuppressWarnings("unchecked")
    private JSONObject postJson(String path, String bearerToken, JSONObject body) throws IOException {
        HttpURLConnection connection = null;
        try {
            URL url = new URL(baseUrl + path);
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            connection.setConnectTimeout(connectTimeoutMs);
            connection.setReadTimeout(readTimeoutMs);
            connection.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
            connection.setRequestProperty("Accept", "application/json");
            if (bearerToken != null && !bearerToken.isEmpty()) { connection.setRequestProperty("Authorization", "Bearer " + bearerToken); }

            byte[] payload = body.toString().getBytes(StandardCharsets.UTF_8);
            OutputStream outputStream = connection.getOutputStream();
            outputStream.write(payload);
            outputStream.flush();
            outputStream.close();

            int statusCode = connection.getResponseCode();
            String responseText = readAll(statusCode >= 400 ? connection.getErrorStream() : connection.getInputStream());
            JSONObject result;
            try {
                result = responseText.isEmpty() ? new JSONObject() : new JSONObject(responseText);
            } catch (JSONException ex) {
                // body 不是 JSON：多半是 baseUrl 写错(少了路径前缀/打到了别的服务)，
                // 网关或容器回了一张 HTML 错误页。把状态码和正文头部带上，别让它变成
                // 一个含义不明的 JSON 解析错。
                // 注意显式 catch JSONException：制品里它是 RuntimeException，Android 框架里是 checked，
                // 用 catch (RuntimeException) 会在 Android 上漏接。
                throw new IOException("HTTP " + statusCode + " with non-JSON body from " + url + ": " + head(responseText), ex);
            }
            if (statusCode >= 400) { throw new IOException("HTTP " + statusCode + ": " + result.optString("error")); }
            return result;
        } finally {
            if (connection != null) { connection.disconnect(); }
        }
    }

    private EncodedPayload encodePayload(JSONObject payload) throws IOException {
        JSONObject safePayload = payload == null ? new JSONObject() : payload;
        String payloadJson = safePayload.toString();
        byte[] raw = payloadJson.getBytes(StandardCharsets.UTF_8);
        if (raw.length < COMPRESS_PAYLOAD_THRESHOLD_BYTES) {
            return EncodedPayload.identity(safePayload);
        }
        byte[] compressed = gzip(raw);
        int wireSize = ((compressed.length + 2) / 3) * 4;
        if (wireSize >= raw.length) {
            return EncodedPayload.identity(safePayload);
        }
        return EncodedPayload.compressed(java.util.Base64.getEncoder().encodeToString(compressed), PAYLOAD_ENCODING_GZIP_BASE64_JSON, raw.length, compressed.length);
    }

    private byte[] gzip(byte[] input) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(outputStream);
        gzipOutputStream.write(input);
        gzipOutputStream.finish();
        gzipOutputStream.close();
        return outputStream.toByteArray();
    }

    private static final class EncodedPayload {
        private final Object payload;
        private final String encoding;
        private final int rawSize;
        private final int compressedSize;

        private EncodedPayload(Object payload, String encoding, int rawSize, int compressedSize) {
            this.payload = payload;
            this.encoding = encoding;
            this.rawSize = rawSize;
            this.compressedSize = compressedSize;
        }

        private static EncodedPayload identity(Object payload) {
            return new EncodedPayload(payload, "", 0, 0);
        }

        private static EncodedPayload compressed(String payload, String encoding, int rawSize, int compressedSize) {
            return new EncodedPayload(payload, encoding, rawSize, compressedSize);
        }
    }
    private static String head(String text) {
        if (text == null) {
            return "null";
        }
        String trimmed = text.trim();
        return trimmed.length() <= 200 ? trimmed : trimmed.substring(0, 200) + "...(" + trimmed.length() + " chars)";
    }

    private void notifyError(Throwable throwable) {
        if (throwable == null) {
            return;
        }
        logger.error("relay client error", throwable);
    }

    private void ensureLoggedIn() throws IOException {
        if (token == null || token.isEmpty()) {
            login();
        }
    }

    private void clearSession() {
        token = null;
        wsUrl = null;
    }

    private boolean isUnauthorized(IOException ex) {
        String message = safeMessage(ex);
        return message.contains("401") || message.contains("unauthorized") || message.contains("Unauthorized");
    }

    private long computeRetryDelayMs(int attempt) {
        int safeAttempt = Math.max(0, Math.min(attempt, 6));
        long capped = BASE_RETRY_DELAY_MS * (1L << safeAttempt);
        if (capped < BASE_RETRY_DELAY_MS) {
            capped = BASE_RETRY_DELAY_MS;
        }
        capped = Math.min(capped, MAX_RETRY_DELAY_MS);
        if (capped <= BASE_RETRY_DELAY_MS) {
            return BASE_RETRY_DELAY_MS;
        }
        return BASE_RETRY_DELAY_MS + randomLong(capped - BASE_RETRY_DELAY_MS + 1L);
    }

    private long computeHeartbeatDelayMs() {
        return HEARTBEAT_INTERVAL_MS + randomLong(HEARTBEAT_JITTER_MS + 1L);
    }

    private boolean requiresSerializedRelayHandler(RelayHandler handler) {
        return hasAutoBindFields(handler == null ? null : handler.getClass());
    }

    private RelayHandler createRequestScopedRelayHandler(RelayHandler handler) {
        if (handler == null) {
            return null;
        }
        Class<?> handlerClass = handler.getClass();
        if (!hasAutoBindFields(handlerClass)) {
            return handler;
        }
        try {
            java.lang.reflect.Constructor<?> constructor = handlerClass.getDeclaredConstructor();
            constructor.setAccessible(true);
            Object instance = constructor.newInstance();
            if (instance instanceof RelayHandler) {
                return (RelayHandler) instance;
            }
        } catch (Throwable ignore) {
        }
        return handler;
    }

    private boolean hasAutoBindFields(Class<?> type) {
        Class<?> current = type;
        while (current != null && current != Object.class) {
            Field[] fields = current.getDeclaredFields();
            for (Field field : fields) {
                if (field.getAnnotation(AutoBind.class) != null) {
                    return true;
                }
            }
            current = current.getSuperclass();
        }
        return false;
    }

    private void ensureJobExecutor() {
        int desired = maxInFlight <= 0 ? 1 : maxInFlight;
        synchronized (executorLock) {
            if (jobExecutor != null && jobExecutor.getMaximumPoolSize() == desired) {
                return;
            }
            ThreadPoolExecutor previous = jobExecutor;
            // 按需扩容而非按 maxInFlight 预热：core=0 + SynchronousQueue 是「直接交接」——
            // 有空闲 worker 就复用，没有才新建，最多建到 desired。线程数因此等于真实并发峰值
            // (比如只有 8 路并发就只有 8 条 worker)，而不是被 maxInFlight(默认 256)顶满。
            // 手机上每条线程都占一份栈(实测 64 条 ≈ 24MB Stack)，这样能把空占的栈省掉，
            // 空闲 60s 后 worker 还会自己退出，稳态回到 0 条。
            jobExecutor = new ThreadPoolExecutor(
                0,
                desired,
                60L,
                TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable runnable) {
                        Thread thread = new Thread(runnable, "r0rpc-job-worker");
                        thread.setDaemon(true);
                        return thread;
                    }
                },
                // 服务端已按 maxInFlight 限制在途数量，正常到不了 desired；万一真饱和了，
                // 就在接收线程上就地把这条 job 跑掉(天然背压:跑完才继续读下一条)，绝不丢。
                new ThreadPoolExecutor.CallerRunsPolicy()
            );
            if (previous != null) {
                previous.shutdownNow();
            }
        }
    }

    private void shutdownJobExecutor() {
        synchronized (executorLock) {
            if (jobExecutor == null) {
                return;
            }
            jobExecutor.shutdownNow();
            jobExecutor = null;
        }
    }

    private boolean isServerSilent() {
        long lastSeenAt = lastServerActivityAt;
        return lastSeenAt > 0L && System.currentTimeMillis() - lastSeenAt > computeServerSilenceTimeoutMs();
    }

    private long computeServerSilenceTimeoutMs() {
        long timeoutMs = SERVER_SILENCE_TIMEOUT_MS;
        if (readTimeoutMs > 0) {
            timeoutMs = Math.min(timeoutMs, Math.max(10000L, (long) readTimeoutMs));
        }
        return Math.max(10000L, timeoutMs);
    }

    private long randomLong(long boundExclusive) {
        if (boundExclusive <= 0L) {
            return 0L;
        }
        return ThreadLocalRandom.current().nextLong(boundExclusive);
    }

    private String safeMessage(Throwable throwable) {
        if (throwable == null || throwable.getMessage() == null) {
            return throwable == null ? "unknown" : throwable.getClass().getSimpleName();
        }
        return throwable.getMessage();
    }

    private String currentWsUrl() { return wsUrl == null || wsUrl.isEmpty() ? buildWsUrl() : wsUrl; }

    private String buildWsUrl() {
        String wsBase = baseUrl;
        if (wsBase.startsWith("https://")) {
            wsBase = "wss://" + wsBase.substring("https://".length());
        } else if (wsBase.startsWith("http://")) {
            wsBase = "ws://" + wsBase.substring("http://".length());
        }
        return wsBase + "/api/client/ws?token=" + urlEncode(token == null ? "" : token);
    }

    private static String normalizeBaseUrl(String value) {
        String normalized = value == null ? "" : value.trim();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException("baseUrl can not be empty");
        }
        if (!normalized.startsWith("http://") && !normalized.startsWith("https://")) {
            normalized = "http://" + normalized;
        }
        return trimTrailingSlash(normalized);
    }

    private static String trimTrailingSlash(String value) {
        if (value.endsWith("/")) { return value.substring(0, value.length() - 1); }
        return value;
    }

    private static String readAll(InputStream inputStream) throws IOException {
        if (inputStream == null) { return ""; }
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
        StringBuilder builder = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) { builder.append(line); }
        return builder.toString();
    }


    private static String urlEncode(String value) {
        StringBuilder builder = new StringBuilder();
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < bytes.length; i++) {
            int item = bytes[i] & 0xff;
            boolean safe = (item >= 'a' && item <= 'z') || (item >= 'A' && item <= 'Z') || (item >= '0' && item <= '9') || item == '-' || item == '_' || item == '.' || item == '~';
            if (safe) {
                builder.append((char) item);
            } else {
                builder.append('%');
                String hex = Integer.toHexString(item).toUpperCase();
                if (hex.length() == 1) { builder.append('0'); }
                builder.append(hex);
            }
        }
        return builder.toString();
    }

    private static void sleepQuietly(long millis) {
        try { Thread.sleep(millis); } catch (InterruptedException ignore) { Thread.currentThread().interrupt(); }
    }

    private static void closeQuietly(SimpleWebSocket socket) {
        if (socket == null) { return; }
        try { socket.close(); } catch (Exception ignore) {}
    }
}




