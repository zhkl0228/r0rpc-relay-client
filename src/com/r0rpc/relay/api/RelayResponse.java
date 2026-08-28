package com.r0rpc.relay.api;


import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Array;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public final class RelayResponse {
    public interface ResultSender {
        void send(String requestId, String status, int httpCode, JSONObject payload, String error, long latencyMs) throws IOException;
    }

    private final String requestId;
    private final ResultSender resultSender;
    private final long startedAt;
    private final AtomicBoolean responded = new AtomicBoolean(false);

    public RelayResponse(String requestId, ResultSender resultSender, long startedAt) {
        this.requestId = requestId;
        this.resultSender = resultSender;
        this.startedAt = startedAt;
    }

    public boolean success() { return success(new JSONObject()); }
    public boolean success(Object data) { return sendOnce("success", 200, toPayload(data), ""); }
    public boolean failed(String errorMessage) { return failed(500, errorMessage); }
    public boolean failed(int httpCode, String errorMessage) {
        return sendOnce("error", httpCode <= 0 ? 500 : httpCode, new JSONObject(), errorMessage == null ? "" : errorMessage);
    }
    public boolean failed(Throwable throwable) { return failed(500, stackTraceOf(throwable)); }
    public boolean isResponded() { return responded.get(); }

    private boolean sendOnce(String status, int httpCode, JSONObject payload, String error) {
        if (!responded.compareAndSet(false, true)) { return false; }
        try {
            resultSender.send(requestId, status, httpCode, payload, error, System.currentTimeMillis() - startedAt);
            return true;
        } catch (IOException ex) {
            throw new IllegalStateException("send response failed", ex);
        }
    }

    /** 把 handler 回的东西规整成 JSONObject：JSONObject 直接用；Map 递归转；标量包成 {"data": ...}。 */
    private static JSONObject toPayload(Object data) {
        if (data == null) { return new JSONObject(); }
        if (data instanceof JSONObject) { return (JSONObject) data; }
        if (data instanceof Map) { return mapToJson((Map<?, ?>) data); }
        return new JSONObject().put("data", normalizeValue(data));
    }

    private static JSONObject mapToJson(Map<?, ?> input) {
        JSONObject output = new JSONObject();
        for (Map.Entry<?, ?> entry : input.entrySet()) {
            output.put(String.valueOf(entry.getKey()), normalizeValue(entry.getValue()));
        }
        return output;
    }

    /** 转成 JSON 能直接吃的值：null→NULL、byte[]→Base64、Map→JSONObject、数组/Iterable→JSONArray。 */
    private static Object normalizeValue(Object value) {
        if (value == null) { return JSONObject.NULL; }
        if (value instanceof JSONObject || value instanceof JSONArray) { return value; }
        if (value instanceof String || value instanceof Number || value instanceof Boolean) { return value; }
        if (value instanceof byte[]) { return java.util.Base64.getEncoder().encodeToString((byte[]) value); }
        if (value instanceof Map) { return mapToJson((Map<?, ?>) value); }
        if (value instanceof Iterable) {
            JSONArray output = new JSONArray();
            for (Object item : (Iterable<?>) value) { output.put(normalizeValue(item)); }
            return output;
        }
        if (value.getClass().isArray()) {
            JSONArray output = new JSONArray();
            int length = Array.getLength(value);
            for (int i = 0; i < length; i++) { output.put(normalizeValue(Array.get(value, i))); }
            return output;
        }
        return String.valueOf(value);
    }

    private static String stackTraceOf(Throwable throwable) {
        if (throwable == null) { return ""; }
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        throwable.printStackTrace(printWriter);
        printWriter.flush();
        return stringWriter.toString();
    }
}
