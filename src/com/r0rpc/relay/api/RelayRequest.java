package com.r0rpc.relay.api;

import org.json.JSONObject;

public final class RelayRequest {
    private final String requestId;
    private final String group;
    private final String action;
    private final String clientId;
    private final JSONObject payload;

    public RelayRequest(String requestId, String group, String action, String clientId, JSONObject payload) {
        this.requestId = requestId == null ? "" : requestId;
        this.group = group == null ? "" : group;
        this.action = action == null ? "" : action;
        this.clientId = clientId == null ? "" : clientId;
        this.payload = payload == null ? new JSONObject() : payload;
    }

    public String getRequestId() { return requestId; }
    public String getGroup() { return group; }
    public String getAction() { return action; }
    public String getClientId() { return clientId; }
    public JSONObject getPayload() { return payload; }

    public String getString(String key) { return payload.optString(key, ""); }
    public int getInt(String key, int defaultValue) { return payload.optInt(key, defaultValue); }
}
