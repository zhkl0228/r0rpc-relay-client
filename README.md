# Java Client

This folder contains the Java relay client SDK used by Android/Xposed integrations.

## Build

From this directory run:

```powershell
powershell -ExecutionPolicy Bypass -File .\build.ps1
```

The jar will be created at `dist/r0rpc-relay-client.jar`.

## Basic usage

```java
RelayClient client = new RelayClient(
    "https://gzmtx.cn/r0rpc",
    "device",
    "850128",
    "device-001",
    "default"
);

client.registerAction("get_profile", payload -> {
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("uid", "10001");
    result.put("nickname", "demo");
    return RpcResponse.success(result);
});

client.start();
```

## Relay API usage

```java
new RelayClient("https://gzmtx.cn/r0rpc", username, password, clientId, group)
    .registerHandler("ping", new RelayHandler() {
        @Override
        public void handleRequest(RelayRequest request, RelayResponse response) {
            response.success(request.getPayload());
        }
    })
    .start();
```

## AutoBind usage

```java
import com.r0rpc.relay.api.databind.AutoBind;

public final class DecryptHandler implements RelayHandler {
    @AutoBind
    private String encode_str;

    @Override
    public void handleRequest(RelayRequest request, RelayResponse response) {
        response.success(encode_str);
    }
}
```

Invoke body keeps `payload` flat:

```json
{
  "timeoutSeconds": 20,
  "payload": {
    "encode_str": "xxx"
  }
}
```

