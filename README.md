# Java Client

This folder contains the Java relay client SDK used by Android/Xposed integrations.

## Build

从当前目录跑（不需要 Android SDK，纯 Java 库）：

```bash
mvn package
```

产物在 `dist/r0rpc-relay-client.jar`（Java 8 字节码），并会自动同步到两个 demo 的
`lib/`、`app/libs/` 下，免得三份 jar 各自漂移。

> jar 里带了一个极小的 `android.util.Log` 桩：Android 上会被框架里的真类盖掉，
> 桌面 JVM 上（比如跑 `ExampleMain`）则靠它才能跑起来。所以别把它从 jar 里剔掉。

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

