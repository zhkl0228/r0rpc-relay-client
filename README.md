# Java Client

This folder contains the Java relay client SDK used by Android/Xposed integrations.

## 依赖（Maven Central）

- **JSON**：数据模型用 `org.json.JSONObject`——Android 走系统自带，桌面 JVM 需自行加 `org.json:json`（`provided`，不随主 jar 带）。
- **WebSocket**：用 `org.java-websocket:Java-WebSocket`（+`slf4j-api`）——Android/Java 8 没有内置 WS 客户端，作为传递依赖自动带上，无需手动加。

主 jar：

```xml
<dependency>
    <groupId>com.github.zhkl0228</groupId>
    <artifactId>r0rpc-relay-client</artifactId>
    <version>1.2.1</version>
</dependency>
```

Android 运行时直接加载 dex 的场景（如 Frida），取 `dex` 分类器——jar 里是 `classes.dex`，
**自包含**（SDK + Java-WebSocket + slf4j 都 d8 进去了），可直接 `DexClassLoader`：

```xml
<dependency>
    <groupId>com.github.zhkl0228</groupId>
    <artifactId>r0rpc-relay-client</artifactId>
    <version>1.2.1</version>
    <classifier>dex</classifier>
</dependency>
```

## Build

从当前目录跑（不需要 Android SDK，纯 Java 库）：

```bash
mvn package
```

产物在 `target/r0rpc-relay-client-1.2.1.jar`（Java 8 字节码）。构建时还用 d8 产出一份 `dex` 分类器
（含 `classes.dex`），随 deploy 一起发 Central；android-frida-demo 运行时从 Central 下载它，xposed-demo
把主 jar 交给 AGP 自己 dex——两个 demo 都不再抱本地 jar。

## 发布 Maven Central

```bash
mvn clean deploy
```

会同时发主 jar、`sources`、`javadoc` 和 `dex` 分类器（GPG 签名 + `central-publishing`）。
**要用 JDK 17+ 构建**：dex 那步调 `com.android.tools:r8` 的 d8，d8 要求 JDK 17+
（编译仍产出 release 8 字节码）。

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

client.registerHandler("get_profile", (request, response) -> {
    JSONObject result = new JSONObject();   // 数据模型是 org.json.JSONObject
    result.put("uid", "10001");
    result.put("nickname", "demo");
    response.success(result);
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

