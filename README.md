# Java Client

This folder contains the Java relay client SDK used by Android/Xposed integrations.

## 依赖（Maven Central）

无第三方依赖，J2SE / 桌面 JVM / Android（AGP 自己 dex）都用主 jar：

```xml
<dependency>
    <groupId>com.github.zhkl0228</groupId>
    <artifactId>r0rpc-relay-client</artifactId>
    <version>1.0.0</version>
</dependency>
```

Android 运行时直接加载 dex 的场景（如 Frida），取 `dex` 分类器——jar 里是 `classes.dex`，
可直接 `DexClassLoader`：

```xml
<dependency>
    <groupId>com.github.zhkl0228</groupId>
    <artifactId>r0rpc-relay-client</artifactId>
    <version>1.0.0</version>
    <classifier>dex</classifier>
</dependency>
```

## Build

从当前目录跑（不需要 Android SDK，纯 Java 库）：

```bash
mvn package
```

产物在 `target/r0rpc-relay-client-1.0.0.jar`（Java 8 字节码）。构建时还会用 d8 产出一份
dex jar 并同步到 `../android-frida-demo/lib/r0rpc-relay-client.jar`（含 `classes.dex`，供 Frida
adb push 后 `DexClassLoader` 加载）。xposed-demo 改从 Maven Central 取依赖，不再需要本地 jar。

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

