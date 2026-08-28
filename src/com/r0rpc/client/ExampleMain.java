package com.r0rpc.client;

import com.r0rpc.relay.api.RelayHandler;
import com.r0rpc.relay.api.RelayRequest;
import com.r0rpc.relay.api.RelayResponse;

import org.json.JSONObject;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;

public final class ExampleMain {
    private ExampleMain() {
    }

    public static void main(String[] args) throws Exception {
        final RelayClient client = new RelayClient(
            "https://gzmtx.cn/r0rpc",   // 线上默认；必须带 /r0rpc 前缀
            "device",
            "850128",
            "device-001",              // clientId 要稳定，别用随机值
            "default",
            "J2SE"                     // platform：桌面 JVM，与各设备端(Android/*、Apple/*)区分
        );

        client.registerHandler("ping", new RelayHandler() {
            @Override
            public void handleRequest(RelayRequest request, RelayResponse response) throws Exception {
                JSONObject result = new JSONObject();
                result.put("ok", true);
                result.put("clientType", "j2se");   // 让调用方区分是哪种 client 执行的
                result.put("message", "pong");
                result.put("clientId", request.getClientId());
                result.put("group", request.getGroup());
                result.put("time", System.currentTimeMillis());
                // 桌面 JVM 没有 App 包名，回主机/进程/运行时身份，证明是哪台机器哪个进程执行的。
                // getLocalHost 理论上会抛 UnknownHostException——真抛了让它带堆栈回调用方（handleRequest
                // throws Exception），不写不存在的兜底。
                String jvmName = ManagementFactory.getRuntimeMXBean().getName(); // 形如 12345@host
                int at = jvmName.indexOf('@');
                result.put("host", InetAddress.getLocalHost().getHostName());
                result.put("pid", at > 0 ? jvmName.substring(0, at) : jvmName);
                result.put("os", System.getProperty("os.name") + " " + System.getProperty("os.version")
                        + " " + System.getProperty("os.arch"));
                result.put("java", System.getProperty("java.version"));
                response.success(result);
            }
        });

        client.registerHandler("raise.error", new RelayHandler() {
            @Override
            public void handleRequest(RelayRequest request, RelayResponse response) {
                throw new RuntimeException("demo exception from java client");
            }
        });

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                client.stop();
                client.logoutQuietly();
            }
        }));

        client.loopForever();
    }
}
