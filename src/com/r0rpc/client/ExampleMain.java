package com.r0rpc.client;

import com.r0rpc.relay.api.RelayHandler;
import com.r0rpc.relay.api.RelayRequest;
import com.r0rpc.relay.api.RelayResponse;

import java.util.LinkedHashMap;
import java.util.Map;

public final class ExampleMain {
    private ExampleMain() {
    }

    public static void main(String[] args) throws Exception {
        final RelayClient client = new RelayClient(
            "https://gzmtx.cn/r0rpc",   // 线上默认；必须带 /r0rpc 前缀
            "device",
            "850128",
            "device-001",              // clientId 要稳定，别用随机值
            "default"
        );

        client.registerHandler("ping", new RelayHandler() {
            @Override
            public void handleRequest(RelayRequest request, RelayResponse response) {
                Map<String, Object> result = new LinkedHashMap<String, Object>();
                result.put("ok", true);
                result.put("message", "pong");
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
