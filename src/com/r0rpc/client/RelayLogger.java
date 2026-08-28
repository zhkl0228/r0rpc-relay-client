package com.r0rpc.client;

/**
 * SDK 自己的日志出口。
 * <p>
 * 这样 SDK 就不用 import {@code android.util.Log}——原先为了在桌面 JVM 上也能编译/运行，
 * jar 里塞了一个 {@code android/util/Log} 桩类，那玩意在 Android 上是死代码、在 jar 里也脏。
 * <p>
 * 默认实现打到 {@code System.out}/{@code System.err}：Android 的应用进程会把这两个流重定向进
 * logcat（{@code RuntimeInit.redirectLogStreams()}，tag 是 {@code System.out}/{@code System.err}），
 * 桌面 JVM 上就是控制台。想要自己的 logcat tag，用 {@link RelayClient#logger(RelayLogger)} 换掉。
 */
public interface RelayLogger {

    /** 连接断开、重连、服务端静默这类正常但值得知道的事。 */
    void warn(String message);

    /** SDK 内部没人处理的异常（连接/派发出错等）都到这里。 */
    void error(String message, Throwable error);

}
