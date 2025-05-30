package com.example.netclient;

import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;
import io.vertx.core.buffer.Buffer;
// 使用 RxJava 版本的 Vertx 和 NetServer
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.net.NetServer;
import io.vertx.reactivex.core.net.NetSocket;
import org.reactivestreams.Subscriber; // RxJava Flowable's Subscriber

import java.util.concurrent.TimeUnit;

public class FlowableNetServerExample {

  public static void main(String[] args) {
    // 1. 创建 RxJava 版本的 Vertx 实例
    Vertx vertx = Vertx.vertx();

    // 2. 创建 NetServer 实例 (RxJava 版本)
    NetServer server = vertx.createNetServer();

    // 3. 设置连接处理器
    server.connectHandler(socket -> {
      System.out.println("Client connected: " + socket.remoteAddress());

      // 示例：创建一个每秒发出一个字符串的 Flowable
      Flowable<String> stringFlowable = Flowable.interval(1, TimeUnit.SECONDS, Schedulers.io())
        .map(i -> "Tick " + i + " from server\n")
        .doOnSubscribe(subscription -> System.out.println("Flowable subscribed by client " + socket.remoteAddress()))
        .doOnCancel(() -> System.out.println("Flowable cancelled for client " + socket.remoteAddress()))
        .doOnComplete(() -> System.out.println("Flowable completed for client " + socket.remoteAddress()));

      // 将 Flowable<String> 转换为 Flowable<Buffer>
      Flowable<Buffer> bufferFlowable = stringFlowable.map(Buffer::buffer);

      // RxNetSocket 本身就是 RxWriteStream<Buffer>
      // RxWriteStream 有一个 toSubscriber() 方法可以得到一个 Subscriber
      // 这个 Subscriber 会处理背压，当 socket 的写入缓冲区满时，它会停止从 Flowable 请求数据，
      // 当缓冲区有空间时（通过 drainHandler），它会继续请求。
      Subscriber<Buffer> socketSubscriber = socket.toSubscriber();

      // 将 Flowable 订阅到 socket 的 Subscriber
      // 这会自动处理 Flowable 的 onNext, onError, onComplete
      // onComplete 时，默认情况下，WriteStreamSubscriber 会调用 end() 在 WriteStream 上（对于NetSocket是关闭连接）
      // onError 时，默认情况下，WriteStreamSubscriber 会调用 end() 或 close()
      bufferFlowable.subscribe(socketSubscriber);


      // (可选) 处理客户端的输入
      socket.handler(buffer -> {
        System.out.println("Received from client (" + socket.remoteAddress() + "): " + buffer.toString().trim());
        if ("stop".equalsIgnoreCase(buffer.toString().trim())) {
          System.out.println("Client requested stop. Closing socket.");
          // 注意：直接关闭 socket 会导致 Flowable 的订阅被取消 (如果 Flowable 仍在发射)
          // 或者如果 Flowable 已经完成，则这只是关闭 socket。
          socket.close();
        }
      });

      // 设置 socket 关闭处理器
      socket.closeHandler(v -> {
        System.out.println("Client disconnected: " + socket.remoteAddress());
        // 当 socket 关闭时，Flowable 的订阅通常会被取消（如果它还在运行）
        // WriteStreamSubscriber 也会处理这个
      });

      // 设置 socket 异常处理器
      socket.exceptionHandler(t -> {
        System.err.println("Socket error for " + socket.remoteAddress() + ": " + t.getMessage());
        // 异常通常也会导致 socket 关闭和 Flowable 取消
      });
    });

    // 4. 启动服务器监听 (RxJava 版本返回 Single)
    server.listen(8888, "localhost")
      .onSuccess(serverSocket -> {
        System.out.println("Server listening on port 8888");
      })
      .onFailure(throwable -> {
        System.err.println("Server failed to listen on port 8888: " + throwable.getMessage());
      });

    System.out.println("NetServer creation initiated. The listen() call is non-blocking.");
  }
}
