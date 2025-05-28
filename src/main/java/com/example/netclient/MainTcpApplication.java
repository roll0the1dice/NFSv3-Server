package com.example.netclient;

import io.vertx.core.VertxOptions;
import io.vertx.reactivex.core.Vertx;

public class MainTcpApplication {
  public static void main(String[] args) {
//    VertxOptions options = new VertxOptions()
//      .setBlockedThreadCheckInterval(5000) // 检查间隔，单位毫秒 (默认1000ms)
//      .setMaxEventLoopExecuteTime(5000)    // 事件循环允许的最大执行时间，单位毫秒 (默认2000ms)
//      .setMaxWorkerExecuteTime(6000 * 60 * 1000); // 工作线程允许的最大执行时间

      Vertx vertx = Vertx.vertx();

      System.out.println("Deploying TcpServerVerticle...");
      vertx.deployVerticle(new TcpServerVerticle()) // If port is hardcoded or read differently in Verticle
        .onSuccess(deploymentID -> {
          System.out.println("Tcp ServerVerticle deployed successfully with ID: " + deploymentID);
          // The logs from TcpServerVerticle will show the configured and actual ports.
        })
        .onFailure(err -> {
          System.err.println("TcpServerVerticle deployment failed: " + err.getMessage());
          err.printStackTrace(); // Good for seeing full stack trace of deployment failure
        });
      }
}
