package com.example.netclient;

import io.vertx.reactivex.core.Vertx;

public class MainTcpApplication {
  public static void main(String[] args) {
      Vertx vertx = Vertx.vertx();

      System.out.println("Deploying TcpServerVerticle...");
      vertx.deployVerticle(new TcpServerVerticle()) // If port is hardcoded or read differently in Verticle
        .onSuccess(deploymentID -> {
          System.out.println("TcpServerVerticle deployed successfully with ID: " + deploymentID);
          // The logs from TcpServerVerticle will show the configured and actual ports.
        })
        .onFailure(err -> {
          System.err.println("TcpServerVerticle deployment failed: " + err.getMessage());
          err.printStackTrace(); // Good for seeing full stack trace of deployment failure
        });
      }
}
