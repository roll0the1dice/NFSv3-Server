package com.example.netclient;

import io.vertx.reactivex.core.Vertx;

public class MainMountApplication {
  public static void main(String[] args) {
    Vertx vertx = Vertx.vertx();

    // If TcpServerVerticle reads its port from config(), you can set it here:
    // DeploymentOptions deploymentOptions = new DeploymentOptions()
    //        .setConfig(new JsonObject().put("tcp.port", 23333)); // Example: pass port 23333

    System.out.println("Deploying MountServerVerticle...");
    vertx.deployVerticle(new MountVerticle()) // If port is hardcoded or read differently in Verticle
      .onSuccess(deploymentID -> {
        System.out.println("MountServerVerticle deployed successfully with ID: " + deploymentID);
        // The logs from TcpServerVerticle will show the configured and actual ports.
      })
      .onFailure(err -> {
        System.err.println("TcpServerVerticle deployment failed: " + err.getMessage());
        err.printStackTrace(); // Good for seeing full stack trace of deployment failure
      });
  }
}
