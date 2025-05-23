package com.example.netclient;

import com.example.netclient.utils.NetTool;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.DecoderException;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;


@Slf4j
public class MountVerticle extends AbstractVerticle {
  private static final int PORT = 23333; // 服务器监听的端口
  private static final String HOST = "0.0.0.0"; // 监听所有网络接口

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    // 创建 NetServerOptions (可选，用于配置服务器)
    NetServerOptions options = new NetServerOptions()
      .setPort(PORT)
      .setHost(HOST)
      .setTcpKeepAlive(true); // 示例：启用 TCP KeepAlive

    // 创建 TCP 服务器
    NetServer server = vertx.createNetServer(options);

    // 设置连接处理器
    server.connectHandler(socket -> {
      log.info("客户端连接成功: " + socket.remoteAddress());

      // 为每个连接的 socket 设置数据处理器
      socket.handler(buffer -> {
        String receivedData = buffer.toString("UTF-8");
        log.info("从客户端 [" + socket.remoteAddress() + "] 收到数据大小: " + receivedData.length());

        log.info("Raw request buffer (" + buffer.length() + " bytes):");
        // 简单的十六进制打印
        for (int i = 0; i < buffer.length(); i++) {
          System.out.printf("%02X ", buffer.getByte(i));
          if ((i + 1) % 16 == 0 || i == buffer.length() - 1) {
            System.out.println();
          }
        }
        log.info("---- End of Raw Buffer ----");

        int recordMakerRaw = buffer.getInt(0);
        int xid = buffer.getInt(4);
        int msgType = buffer.getInt(8); // Should be CALL (0)
        int rpcVersion = buffer.getInt(12); // Should be 2
        int programNumber = buffer.getInt(16);
        int programVersion = buffer.getInt(20);
        int procedureNumber = buffer.getInt(24);
        int credentialsFlavor = buffer.getInt(28);
        int credentialsBodyLength = buffer.getInt(32);
        CredentialsInRPC credentialsInRPC = null;
        if (credentialsBodyLength > 0) {
          credentialsInRPC = new CredentialsInRPC(buffer.slice(36, 36 + credentialsBodyLength).getBytes());
        }
        int startOffset = 36 + credentialsBodyLength;
        int verifierFlavor = buffer.getInt(startOffset);
        int verifierLength = buffer.getInt(startOffset + 4);
        int pathLength;
        String path = "";
        // MNT 调用
        if (buffer.length() > 44 && procedureNumber == 1) {
          pathLength = buffer.getInt(startOffset + 8);
          try {
            path = new String(buffer.slice(startOffset + 12, startOffset + 12 + pathLength).getBytes(), "UTF-8").trim();
          } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
          }
        }

        log.info("path: {}, pathOfLength: {}", path, path.length());

        if (procedureNumber == 0) {
          byte[] xdrReplyBytes = createNfsNullReply(xid);

          log.info("Raw response buffer (" + buffer.length() + " bytes):");
          // 简单的十六进制打印
          for (int i = 0; i < xdrReplyBytes.length; i++) {
            System.out.printf("%02X ", xdrReplyBytes[i]);
            if ((i + 1) % 16 == 0 || i == xdrReplyBytes.length - 1) {
              System.out.println();
            }
          }
          log.info("---- End of Raw response Buffer ----");

          socket.write(Buffer.buffer(xdrReplyBytes));
        } else if (procedureNumber == 1) {
          byte[] xdrReplyBytes = null;
          try {
            xdrReplyBytes = createNfsMNTReply(xid);
          } catch (DecoderException e) {
            throw new RuntimeException(e);
          }

          log.info("Raw response buffer (" + buffer.length() + " bytes):");
          // 简单的十六进制打印
          for (int i = 0; i < xdrReplyBytes.length; i++) {
            System.out.printf("%02X ", xdrReplyBytes[i]);
            if ((i + 1) % 16 == 0 || i == xdrReplyBytes.length - 1) {
              System.out.println();
            }
          }
          log.info("---- End of Raw response Buffer ----");

          socket.write(Buffer.buffer(xdrReplyBytes));
        }

        // 如果客户端发送 "quit"，则关闭连接
        if ("quit".equalsIgnoreCase(receivedData.trim())) {
          log.info("客户端 [" + socket.remoteAddress() + "] 请求关闭连接。");
          socket.close();
        }
      });

      // 设置关闭处理器
      socket.closeHandler(v -> {
        log.info("客户端断开连接: " + socket.remoteAddress());
      });

      // 设置异常处理器
      socket.exceptionHandler(throwable -> {
        System.err.println("客户端 [" + socket.remoteAddress() + "] 发生错误: " + throwable.getMessage());
        socket.close(); // 发生错误时关闭连接
      });
    });

    // 启动服务器并监听端口
    server.listen(PORT).onSuccess(s -> {
      log.info("Server started on host " + " and port " + s.actualPort());
    }).onFailure(cause -> {
      System.err.println("Failed to start server: " + cause.getMessage());
      vertx.close();
    });

    // 也可以直接指定端口和主机，而不使用 NetServerOptions
    // server.listen(PORT, HOST, res -> { /* ... */ });
  }

  @Override
  public void stop(Promise<Void> stopPromise) throws Exception {
    log.info("TCP 服务器正在关闭...");
    // 可以在这里添加关闭服务器的逻辑，但通常 Vert.x 会自动处理
    stopPromise.complete();
  }
  // 主方法用于部署 Verticle (方便测试)
  public static void main(String[] args) {
    Vertx vertx = Vertx.vertx();

    vertx.deployVerticle(new TcpServerVerticle()) // MySimpleVerticle must have a no-arg constructor
      .onSuccess(deploymentID -> log.info("Deployed class with ID: " + deploymentID))
      .onFailure(err -> System.err.println("Deployment failed: " + err.getMessage()));
  }

  // RPC Constants (values are in decimal for Java int literals)
  private static final int MSG_TYPE_REPLY = 1;          // 0x00000001
  private static final int REPLY_STAT_MSG_ACCEPTED = 0; // 0x00000000
  private static final int VERF_FLAVOR_AUTH_NONE = 0;   // 0x00000000
  private static final int VERF_LENGTH_ZERO = 0;        // 0x00000000
  private static final int ACCEPT_STAT_SUCCESS = 0;     // 0x00000000
  private static final int MOUNT_STATUS_OK = 0;
  private static final int MOUNT_FLAVORS = 1;
  private static final int MOUNT_FLAVOR_AUTH_UNIX = 1;

  public static byte[] createNfsNullReply(int requestXid) {
    // --- Calculate RPC Message Body Length ---
    // XID (4 bytes)
    // Message Type (4 bytes)
    // Reply Status (4 bytes)
    // Verifier Flavor (4 bytes)
    // Verifier Length (4 bytes)
    // Acceptance Status (4 bytes)
    // Total = 6 * 4 = 24 bytes
    final int rpcMessageBodyLength = 24;

    // --- Create ByteBuffer for the RPC Message Body ---
    // We will fill this first, then prepend the record mark.
    ByteBuffer rpcBodyBuffer = ByteBuffer.allocate(rpcMessageBodyLength);
    rpcBodyBuffer.order(ByteOrder.BIG_ENDIAN); // XDR is Big Endian

    // 1. XID (Transaction Identifier) - from request
    rpcBodyBuffer.putInt(requestXid);

    // 2. Message Type (mtype)
    rpcBodyBuffer.putInt(MSG_TYPE_REPLY);

    // 3. Reply Body (reply_body)
    //    3.1. Reply Status (stat of union switch (msg_type mtype))
    rpcBodyBuffer.putInt(REPLY_STAT_MSG_ACCEPTED);

    //    3.2. Accepted Reply (areply)
    //        3.2.1. Verifier (verf - opaque_auth structure)
    rpcBodyBuffer.putInt(VERF_FLAVOR_AUTH_NONE); // Flavor
    rpcBodyBuffer.putInt(VERF_LENGTH_ZERO);      // Length of body (0 for AUTH_NONE)
    // Body is empty

    //        3.2.2. Acceptance Status (stat of union switch (accept_stat stat))
    rpcBodyBuffer.putInt(ACCEPT_STAT_SUCCESS);

    //        3.2.3. Results (for NFSPROC3_NULL, this is void, so no data)

    // --- Construct Record Marking ---
    // Highest bit set (0x80000000) ORed with the length of the RPC message body.
    // In Java, an int is 32-bit.
    int recordMarkValue = 0x80000000 | rpcMessageBodyLength;

    // --- Create ByteBuffer for the Full XDR Response ---
    // Record Mark (4 bytes) + RPC Message Body (rpcMessageBodyLength bytes)
    ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcMessageBodyLength);
    fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);

    // Put the record mark
    fullResponseBuffer.putInt(recordMarkValue);
    // Put the RPC message body (which is already in rpcBodyBuffer)
    fullResponseBuffer.put(rpcBodyBuffer.array()); // .array() gets the underlying byte array

    // Return the complete byte array
    return fullResponseBuffer.array();
  }

  public static byte[] createNfsMNTReply(int requestXid) throws DecoderException {
    // --- Calculate RPC Message Body Length ---
    // XID (4 bytes)
    // Message Type (4 bytes)
    // Reply Status (4 bytes)
    // Verifier Flavor (4 bytes)
    // Verifier Length (4 bytes)
    // Acceptance Status (4 bytes)   ---- up to this, Total = 6 * 4 = 24 bytes
    // Mount Service
    //    Status (4 bytes)
    //    fhandle
    //      length
    //      FileHandleData
    //    Flavors
    //    Flavor
    // Total = 6 * 4 = 24 bytes
    final int rpcMessageBodyLength = 24;

    // --- Create ByteBuffer for the RPC Message Body ---
    // We will fill this first, then prepend the record mark.
    ByteBuffer rpcBodyBuffer = ByteBuffer.allocate(rpcMessageBodyLength);
    rpcBodyBuffer.order(ByteOrder.BIG_ENDIAN); // XDR is Big Endian

    // 1. XID (Transaction Identifier) - from request
    rpcBodyBuffer.putInt(requestXid);

    // 2. Message Type (mtype)
    rpcBodyBuffer.putInt(MSG_TYPE_REPLY);

    // 3. Reply Body (reply_body)
    //    3.1. Reply Status (stat of union switch (msg_type mtype))
    rpcBodyBuffer.putInt(REPLY_STAT_MSG_ACCEPTED);

    //    3.2. Accepted Reply (areply)
    //        3.2.1. Verifier (verf - opaque_auth structure)
    rpcBodyBuffer.putInt(VERF_FLAVOR_AUTH_NONE); // Flavor
    rpcBodyBuffer.putInt(VERF_LENGTH_ZERO);      // Length of body (0 for AUTH_NONE)
    // Body is empty

    //        3.2.2. Acceptance Status (stat of union switch (accept_stat stat))
    rpcBodyBuffer.putInt(ACCEPT_STAT_SUCCESS);

    //        3.2.3. Results (for NFSPROC3_NULL, this is void, so no data)
    int rpcMountLength = 4 + 4 + 28 + 4 + 4;
    ByteBuffer rpcMountBuffer = ByteBuffer.allocate(rpcMountLength);
    rpcBodyBuffer.order(ByteOrder.BIG_ENDIAN); // XDR is Big Endian

    rpcMountBuffer.putInt(MOUNT_STATUS_OK);
    rpcMountBuffer.putInt(0x0000001C);
    String dataLiteral = "0100070002000002000000003e3e7dae34c9471896e6218574c98110";
    rpcMountBuffer.put(NetTool.hexStringToByteArray(dataLiteral));
    rpcMountBuffer.putInt(MOUNT_FLAVORS);
    rpcMountBuffer.putInt(MOUNT_FLAVOR_AUTH_UNIX);

    // --- Construct Record Marking ---
    // Highest bit set (0x80000000) ORed with the length of the RPC message body.
    // In Java, an int is 32-bit.
    int recordMarkValue = 0x80000000 + rpcMessageBodyLength + rpcMountLength;

    log.info("total RPC length: " + (rpcMountLength + rpcMessageBodyLength));

    // --- Create ByteBuffer for the Full XDR Response ---
    // Record Mark (4 bytes) + RPC Message Body (rpcMessageBodyLength bytes)
    ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcMessageBodyLength + rpcMountLength);
    fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);

    // Put the record mark
    fullResponseBuffer.putInt(recordMarkValue);
    // Put the RPC message body (which is already in rpcBodyBuffer)
    fullResponseBuffer.put(rpcBodyBuffer.array()); // .array() gets the underlying byte array
    fullResponseBuffer.put(rpcMountBuffer.array());

    // Return the complete byte array
    return fullResponseBuffer.array();
  }
  // Helper method to print byte array as hex string for verification
  public static String bytesToHex(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    for (byte b : bytes) {
      sb.append(String.format("%02X ", b));
    }
    return sb.toString().trim();
  }


  public static void handleRPCRequest(Buffer buffer) {

  }

}
