package com.example.netclient;

import com.example.netclient.enums.RpcReplyMessage;
import com.example.netclient.enums.MountProcedure;
import com.example.netclient.enums.MountStatus;
import com.example.netclient.enums.RpcParseState;
import com.example.netclient.utils.EnumUtil;
import com.example.netclient.utils.NetTool;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.net.NetSocket;
import io.vertx.core.parsetools.RecordParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.DecoderException;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;


@Slf4j
public class MountVerticle extends AbstractVerticle {
  private static final int MOUNT_PROGRAM = 100005;
  private static final int MOUNT_VERSION = 3;
  private static final int MOUNT_STATUS_OK = 0;
  private static final int MOUNT_FLAVORS = 1;
  private static final int MOUNT_FLAVOR_AUTH_UNIX = 1;

  private static final int PORT = 23333; // 服务器监听的端口
  private static final String HOST = "0.0.0.0"; // 监听所有网络接口

  private RpcParseState currentState = RpcParseState.READING_MARKER;
  private boolean isLastFragment = true;
  private int expectedFragmentLength = 0;
  private List<Buffer> messageFragments = new ArrayList<>();

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
      RecordParser recordParser = RecordParser.newFixed(4);

      recordParser.handler(buffer -> {
        // 读头标记模式
        if (currentState == RpcParseState.READING_MARKER) {
          int recordMarkerRaw = buffer.getInt(0);
          isLastFragment = (0x80000000 & recordMarkerRaw) != 0;
          expectedFragmentLength = recordMarkerRaw & 0x7FFFFFFF;

          // 心跳检测
          if (expectedFragmentLength == 0) {
            // 下一个片段
            recordParser.fixedSizeMode(4);
            currentState = RpcParseState.READING_MARKER;
          } else {
            // 根据获取到头标记中的片段的大小，开启读片段模式
            recordParser.fixedSizeMode(expectedFragmentLength);
            currentState = RpcParseState.READING_FRAGMENT_DATA;
          }
        } else if (currentState == RpcParseState.READING_FRAGMENT_DATA) {
          messageFragments.add(buffer);
          // 整个片段读取完成，开启下一轮片段的读取（先读段头中的标记）
          recordParser.fixedSizeMode(4);
          currentState = RpcParseState.READING_MARKER;

          // 如果这个片段是请求的最后一个片段
          if (isLastFragment) {
            handleRpcRequest(socket);
          }

        } else {
          throw new IllegalStateException("Unexpected state: " + currentState);
        }
      });

      // 为每个连接的 socket 设置数据处理器
      socket.handler(recordParser);

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

  private void handleRpcRequest(NetSocket socket) {
    Buffer buffer = Buffer.buffer();
    for (Buffer fragment : messageFragments) {
      buffer.appendBuffer(fragment);
    }
    messageFragments.clear();

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


    int xid = buffer.getInt(0);
    int msgType = buffer.getInt(4); // Should be CALL (0)
    int rpcVersion = buffer.getInt(8); // Should be 2
    int programNumber = buffer.getInt(12);
    int programVersion = buffer.getInt(16);
    int procedureNumber = buffer.getInt(20);
    int credentialsFlavor = buffer.getInt(24);
    int credentialsBodyLength = buffer.getInt(28);
    CredentialsInRPC credentialsInRPC = null;
    if (credentialsBodyLength > 0) {
      credentialsInRPC = new CredentialsInRPC(buffer.slice(32, 32 + credentialsBodyLength).getBytes());
    }
    int startOffset = 32 + credentialsBodyLength;
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

    log.info("NFS Request - XID: 0x{}, Program: {}, Version: {}, Procedure: {}",
    Integer.toHexString(xid), programNumber, programVersion, procedureNumber);

    /* 验证程序号和版本号 */
    if (programNumber != MOUNT_PROGRAM || programVersion != MOUNT_VERSION) {
      log.error("Invalid program number or version: program={}, version={}", programNumber, programVersion);
      return;
    }

    MountProcedure procedureNumberEnum = EnumUtil.fromCode(MountProcedure.class, procedureNumber);

    byte[] xdrReplyBytes = null;
    try {
      switch (procedureNumberEnum) {
        case MOUNTPROC_NULL:
          xdrReplyBytes = createNfsNullReply(xid);
          break;
        case MOUNTPROC_MNT:
          xdrReplyBytes = createNfsMNTReply(xid, path);
          break;
        case MOUNTPROC_DUMP:
          xdrReplyBytes = createNfsDumpReply(xid);
          break;
        case MOUNTPROC_UMNT:
          xdrReplyBytes = createNfsUMNTReply(xid);
          break;
        case MOUNTPROC_UMNTALL:
          xdrReplyBytes = createNfsUMNTALLReply(xid);
          break;
        case MOUNTPROC_EXPORT:
          xdrReplyBytes = createNfsExportReply(xid);
          break;
        default:
          log.error("Unsupported procedure number: {}", procedureNumber);
          return;
      }

      if (xdrReplyBytes != null) {
          log.info("Raw response buffer (" + xdrReplyBytes.length + " bytes):");
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
    } catch (Exception e) {
      log.error("Error processing MOUNT request", e);
    }

    // 如果客户端发送 "quit"，则关闭连接
    if ("quit".equalsIgnoreCase(receivedData.trim())) {
      log.info("客户端 [" + socket.remoteAddress() + "] 请求关闭连接。");
      socket.close();
    }
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
    rpcBodyBuffer.putInt(RpcReplyMessage.MSG_TYPE_REPLY.getCode());

    // 3. Reply Body (reply_body)
    //    3.1. Reply Status (stat of union switch (msg_type mtype))
    rpcBodyBuffer.putInt(RpcReplyMessage.REPLY_STAT_MSG_ACCEPTED.getCode());

    //    3.2. Accepted Reply (areply)
    //        3.2.1. Verifier (verf - opaque_auth structure)
    rpcBodyBuffer.putInt(RpcReplyMessage.VERF_FLAVOR_AUTH_NONE.getCode()); // Flavor
    rpcBodyBuffer.putInt(RpcReplyMessage.VERF_LENGTH_ZERO.getCode());      // Length of body (0 for AUTH_NONE)
    // Body is empty

    //        3.2.2. Acceptance Status (stat of union switch (accept_stat stat))
    rpcBodyBuffer.putInt(RpcReplyMessage.ACCEPT_STAT_SUCCESS.getCode());

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

  public static byte[] createNfsMNTReply(int requestXid, String path) throws DecoderException {
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
    final int rpcMessageBodyLength = 24;

    // --- Create ByteBuffer for the RPC Message Body ---
    ByteBuffer rpcBodyBuffer = ByteBuffer.allocate(rpcMessageBodyLength);
    rpcBodyBuffer.order(ByteOrder.BIG_ENDIAN);

    // 1. XID (Transaction Identifier) - from request
    rpcBodyBuffer.putInt(requestXid);

    // 2. Message Type (mtype)
    rpcBodyBuffer.putInt(RpcReplyMessage.MSG_TYPE_REPLY.getCode());

    // 3. Reply Body (reply_body)
    //    3.1. Reply Status (stat of union switch (msg_type mtype))
    rpcBodyBuffer.putInt(RpcReplyMessage.REPLY_STAT_MSG_ACCEPTED.getCode());

    //    3.2. Accepted Reply (areply)
    //        3.2.1. Verifier (verf - opaque_auth structure)
    rpcBodyBuffer.putInt(RpcReplyMessage.VERF_FLAVOR_AUTH_NONE.getCode()); // Flavor
    rpcBodyBuffer.putInt(RpcReplyMessage.VERF_LENGTH_ZERO.getCode());      // Length of body (0 for AUTH_NONE)

    //        3.2.2. Acceptance Status (stat of union switch (accept_stat stat))
    rpcBodyBuffer.putInt(RpcReplyMessage.ACCEPT_STAT_SUCCESS.getCode());

    // Mount Service Reply
    int rpcMountLength = 4 + 4 + 28 + 4 + 4;
    ByteBuffer rpcMountBuffer = ByteBuffer.allocate(rpcMountLength);
    rpcMountBuffer.order(ByteOrder.BIG_ENDIAN);

    // Check if path exists and is accessible
    int mountStatus = MountStatus.MNT_OK.getCode();

    // actual path validation logic here
    if (!path.startsWith("/")) {
      mountStatus = MountStatus.MNT_ERR_INVAL.getCode();
    }

    rpcMountBuffer.putInt(mountStatus);
    if (mountStatus == MountStatus.MNT_OK.getCode()) {
      rpcMountBuffer.putInt(0x0000001C); // File handle length
      // Create a file handle using the provided format
      // Format from the provided hex dump:
      // 01 00 07 00 02 00 00 02 00 00 00 00 3e 3e 7d ae 34 c9 47 18 96 e6 21 85 74 c9 81 10
      String dataLiteral = "0100070002000002000000003e3e7dae34c9471896e6218574c98110";
      rpcMountBuffer.put(NetTool.hexStringToByteArray(dataLiteral));

      rpcMountBuffer.putInt(MOUNT_FLAVORS);
      rpcMountBuffer.putInt(MOUNT_FLAVOR_AUTH_UNIX);
    }

    // --- Construct Record Marking ---
    int recordMarkValue = 0x80000000 | (rpcMessageBodyLength + rpcMountLength);

    // --- Create ByteBuffer for the Full XDR Response ---
    ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcMessageBodyLength + rpcMountLength);
    fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);

    // Put the record mark
    fullResponseBuffer.putInt(recordMarkValue);
    // Put the RPC message body
    fullResponseBuffer.put(rpcBodyBuffer.array());
    fullResponseBuffer.put(rpcMountBuffer.array());

    return fullResponseBuffer.array();
  }

  public static byte[] createNfsDumpReply(int requestXid) {
    final int rpcMessageBodyLength = 24;
    ByteBuffer rpcReplyHeaderBuffer = ByteBuffer.allocate(rpcMessageBodyLength);
    rpcReplyHeaderBuffer.order(ByteOrder.BIG_ENDIAN);

    // Standard RPC reply header
    rpcReplyHeaderBuffer.putInt(requestXid);
    rpcReplyHeaderBuffer.putInt(RpcReplyMessage.MSG_TYPE_REPLY.getCode());
    rpcReplyHeaderBuffer.putInt(RpcReplyMessage.REPLY_STAT_MSG_ACCEPTED.getCode());
    rpcReplyHeaderBuffer.putInt(RpcReplyMessage.VERF_FLAVOR_AUTH_NONE.getCode());
    rpcReplyHeaderBuffer.putInt(RpcReplyMessage.VERF_LENGTH_ZERO.getCode());
    rpcReplyHeaderBuffer.putInt(RpcReplyMessage.ACCEPT_STAT_SUCCESS.getCode());

    // DUMP reply is just an empty list
    int rpcDumpLength = 4; // Just the length of the list (0)
    ByteBuffer rpcDumpBuffer = ByteBuffer.allocate(rpcDumpLength);
    rpcDumpBuffer.order(ByteOrder.BIG_ENDIAN);
    rpcDumpBuffer.putInt(0); // Empty list

    // Record marking
    int recordMarkValue = 0x80000000 | (rpcMessageBodyLength + rpcDumpLength);

    ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcMessageBodyLength + rpcDumpLength);
    fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);
    fullResponseBuffer.putInt(recordMarkValue);
    fullResponseBuffer.put(rpcReplyHeaderBuffer.array());
    fullResponseBuffer.put(rpcDumpBuffer.array());

    return fullResponseBuffer.array();
  }

  public static byte[] createNfsUMNTReply(int requestXid) {
    final int rpcMessageBodyLength = 24;
    ByteBuffer rpcBodyBuffer = ByteBuffer.allocate(rpcMessageBodyLength);
    rpcBodyBuffer.order(ByteOrder.BIG_ENDIAN);

    // Standard RPC reply header
    rpcBodyBuffer.putInt(requestXid);
    rpcBodyBuffer.putInt(RpcReplyMessage.MSG_TYPE_REPLY.getCode());
    rpcBodyBuffer.putInt(RpcReplyMessage.REPLY_STAT_MSG_ACCEPTED.getCode());
    rpcBodyBuffer.putInt(RpcReplyMessage.VERF_FLAVOR_AUTH_NONE.getCode());
    rpcBodyBuffer.putInt(RpcReplyMessage.VERF_LENGTH_ZERO.getCode());
    rpcBodyBuffer.putInt(RpcReplyMessage.ACCEPT_STAT_SUCCESS.getCode());

    // UMNT reply is void
    int recordMarkValue = 0x80000000 | rpcMessageBodyLength;

    ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcMessageBodyLength);
    fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);
    fullResponseBuffer.putInt(recordMarkValue);
    fullResponseBuffer.put(rpcBodyBuffer.array());

    return fullResponseBuffer.array();
  }

  public static byte[] createNfsUMNTALLReply(int requestXid) {
    // UMNTALL reply is identical to UMNT reply
    return createNfsUMNTReply(requestXid);
  }

  public static byte[] createNfsExportReply(int requestXid) {
    final int rpcMessageBodyLength = 24;
    ByteBuffer rpcBodyBuffer = ByteBuffer.allocate(rpcMessageBodyLength);
    rpcBodyBuffer.order(ByteOrder.BIG_ENDIAN);

    // Standard RPC reply header
    rpcBodyBuffer.putInt(requestXid);
    rpcBodyBuffer.putInt(RpcReplyMessage.MSG_TYPE_REPLY.getCode());
    rpcBodyBuffer.putInt(RpcReplyMessage.REPLY_STAT_MSG_ACCEPTED.getCode());
    rpcBodyBuffer.putInt(RpcReplyMessage.VERF_FLAVOR_AUTH_NONE.getCode());
    rpcBodyBuffer.putInt(RpcReplyMessage.VERF_LENGTH_ZERO.getCode());
    rpcBodyBuffer.putInt(RpcReplyMessage.ACCEPT_STAT_SUCCESS.getCode());

    // Export list with one entry
    int rpcExportLength = 4 + 4 + 4 + 4 + 4; // List length + path length + path + groups length
    ByteBuffer rpcExportBuffer = ByteBuffer.allocate(rpcExportLength);
    rpcExportBuffer.order(ByteOrder.BIG_ENDIAN);

    rpcExportBuffer.putInt(1); // One export
    rpcExportBuffer.putInt(4); // Path length
    rpcExportBuffer.putInt(0); // No groups
    rpcExportBuffer.putInt(0); // No groups length

    // Record marking
    int recordMarkValue = 0x80000000 | (rpcMessageBodyLength + rpcExportLength);

    ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcMessageBodyLength + rpcExportLength);
    fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);
    fullResponseBuffer.putInt(recordMarkValue);
    fullResponseBuffer.put(rpcBodyBuffer.array());
    fullResponseBuffer.put(rpcExportBuffer.array());

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

}
