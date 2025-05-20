package com.example.netclient;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.datagram.DatagramPacket;
import io.vertx.core.datagram.DatagramSocket;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.net.NetSocket;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class CustomPortmapperVerticle extends AbstractVerticle {

  private static final int RPCBIND_PORT = 8111;
  private static final int PORTMAPPER_PROGRAM = 100000;
  private static final int PORTMAPPER_VERSION = 2;
  private static final int PMAPPROC_GETPORT = 3;

  private static final int MSG_TYPE_CALL = 0;
  private static final int MSG_TYPE_REPLY = 1;

  private static final int REPLY_STAT_MSG_ACCEPTED = 0;
  private static final int ACCEPT_STAT_SUCCESS = 0;

  private static final int AUTH_NULL_FLAVOR = 0;
  private static final int AUTH_NULL_LENGTH = 0;

  private static final int IPPROTO_TCP = 6;
  private static final int IPPROTO_UDP = 17;

  // 存储我们“知道”的服务 (程序号 -> 版本号 -> 协议 -> 端口)
  private final Map<Integer, Map<Integer, Map<Integer, Integer>>> registeredServices = new HashMap<>();

  private static class RpcKey { // 辅助类作为 Map 的 Key
    int program;
    int version;
    int protocol;

    RpcKey(int program, int version, int protocol) {
      this.program = program;
      this.version = version;
      this.protocol = protocol;
    }
    // hashCode and equals necessary for Map keys
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      RpcKey rpcKey = (RpcKey) o;
      return program == rpcKey.program && version == rpcKey.version && protocol == rpcKey.protocol;
    }
    @Override
    public int hashCode() {
      return Objects.hash(program, version, protocol);
    }
  }
  private final Map<RpcKey, Integer> serviceRegistry = new HashMap<>();


  @Override
  public void start(Promise<Void> startPromise) {
    // 预注册一个示例服务
    // 比如，我们的自定义服务 MY_AWESOME_PROG (0x20000001), version 1, TCP, on port 9999
    registerService(0x20000001, 1, IPPROTO_TCP, 9999);
    registerService(0x20000001, 1, IPPROTO_UDP, 9998);


    // --- TCP Server ---
    NetServerOptions options = new NetServerOptions()
      .setPort(RPCBIND_PORT)
      .setHost("0.0.0.0")
      .setTcpKeepAlive(true);
    NetServer tcpServer = vertx.createNetServer(options);
    tcpServer.connectHandler(this::handleTcpConnection);
    tcpServer.listen(RPCBIND_PORT, "0.0.0.0")
      .onSuccess(server -> {
        System.out.println("TCP server listening on port " + RPCBIND_PORT);
      })
      .onFailure(cause -> {
        System.err.println("Failed to start server: " + cause.getMessage());
        vertx.close();
      });

  }

  public void registerService(int program, int version, int protocol, int port) {
    serviceRegistry.put(new RpcKey(program, version, protocol), port);
    System.out.println("Registered service: prog=" + Integer.toHexString(program) +
      ", vers=" + version + ", proto=" + protocol + ", port=" + port);
  }

  private void handleTcpConnection(NetSocket socket) {
//    socket.handler(buffer -> {
//      System.out.println("TCP: Received " + buffer.length() + " bytes from " + socket.remoteAddress());
//      Buffer reply = processRpcRequest(buffer);
//      if (reply != null) {
//        socket.write(reply);
//      } else {
//        // Optional: close connection if request is invalid or not handled
//        // socket.close();
//      }
//    });
//    socket.exceptionHandler(t -> System.err.println("TCP Socket Exception: " + t.getMessage()));
    System.out.println("SERVER: TCP Connection established from " + socket.remoteAddress() +
      " to " + socket.localAddress() + ". Socket ID: " + socket.toString()); // 添加套接字ID

    socket.handler(buffer -> {
      System.out.println("SERVER: Received " + buffer.length() + " bytes on socket " + socket.toString() +
        " from " + socket.remoteAddress());
      // ... (rest of your buffer processing)
    });

    socket.closeHandler(v -> {
      System.out.println("SERVER: TCP Connection closed by " + socket.remoteAddress() +
        ". Socket ID: " + socket.toString());
    });

    socket.exceptionHandler(t -> {
      System.err.println("SERVER: TCP Socket Exception on socket " + socket.toString() +
        " from " + socket.remoteAddress() + ": " + t.getMessage());
      t.printStackTrace(); // 打印完整堆栈
      // socket.close(); // 确保异常后关闭
    });
    System.out.println("SERVER: Handlers set for socket " + socket.toString());
  }

  private Buffer processRpcRequest(Buffer request) {
    // Basic validation: RPC call message for GETPORT is around 56 bytes
    if (request.length() < 56) {
      System.err.println("Request too short: " + request.length());
      return null;
    }

    System.out.println("Raw request buffer (" + request.length() + " bytes):");
    // 简单的十六进制打印
    for (int i = 0; i < request.length(); i++) {
      System.out.printf("%02X ", request.getByte(i));
      if ((i + 1) % 16 == 0 || i == request.length() - 1) {
        System.out.println();
      }
    }
    System.out.println("---- End of Raw Buffer ----");

    try {
      int xid = request.getInt(0);
      int msgType = request.getInt(4); // Should be CALL (0)
      // int rpcVersion = request.getInt(8); // Should be 2
      int program = request.getInt(12);
      int version = request.getInt(16);
      int procedure = request.getInt(20);

      // We only care about calls to portmapper program, version 2, procedure GETPORT
      if (msgType == MSG_TYPE_CALL &&
        program == PORTMAPPER_PROGRAM &&
        version == PORTMAPPER_VERSION &&
        procedure == PMAPPROC_GETPORT) {

        // Parse GETPORT arguments (offset starts after RPC header + cred + verf = 24 bytes)
        // Offset for prog_to_lookup is 24 (header) + 8 (cred) + 8 (verf) = 40
        int progToLookup = request.getInt(40);
        int versToLookup = request.getInt(44);
        int protToLookup = request.getInt(48);
        // int unused = request.getInt(52);

        System.out.println(String.format("GETPORT request: XID=0x%x, Prog=0x%x, Vers=%d, Prot=%d",
          xid, progToLookup, versToLookup, protToLookup));

        int portResult = serviceRegistry.getOrDefault(
          new RpcKey(progToLookup, versToLookup, protToLookup),
          0 // Default to 0 if not found
        );

        System.out.println("Responding with port: " + portResult);
        return createGetPortReply(xid, portResult);
      } else {
        System.out.println(String.format("Ignoring RPC call: XID=0x%x, Prog=0x%x, Vers=%d, Proc=%d",
          xid, program, version, procedure));
      }
    } catch (Exception e) {
      System.err.println("Error processing RPC request: " + e.getMessage());
      e.printStackTrace();
    }
    return null; // Or some error reply
  }

  private Buffer createGetPortReply(int xid, int port) {
    Buffer reply = Buffer.buffer(28); // Standard reply size for GETPORT
    // XID
    reply.appendInt(xid);
    // Message Type (REPLY = 1)
    reply.appendInt(MSG_TYPE_REPLY);
    // Reply Status (MSG_ACCEPTED = 0)
    reply.appendInt(REPLY_STAT_MSG_ACCEPTED);
    // Verifier (AUTH_NULL)
    reply.appendInt(AUTH_NULL_FLAVOR); // Flavor
    reply.appendInt(AUTH_NULL_LENGTH); // Length
    // Accept Status (SUCCESS = 0)
    reply.appendInt(ACCEPT_STAT_SUCCESS);
    // Port Number
    reply.appendInt(port);
    return reply;
  }

  public static void main(String[] args) {
    io.vertx.core.Vertx.vertx().deployVerticle(new CustomPortmapperVerticle());
  }
}
