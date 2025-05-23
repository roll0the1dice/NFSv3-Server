package com.example.netclient;

import org.acplt.oncrpc.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

public class CustomOncRpcPortmapClient {
  public static final String SERVER_HOST = "localhost";
  //public static final int CUSTOM_PORTMAPPER_PORT = 8111; // 你自定义的 "portmapper" 监听的端口

  // 你要查询的目标服务的信息 (这些是 getPort 方法的参数)
  public static final int TARGET_PROGRAM_NUMBER = 100003;
  public static final int TARGET_PROGRAM_VERSION = 3;

  public static void main(String[] args) {
    OncRpcPortmapClient portmapClient = null;
    try {
      System.out.println("CLIENT: Creating OncRpcPortmapClient to connect to " +
        SERVER_HOST);

      // 构造 InetSocketAddress 指向你的自定义 "portmapper" 服务
      //InetSocketAddress customPortmapperAddress = new InetSocketAddress(InetAddress.getByName(SERVER_HOST));
      InetAddress serverAddress = InetAddress.getByName(SERVER_HOST);



      // 使用 OncRpcPortmapClient(InetSocketAddress, int program, int version, int protocol)
      // program 和 version 这里指的是 portmapper 自身的程序号和版本号。
      portmapClient = new OncRpcPortmapClient(
        serverAddress,                // 连接到这个地址
        OncRpcProtocols.ONCRPC_TCP              // 使用 TCP 与 "portmapper" 通信
      );

      System.out.println("CLIENT: OncRpcPortmapClient (potentially to custom port) created.");
      System.out.println("CLIENT: Querying port for program " + Integer.toHexString(TARGET_PROGRAM_NUMBER) +
        " version " + TARGET_PROGRAM_VERSION + " protocol TCP...");

      // 这个 getPort 调用仍然是向上面配置的 "portmapper" (现在在8111) 发送 GETPORT 请求
      int resolvedPort = portmapClient.getPort(
        TARGET_PROGRAM_NUMBER,
        TARGET_PROGRAM_VERSION,
        OncRpcProtocols.ONCRPC_TCP
      );

      System.out.println("CLIENT: Resolved port: " + resolvedPort);

    } catch (OncRpcException e) {
      System.err.println("CLIENT ERROR: OncRpcException: " + e.getMessage());
      e.printStackTrace();
    } catch (IOException e) {
      System.err.println("CLIENT ERROR: IOException: " + e.getMessage());
      e.printStackTrace();
    } finally {
      if (portmapClient != null) {
        try {
          portmapClient.close();
        } catch (Exception e) { /* ignore */ }
      }
    }
  }
}
