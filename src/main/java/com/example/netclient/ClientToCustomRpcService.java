package com.example.netclient;

import org.acplt.oncrpc.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.io.IOException;

public class ClientToCustomRpcService {

  public static final String SERVER_HOST = "localhost";
  public static final int CUSTOM_SERVICE_PORT = 8111;

  // 你的自定义服务的信息
  public static final int MY_CUSTOM_PROGRAM = 0x20000001;
  public static final int MY_CUSTOM_VERSION = 1;
  public static final int MY_PROCEDURE_TO_CALL = 1; // 假设你想调用过程1

  public static void main(String[] args) {
    // --- Configuration ---
    String serverHost = "localhost"; // !!! CHANGE THIS !!!
    int programNumber = 100003; // NFS Program Number
    int versionNumber = 3;      // NFS Version 3
    int procedureNumber = 0;    // NULL Procedure

    OncRpcClient client = null;

    System.out.println("Attempting to connect to RPC server: " + serverHost);
    System.out.println("Program: " + programNumber + ", Version: " + versionNumber + ", Procedure: " + procedureNumber);

    try {
      // 1. Resolve server address
      InetAddress serverAddress = InetAddress.getByName(serverHost);

      // 2. Create an ONC RPC client (TCP in this case)
      // The constructor will implicitly try to contact the portmapper on the server
      // to find the actual port for the (program, version, protocol) triplet.
      client = new OncRpcTcpClient(
        serverAddress,  // Server's IP address
        programNumber,  // RPC Program number
        versionNumber,  // RPC Version number
        12345               // Port number (0 means use portmapper)
      );

      // You can set a timeout for RPC calls (in milliseconds)
      client.setTimeout(5000); // 5 seconds

      // 3. (Optional) Set Authentication
      // For NULL procedure on many services, AUTH_NONE is sufficient (default).
      // If AUTH_SYS is needed:
      // OncRpcClientAuth auth = new OncRpcClientAuthUnix("clienthostname", 0, 0, new int[]{0});
      // client.setAuth(auth);

      System.out.println("Successfully created OncRpcTcpClient. Attempting to call procedure...");

      // 4. Prepare arguments and result placeholders
      // For a NULL procedure, the argument and result are 'void'.
      // org.acplt.oncrpc.XdrVoid is used to represent this.
      XdrVoid rpcArgs = XdrVoid.XDR_VOID;
      XdrVoid rpcResult = XdrVoid.XDR_VOID; // This will not be filled with meaningful data for a void result

      // 5. Make the RPC call
      client.call(
        procedureNumber, // The procedure to call (e.g., NULLPROC)
        rpcArgs,         // Arguments (XdrAble object)
        rpcResult        // Result placeholder (XdrAble object)
      );

      System.out.println("RPC NULL procedure call successful!");

    } catch (OncRpcTimeoutException e) {
      System.err.println("RPC call timed out: " + e.getMessage());
      System.err.println("Ensure the server '" + serverHost + "' is reachable and the RPC service (Program: " + programNumber + ", Version: " + versionNumber + ") is running and registered with portmapper/rpcbind.");
    } catch (OncRpcException e) {
      // This can catch various RPC errors like:
      // - Program not available
      // - Version mismatch
      // - Procedure unavailable
      // - Authentication errors
      // - Portmapper errors
      System.err.println("ONC RPC Error: " + e.getMessage());
      System.err.println("RPC Error (from server or library): " + e.getReason());
      if (e.getCause() != null) {
        System.err.println("Caused by: " + e.getCause().getMessage());
      }
      // e.printStackTrace(); // For more detailed debugging
    } catch (IOException e) {
      // This can catch network errors like "Connection refused" if portmapper is down
      // or server is unreachable, or errors during InetAddress.getByName.
      System.err.println("IO Error (Network or Host Resolution): " + e.getMessage());
      if (e.getMessage() != null && e.getMessage().contains("Connection refused")) {
        System.err.println("Hint: Check if portmapper (rpcbind) is running on port 111 on the server '" + serverHost + "'.");
      }
      // e.printStackTrace(); // For more detailed debugging
    } catch (Exception e) {
      System.err.println("An unexpected error occurred: " + e.getMessage());
      e.printStackTrace();
    } finally {
      // 6. Close the client connection
      if (client != null) {
        try {
          client.close();
          System.out.println("Client connection closed.");
        } catch (OncRpcException e) {
          System.err.println("Error closing client: " + e.getMessage());
        }
      }
    }
  }
}
