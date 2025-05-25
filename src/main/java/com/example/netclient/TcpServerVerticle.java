package com.example.netclient;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.net.NetSocket;
import lombok.extern.slf4j.Slf4j;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import com.example.netclient.util.NetTool;


@Slf4j
public class TcpServerVerticle extends AbstractVerticle {

  private static final int PORT = 12345; // 服务器监听的端口
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
        log.info("---- End of Raw request Buffer ----");

        // Parse RPC header
        int recordMakerRaw = buffer.getInt(0);
        int xid = buffer.getInt(4);
        int msgType = buffer.getInt(8); // Should be CALL (0)
        int rpcVersion = buffer.getInt(12); // Should be 2
        int programNumber = buffer.getInt(16);
        int programVersion = buffer.getInt(20);
        int procedureNumber = buffer.getInt(24);

        // Handle NFS requests
        if (programNumber == NFS_PROGRAM && programVersion == NFS_VERSION) {
          handleNFSRequest(buffer, socket);
        }
        // Handle NFS_ACL requests
        else if (programNumber == NFS_ACL_PROGRAM && programVersion == NFS_ACL_VERSION) {
          handleNFSACLRequest(buffer, socket);
        }
        // Handle MOUNT requests
        else if (programNumber == MOUNT_PROGRAM && programVersion == MOUNT_VERSION) {
          // TODO: Implement MOUNT request handling
          log.info("MOUNT request received - XID: 0x{}, Procedure: {}", 
              Integer.toHexString(xid), procedureNumber);
        }
        else {
          log.error("Unsupported program: program={}, version={}", programNumber, programVersion);
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
  // Helper method to print byte array as hex string for verification
  public static String bytesToHex(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    for (byte b : bytes) {
      sb.append(String.format("%02X ", b));
    }
    return sb.toString().trim();
  }

  public static int extractXid(byte[] xdrRequestBytes) {
    // XID is 4 bytes long and starts after the 4-byte record marking.
    // So, it starts at offset 4.
    final int XID_OFFSET = 4;
    final int XID_LENGTH = 4;

    if (xdrRequestBytes == null || xdrRequestBytes.length < XID_OFFSET + XID_LENGTH) {
      throw new IllegalArgumentException("XDR request byte array is too short to contain an XID. Minimum length: " + (XID_OFFSET + XID_LENGTH) + " bytes.");
    }

    // Wrap the relevant part of the byte array (or the whole array and set position)
    // into a ByteBuffer to easily read an int.
    ByteBuffer buffer = ByteBuffer.wrap(xdrRequestBytes);
    buffer.order(ByteOrder.BIG_ENDIAN); // XDR is Big Endian

    // Move the buffer's position to the start of the XID
    buffer.position(XID_OFFSET);

    // Read the 4-byte integer which is the XID
    int xid = buffer.getInt();

    return xid;
  }

  public static void handleRPCRequest(Buffer buffer) {
    List<byte[]> fragmentDataList = new ArrayList<>();

    boolean lastFragmentReceived = false;
    int totalLength = 0;

    while (!lastFragmentReceived) {
      int recordMakerRaw = buffer.getInt(0);

      boolean isLastFragment = (recordMakerRaw & 0x80000000) != 0;
      int fragmentLength = recordMakerRaw & 0x7FFFFFFF;

      // 跳过开头的RMS
      int dataStartOffset = 4;
      if (fragmentLength > 0 && buffer.length() > dataStartOffset) {
        Buffer bufferSlice = buffer.slice(dataStartOffset, fragmentLength - dataStartOffset);
        fragmentDataList.add(bufferSlice.getBytes());
      }

      totalLength += fragmentLength;
      lastFragmentReceived = isLastFragment;
    }

    FullRpcMessage fullRpcMessage = null;
    if (totalLength > 0 && fragmentDataList.size() > 0) {
      ByteBuffer fullMessageBuffer = ByteBuffer.allocate(totalLength);
      for (byte[] fragmentData : fragmentDataList) {
        fullMessageBuffer.put(fragmentData);
      }
      fullRpcMessage = new FullRpcMessage(fullMessageBuffer.array());
    }

    if (fullRpcMessage != null) {
      fullRpcMessage.decodeAndPrint();
    }
  }

  // NFS Program Constants
  private static final int NFS_PROGRAM = 100003;
  private static final int NFS_VERSION = 3;
  
  // NFS_ACL Program Constants
  private static final int NFS_ACL_PROGRAM = 100227;
  private static final int NFS_ACL_VERSION = 3;
  
  // NFS_ACL Procedure Numbers
  private static final int NFSPROC_ACL_NULL = 0;
  private static final int NFSPROC_ACL_GETACL = 1;
  private static final int NFSPROC_ACL_SETACL = 2;
  
  // MOUNT Program Constants
  private static final int MOUNT_PROGRAM = 100005;
  private static final int MOUNT_VERSION = 3;
  
  // NFS Procedure Numbers
  private static final int NFSPROC_NULL = 0;
  private static final int NFSPROC_GETATTR = 1;
  private static final int NFSPROC_SETATTR = 2;
  private static final int NFSPROC_LOOKUP = 3;
  private static final int NFSPROC_ACCESS = 4;
  private static final int NFSPROC_READLINK = 5;
  private static final int NFSPROC_READ = 6;
  private static final int NFSPROC_WRITE = 7;
  private static final int NFSPROC_CREATE = 8;
  private static final int NFSPROC_MKDIR = 9;
  private static final int NFSPROC_SYMLINK = 10;
  private static final int NFSPROC_MKNOD = 11;
  private static final int NFSPROC_REMOVE = 12;
  private static final int NFSPROC_RMDIR = 13;
  private static final int NFSPROC_RENAME = 14;
  private static final int NFSPROC_LINK = 15;
  private static final int NFSPROC_READDIR = 16;
  private static final int NFSPROC_READDIRPLUS = 17;
  private static final int NFSPROC_FSSTAT = 18;
  private static final int NFSPROC_FSINFO = 19;
  private static final int NFSPROC_PATHCONF = 20;
  private static final int NFSPROC_COMMIT = 21;

  private void handleNFSRequest(Buffer buffer, NetSocket socket) {
    try {
      // Parse RPC header
      int recordMakerRaw = buffer.getInt(0);
      int xid = buffer.getInt(4);
      int msgType = buffer.getInt(8); // Should be CALL (0)
      int rpcVersion = buffer.getInt(12); // Should be 2
      int programNumber = buffer.getInt(16);
      int programVersion = buffer.getInt(20);
      int procedureNumber = buffer.getInt(24);
      
      log.info("NFS Request - XID: 0x{}, Program: {}, Version: {}, Procedure: {}", 
          Integer.toHexString(xid), programNumber, programVersion, procedureNumber);

      // Verify this is an NFS request
      if (programNumber != NFS_PROGRAM || programVersion != NFS_VERSION) {
        log.error("Invalid NFS program number or version: program={}, version={}", 
            programNumber, programVersion);
        return;
      }

      // Parse credentials and verifier
      int credentialsFlavor = buffer.getInt(28);
      int credentialsBodyLength = buffer.getInt(32);
      int startOffset = 36 + credentialsBodyLength;
      int verifierFlavor = buffer.getInt(startOffset);
      int verifierLength = buffer.getInt(startOffset + 4);
      
      // Parse NFS procedure specific data
      startOffset += 8; // Skip verifier
      
      byte[] xdrReplyBytes = null;
      switch (procedureNumber) {
        case NFSPROC_NULL:
          xdrReplyBytes = createNfsNullReply(xid);
          break;
        case NFSPROC_GETATTR:
          xdrReplyBytes = createNfsGetAttrReply(xid, buffer, startOffset);
          break;
        case NFSPROC_SETATTR:
          xdrReplyBytes = createNfsSetAttrReply(xid, buffer, startOffset);
          break;
        case NFSPROC_LOOKUP:
          xdrReplyBytes = createNfsLookupReply(xid, buffer, startOffset);
          break;
        case NFSPROC_ACCESS:
          xdrReplyBytes = createNfsAccessReply(xid, buffer, startOffset);
          break;
        case NFSPROC_READLINK:
          xdrReplyBytes = createNfsReadLinkReply(xid, buffer, startOffset);
          break;
        case NFSPROC_READ:
          xdrReplyBytes = createNfsReadReply(xid, buffer, startOffset);
          break;
        case NFSPROC_WRITE:
          xdrReplyBytes = createNfsWriteReply(xid, buffer, startOffset);
          break;
        case NFSPROC_CREATE:
          xdrReplyBytes = createNfsCreateReply(xid, buffer, startOffset);
          break;
        case NFSPROC_MKDIR:
          xdrReplyBytes = createNfsMkdirReply(xid, buffer, startOffset);
          break;
        case NFSPROC_SYMLINK:
          xdrReplyBytes = createNfsSymlinkReply(xid, buffer, startOffset);
          break;
        case NFSPROC_MKNOD:
          xdrReplyBytes = createNfsMknodReply(xid, buffer, startOffset);
          break;
        case NFSPROC_REMOVE:
          xdrReplyBytes = createNfsRemoveReply(xid, buffer, startOffset);
          break;
        case NFSPROC_RMDIR:
          xdrReplyBytes = createNfsRmdirReply(xid, buffer, startOffset);
          break;
        case NFSPROC_RENAME:
          xdrReplyBytes = createNfsRenameReply(xid, buffer, startOffset);
          break;
        case NFSPROC_LINK:
          xdrReplyBytes = createNfsLinkReply(xid, buffer, startOffset);
          break;
        case NFSPROC_READDIR:
          xdrReplyBytes = createNfsReadDirReply(xid, buffer, startOffset);
          break;
        case NFSPROC_READDIRPLUS:
          xdrReplyBytes = createNfsReadDirPlusReply(xid, buffer, startOffset);
          break;
        case NFSPROC_FSSTAT:
          xdrReplyBytes = createNfsFSStatReply(xid, buffer, startOffset);
          break;
        case NFSPROC_FSINFO:
          xdrReplyBytes = createNfsFSInfoReply(xid);
          break;
        case NFSPROC_PATHCONF:
          xdrReplyBytes = createNfsPathConfReply(xid, buffer, startOffset);
          break;
        case NFSPROC_COMMIT:
          xdrReplyBytes = createNfsCommitReply(xid, buffer, startOffset);
          break;
        default:
          log.error("Unsupported NFS procedure: {}", procedureNumber);
          return;
      }

      if (xdrReplyBytes != null) {
        log.info("Sending NFS response - XID: 0x{}, Size: {} bytes", 
            Integer.toHexString(xid), xdrReplyBytes.length);

        Buffer replyBuffer = Buffer.buffer(xdrReplyBytes);

        log.info("Raw response buffer (" + buffer.length() + " bytes):");
        // 简单的十六进制打印
        for (int i = 0; i < replyBuffer.length(); i++) {
          System.out.printf("%02X ", replyBuffer.getByte(i));
          if ((i + 1) % 16 == 0 || i == replyBuffer.length() - 1) {
            System.out.println();
          }
        }
        log.info("---- End of Raw response Buffer ----");

        socket.write(replyBuffer);
      }
    } catch (Exception e) {
      log.error("Error processing NFS request", e);
    }
  }

  private byte[] createNfsGetAttrReply(int xid, Buffer request, int startOffset) {
    // Parse file handle from request
    int fhandleLength = request.getInt(startOffset);
    byte[] fhandle = request.slice(startOffset + 4, startOffset + 4 + fhandleLength).getBytes();
    
    // Create reply
    final int rpcMessageBodyLength = 24;
    ByteBuffer rpcBodyBuffer = ByteBuffer.allocate(rpcMessageBodyLength);
    rpcBodyBuffer.order(ByteOrder.BIG_ENDIAN);

    // Standard RPC reply header
    rpcBodyBuffer.putInt(xid);
    rpcBodyBuffer.putInt(MSG_TYPE_REPLY);
    rpcBodyBuffer.putInt(REPLY_STAT_MSG_ACCEPTED);
    rpcBodyBuffer.putInt(VERF_FLAVOR_AUTH_NONE);
    rpcBodyBuffer.putInt(VERF_LENGTH_ZERO);
    rpcBodyBuffer.putInt(ACCEPT_STAT_SUCCESS);

    // NFS GETATTR reply
    // Structure:
    // status (4 bytes)
    // post_op_attr present flag (4 bytes)
    // type (4 bytes)
    // mode (4 bytes)
    // nlink (4 bytes)
    // uid (4 bytes)
    // gid (4 bytes)
    // size (8 bytes)
    // used (8 bytes)
    // rdev (8 bytes)
    // fsid (major) (4 bytes)
    // fsid (minor) (4 bytes)
    // fileid (8 bytes)
    // atime (seconds) (4 bytes)
    // atime (nseconds) (4 bytes)
    // mtime (seconds) (4 bytes)
    // mtime (nseconds) (4 bytes)
    // ctime (seconds) (4 bytes)
    // ctime (nseconds) (4 bytes)
    int rpcNfsLength = 4 + // status
        4 + // post_op_attr present flag
        4 + // type
        4 + // mode
        4 + // nlink
        4 + // uid
        4 + // gid
        8 + // size (8 bytes)
        8 + // used (8 bytes)
        8 + // rdev (8 bytes)
        4 + // fsid (major)
        4 + // fsid (minor)
        8 + // fileid (8 bytes)
        4 + // atime (seconds)
        4 + // atime (nseconds)
        4 + // mtime (seconds)
        4 + // mtime (nseconds)
        4 + // ctime (seconds)
        4;  // ctime (nseconds)

    ByteBuffer rpcNfsBuffer = ByteBuffer.allocate(rpcNfsLength);
    rpcNfsBuffer.order(ByteOrder.BIG_ENDIAN);

    // Status (NFS_OK = 0)
    rpcNfsBuffer.putInt(0);
    
    // post_op_attr
    //rpcNfsBuffer.putInt(0); // present = false
    
    // File attributes
    rpcNfsBuffer.putInt(2);  // type (NF3DIR = 4, directory)
    rpcNfsBuffer.putInt(0x000001ED); // mode (rwxr-xr-x)
    rpcNfsBuffer.putInt(1);  // nlink (2 hard links for directory)
    rpcNfsBuffer.putInt(0);  // uid (root)
    rpcNfsBuffer.putInt(0);  // gid (root)
    rpcNfsBuffer.putLong(4096);  // size (4KB)
    rpcNfsBuffer.putLong(4096);  // used (4KB)
    rpcNfsBuffer.putLong(0);  // rdev (0 for directories)
    rpcNfsBuffer.putInt(0x08c60040);  // fsid (major)
    rpcNfsBuffer.putInt(0x2b5cd8a8);  // fsid (minor)
    rpcNfsBuffer.putLong(0x0000000002000002L);  // fileid (unique file identifier)
    
    // Current time in seconds and nanoseconds
    long currentTimeMillis = System.currentTimeMillis();
    int seconds = (int)(currentTimeMillis / 1000);
    int nseconds = (int)((currentTimeMillis % 1000) * 1_000_000);
    
    // atime
    rpcNfsBuffer.putInt(seconds);  // atime (seconds)
    rpcNfsBuffer.putInt(nseconds);  // atime (nseconds)
    
    // mtime
    rpcNfsBuffer.putInt(seconds);  // mtime (seconds)
    rpcNfsBuffer.putInt(nseconds);  // mtime (nseconds)
    
    // ctime
    rpcNfsBuffer.putInt(seconds);  // ctime (seconds)
    rpcNfsBuffer.putInt(nseconds);

    // Record marking
    int recordMarkValue = 0x80000000 | (rpcMessageBodyLength + rpcNfsLength);

    ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcMessageBodyLength + rpcNfsLength);
    fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);
    fullResponseBuffer.putInt(recordMarkValue);
    fullResponseBuffer.put(rpcBodyBuffer.array());
    fullResponseBuffer.put(rpcNfsBuffer.array());

    return fullResponseBuffer.array();
  }

  private byte[] createNfsLookupReply(int xid, Buffer request, int startOffset) {
    // Parse directory file handle and name from request
    int dirFhandleLength = request.getInt(startOffset);
    byte[] dirFhandle = request.slice(startOffset + 4, startOffset + 4 + dirFhandleLength).getBytes();
    int nameLength = request.getInt(startOffset + 4 + dirFhandleLength);
    String name = request.slice(startOffset + 4 + dirFhandleLength + 4, 
        startOffset + 4 + dirFhandleLength + 4 + nameLength).toString("UTF-8");
    
    log.info("LOOKUP request - directory handle length: {}, name: {}", dirFhandleLength, name);
    
    // Create reply
    final int rpcMessageBodyLength = 24;
    ByteBuffer rpcBodyBuffer = ByteBuffer.allocate(rpcMessageBodyLength);
    rpcBodyBuffer.order(ByteOrder.BIG_ENDIAN);

    // Standard RPC reply header
    rpcBodyBuffer.putInt(xid);
    rpcBodyBuffer.putInt(MSG_TYPE_REPLY);
    rpcBodyBuffer.putInt(REPLY_STAT_MSG_ACCEPTED);
    rpcBodyBuffer.putInt(VERF_FLAVOR_AUTH_NONE);
    rpcBodyBuffer.putInt(VERF_LENGTH_ZERO);
    rpcBodyBuffer.putInt(ACCEPT_STAT_SUCCESS);

    // Calculate size for attributes
    int attrSize = 4 + // present flag
        4 + // type
        4 + // mode
        4 + // nlink
        4 + // uid
        4 + // gid
        8 + // size
        8 + // used
        8 + // rdev
        4 + // fsid (major)
        4 + // fsid (minor)
        8 + // fileid
        4 + // atime (seconds)
        4 + // atime (nseconds)
        4 + // mtime (seconds)
        4 + // mtime (nseconds)
        4 + // ctime (seconds)
        4;  // ctime (nseconds)

    // Generate a unique file handle based on the name
    byte[] fileHandle = generateFileHandle(name);
    int fileHandleLength = fileHandle.length;
    
    log.info("Generated file handle for '{}': {}", name, bytesToHex(fileHandle));

    int rpcNfsLength = 4 + // status
        4 + // object handle length
        fileHandleLength + // object handle data
        4 + // obj_attributes present flag
        attrSize + // obj_attributes
        4 + // dir_attributes present flag
        attrSize;  // dir_attributes

    ByteBuffer rpcNfsBuffer = ByteBuffer.allocate(rpcNfsLength);
    rpcNfsBuffer.order(ByteOrder.BIG_ENDIAN);

    // Status (NFS_OK = 0)
    rpcNfsBuffer.putInt(0);
    
    // Object handle
    rpcNfsBuffer.putInt(fileHandleLength); // handle length
    rpcNfsBuffer.put(fileHandle); // handle data
    
    // Object attributes present flag (1 = true)
    rpcNfsBuffer.putInt(1);
    
    // Object attributes
    // Determine file type based on name
    int fileType;
    if (name.endsWith("/") || name.equals("docs") || name.equals("src") || 
        name.equals("bin") || name.equals("lib") || name.equals("include") || 
        name.equals("share") || name.equals("etc") || name.equals("var") || 
        name.equals("tmp") || name.equals("usr") || name.equals("home") || 
        name.equals("root") || name.equals("boot") || name.equals("dev") || 
        name.equals("proc") || name.equals("sys") || name.equals("mnt")) {
        fileType = 2;  // NF3DIR = 2, directory
    } else {
        fileType = 1;  // NF3REG = 1, regular file
    }
    
    rpcNfsBuffer.putInt(fileType);  // type
    rpcNfsBuffer.putInt(fileType == 2 ? 0x000001ED : 0x000001A4); // mode (rwxr-xr-x for dir, rw-r--r-- for file)
    rpcNfsBuffer.putInt(1);  // nlink
    rpcNfsBuffer.putInt(0);  // uid (root)
    rpcNfsBuffer.putInt(0);  // gid (root)
    rpcNfsBuffer.putLong(0);  // size
    rpcNfsBuffer.putLong(0);  // used
    rpcNfsBuffer.putLong(0);  // rdev
    rpcNfsBuffer.putInt(0x08c60040);  // fsid (major)
    rpcNfsBuffer.putInt(0x2b5cd8a8);  // fsid (minor)
    rpcNfsBuffer.putLong(generateFileId(name));  // fileid (unique for each file)
    
    // Current time in seconds and nanoseconds
    long currentTimeMillis = System.currentTimeMillis();
    int seconds = (int)(currentTimeMillis / 1000);
    int nseconds = (int)((currentTimeMillis % 1000) * 1_000_000);
    
    // atime
    rpcNfsBuffer.putInt(seconds);  // atime (seconds)
    rpcNfsBuffer.putInt(nseconds);  // atime (nseconds)
    
    // mtime
    rpcNfsBuffer.putInt(seconds);  // mtime (seconds)
    rpcNfsBuffer.putInt(nseconds);  // mtime (nseconds)
    
    // ctime
    rpcNfsBuffer.putInt(seconds);  // ctime (seconds)
    rpcNfsBuffer.putInt(nseconds);  // ctime (nseconds)
    
    // Directory attributes present flag (1 = true)
    rpcNfsBuffer.putInt(1);
    
    // Directory attributes
    rpcNfsBuffer.putInt(2);  // type (NF3DIR = 2, directory)
    rpcNfsBuffer.putInt(0x000001ED); // mode (rwxr-xr-x)
    rpcNfsBuffer.putInt(1);  // nlink
    rpcNfsBuffer.putInt(0);  // uid (root)
    rpcNfsBuffer.putInt(0);  // gid (root)
    rpcNfsBuffer.putLong(4096);  // size (4KB)
    rpcNfsBuffer.putLong(4096);  // used (4KB)
    rpcNfsBuffer.putLong(0);  // rdev (0 for directories)
    rpcNfsBuffer.putInt(0x08c60040);  // fsid (major)
    rpcNfsBuffer.putInt(0x2b5cd8a8);  // fsid (minor)
    rpcNfsBuffer.putLong(0x0000000002000002L);  // fileid
    
    // atime
    rpcNfsBuffer.putInt(seconds);  // atime (seconds)
    rpcNfsBuffer.putInt(nseconds);  // atime (nseconds)
    
    // mtime
    rpcNfsBuffer.putInt(seconds);  // mtime (seconds)
    rpcNfsBuffer.putInt(nseconds);  // mtime (nseconds)
    
    // ctime
    rpcNfsBuffer.putInt(seconds);  // ctime (seconds)
    rpcNfsBuffer.putInt(nseconds);  // ctime (nseconds)

    // Record marking
    int recordMarkValue = 0x80000000 | (rpcMessageBodyLength + rpcNfsLength);

    ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcMessageBodyLength + rpcNfsLength);
    fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);
    fullResponseBuffer.putInt(recordMarkValue);
    fullResponseBuffer.put(rpcBodyBuffer.array());
    fullResponseBuffer.put(rpcNfsBuffer.array());

    return fullResponseBuffer.array();
  }

  // Helper method to generate a unique file handle based on the file name
  private byte[] generateFileHandle(String name) {
    // Create a 32-byte file handle
    byte[] handle = new byte[32];
    // Fill with zeros initially
    for (int i = 0; i < handle.length; i++) {
        handle[i] = 0;
    }
    
    // Use the first 8 bytes for a hash of the name
    int hash = name.hashCode();
    handle[0] = (byte)(hash >> 24);
    handle[1] = (byte)(hash >> 16);
    handle[2] = (byte)(hash >> 8);
    handle[3] = (byte)hash;
    
    // Use the next 4 bytes for the file type (1 for regular file, 2 for directory)
    int fileType = name.endsWith("/") || name.equals("docs") || name.equals("src") || 
                   name.equals("bin") || name.equals("lib") || name.equals("include") || 
                   name.equals("share") || name.equals("etc") || name.equals("var") || 
                   name.equals("tmp") || name.equals("usr") || name.equals("home") || 
                   name.equals("root") || name.equals("boot") || name.equals("dev") || 
                   name.equals("proc") || name.equals("sys") || name.equals("mnt") ? 2 : 1;
    handle[4] = (byte)fileType;
    
    // Use the next 4 bytes for a timestamp
    long timestamp = System.currentTimeMillis();
    handle[8] = (byte)(timestamp >> 56);
    handle[9] = (byte)(timestamp >> 48);
    handle[10] = (byte)(timestamp >> 40);
    handle[11] = (byte)(timestamp >> 32);
    
    return handle;
  }

  // Helper method to generate a unique file ID based on the file name
  private long generateFileId(String name) {
    // Use a combination of name hash and timestamp to create a unique ID
    int hash = name.hashCode();
    long timestamp = System.currentTimeMillis();
    return ((long)hash << 32) | (timestamp & 0xFFFFFFFFL);
  }

  private byte[] createNfsReadReply(int xid, Buffer request, int startOffset) {
    // Parse file handle, offset, and count from request
    int fhandleLength = request.getInt(startOffset);
    byte[] fhandle = request.slice(startOffset + 4, startOffset + 4 + fhandleLength).getBytes();
    long offset = request.getLong(startOffset + 4 + fhandleLength);
    int count = request.getInt(startOffset + 4 + fhandleLength + 8);
    
    // Create reply
    final int rpcMessageBodyLength = 24;
    ByteBuffer rpcBodyBuffer = ByteBuffer.allocate(rpcMessageBodyLength);
    rpcBodyBuffer.order(ByteOrder.BIG_ENDIAN);

    // Standard RPC reply header
    rpcBodyBuffer.putInt(xid);
    rpcBodyBuffer.putInt(MSG_TYPE_REPLY);
    rpcBodyBuffer.putInt(REPLY_STAT_MSG_ACCEPTED);
    rpcBodyBuffer.putInt(VERF_FLAVOR_AUTH_NONE);
    rpcBodyBuffer.putInt(VERF_LENGTH_ZERO);
    rpcBodyBuffer.putInt(ACCEPT_STAT_SUCCESS);

    // NFS READ reply
    int rpcNfsLength = 4 + 4 + 4 + 4 + 4 + 4 + 4 + 4 + 4 + 4 + 4 + 4 + 4 + 4 + 4 + 4 + 4 + 4 + 4 + 4;
    ByteBuffer rpcNfsBuffer = ByteBuffer.allocate(rpcNfsLength);
    rpcNfsBuffer.order(ByteOrder.BIG_ENDIAN);

    // Status (NFS_OK = 0)
    rpcNfsBuffer.putInt(0);
    
    // Attributes
    // type (NF3REG = 1)
    rpcNfsBuffer.putInt(1);
    // mode (0644)
    rpcNfsBuffer.putInt(0644);
    // nlink
    rpcNfsBuffer.putInt(1);
    // uid
    rpcNfsBuffer.putInt(0);
    // gid
    rpcNfsBuffer.putInt(0);
    // size
    rpcNfsBuffer.putInt(0);
    // used
    rpcNfsBuffer.putInt(0);
    // rdev
    rpcNfsBuffer.putInt(0);
    // fsid
    rpcNfsBuffer.putInt(0);
    rpcNfsBuffer.putInt(0);
    // fileid
    rpcNfsBuffer.putInt(1);
    // atime
    rpcNfsBuffer.putInt((int)(System.currentTimeMillis() / 1000));
    rpcNfsBuffer.putInt(0);
    // mtime
    rpcNfsBuffer.putInt((int)(System.currentTimeMillis() / 1000));
    rpcNfsBuffer.putInt(0);
    // ctime
    rpcNfsBuffer.putInt((int)(System.currentTimeMillis() / 1000));
    rpcNfsBuffer.putInt(0);
    
    // count
    rpcNfsBuffer.putInt(0);
    // eof
    rpcNfsBuffer.putInt(1);
    // data
    rpcNfsBuffer.putInt(0);

    // Record marking
    int recordMarkValue = 0x80000000 | (rpcMessageBodyLength + rpcNfsLength);

    ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcMessageBodyLength + rpcNfsLength);
    fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);
    fullResponseBuffer.putInt(recordMarkValue);
    fullResponseBuffer.put(rpcBodyBuffer.array());
    fullResponseBuffer.put(rpcNfsBuffer.array());

    return fullResponseBuffer.array();
  }

  private byte[] createNfsWriteReply(int xid, Buffer request, int startOffset) {
    // Parse file handle, offset, and data from request
    int fhandleLength = request.getInt(startOffset);
    byte[] fhandle = request.slice(startOffset + 4, startOffset + 4 + fhandleLength).getBytes();
    long offset = request.getLong(startOffset + 4 + fhandleLength);
    int count = request.getInt(startOffset + 4 + fhandleLength + 8);
    byte[] data = request.slice(startOffset + 4 + fhandleLength + 8 + 4, 
        startOffset + 4 + fhandleLength + 8 + 4 + count).getBytes();
    
    // Create reply
    final int rpcMessageBodyLength = 24;
    ByteBuffer rpcBodyBuffer = ByteBuffer.allocate(rpcMessageBodyLength);
    rpcBodyBuffer.order(ByteOrder.BIG_ENDIAN);

    // Standard RPC reply header
    rpcBodyBuffer.putInt(xid);
    rpcBodyBuffer.putInt(MSG_TYPE_REPLY);
    rpcBodyBuffer.putInt(REPLY_STAT_MSG_ACCEPTED);
    rpcBodyBuffer.putInt(VERF_FLAVOR_AUTH_NONE);
    rpcBodyBuffer.putInt(VERF_LENGTH_ZERO);
    rpcBodyBuffer.putInt(ACCEPT_STAT_SUCCESS);

    // NFS WRITE reply
    int rpcNfsLength = 4 + 4 + 4 + 4 + 4 + 4 + 4 + 4 + 4 + 4 + 4 + 4 + 4 + 4 + 4 + 4 + 4 + 4 + 4 + 4;
    ByteBuffer rpcNfsBuffer = ByteBuffer.allocate(rpcNfsLength);
    rpcNfsBuffer.order(ByteOrder.BIG_ENDIAN);

    // Status (NFS_OK = 0)
    rpcNfsBuffer.putInt(0);
    
    // Attributes
    // type (NF3REG = 1)
    rpcNfsBuffer.putInt(1);
    // mode (0644)
    rpcNfsBuffer.putInt(0644);
    // nlink
    rpcNfsBuffer.putInt(1);
    // uid
    rpcNfsBuffer.putInt(0);
    // gid
    rpcNfsBuffer.putInt(0);
    // size
    rpcNfsBuffer.putInt(count);
    // used
    rpcNfsBuffer.putInt(count);
    // rdev
    rpcNfsBuffer.putInt(0);
    // fsid
    rpcNfsBuffer.putInt(0);
    rpcNfsBuffer.putInt(0);
    // fileid
    rpcNfsBuffer.putInt(1);
    // atime
    rpcNfsBuffer.putInt((int)(System.currentTimeMillis() / 1000));
    rpcNfsBuffer.putInt(0);
    // mtime
    rpcNfsBuffer.putInt((int)(System.currentTimeMillis() / 1000));
    rpcNfsBuffer.putInt(0);
    // ctime
    rpcNfsBuffer.putInt((int)(System.currentTimeMillis() / 1000));
    rpcNfsBuffer.putInt(0);
    
    // count
    rpcNfsBuffer.putInt(count);
    // committed
    rpcNfsBuffer.putInt(0); // FILE_SYNC
    // verf
    rpcNfsBuffer.putInt(0);
    rpcNfsBuffer.putInt(0);

    // Record marking
    int recordMarkValue = 0x80000000 | (rpcMessageBodyLength + rpcNfsLength);

    ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcMessageBodyLength + rpcNfsLength);
    fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);
    fullResponseBuffer.putInt(recordMarkValue);
    fullResponseBuffer.put(rpcBodyBuffer.array());
    fullResponseBuffer.put(rpcNfsBuffer.array());

    return fullResponseBuffer.array();
  }

  private byte[] createNfsFSInfoReply(int xid) {
    final int rpcMessageBodyLength = 24;
    ByteBuffer rpcBodyBuffer = ByteBuffer.allocate(rpcMessageBodyLength);
    rpcBodyBuffer.order(ByteOrder.BIG_ENDIAN);

    // Standard RPC reply header
    rpcBodyBuffer.putInt(xid);
    rpcBodyBuffer.putInt(MSG_TYPE_REPLY);
    rpcBodyBuffer.putInt(REPLY_STAT_MSG_ACCEPTED);
    rpcBodyBuffer.putInt(VERF_FLAVOR_AUTH_NONE);
    rpcBodyBuffer.putInt(VERF_LENGTH_ZERO);
    rpcBodyBuffer.putInt(ACCEPT_STAT_SUCCESS);

    // NFS FSINFO reply
    // Structure:
    // status (4 bytes)
    // post_op_attr present flag (4 bytes)
    // rtmax (4 bytes)
    // rtpref (4 bytes)
    // rtmult (4 bytes)
    // wtmax (4 bytes)
    // wtpref (4 bytes)
    // wtmult (4 bytes)
    // dtpref (4 bytes)
    // maxfilesize (8 bytes)
    // time_delta (8 bytes)
    // extra field (4 bytes)
    int rpcNfsLength = 4 + // status
        4 + // post_op_attr present flag
        4 + // rtmax
        4 + // rtpref
        4 + // rtmult
        4 + // wtmax
        4 + // wtpref
        4 + // wtmult
        4 + // dtpref
        8 + // maxfilesize
        8 + // time_delta
        4;  // extra field

    ByteBuffer rpcNfsBuffer = ByteBuffer.allocate(rpcNfsLength);
    rpcNfsBuffer.order(ByteOrder.BIG_ENDIAN);

    // Status (NFS_OK = 0)
    rpcNfsBuffer.putInt(0);
    
    // post_op_attr
    rpcNfsBuffer.putInt(0); // present = false
    
    // FSINFO specific fields
    rpcNfsBuffer.putInt(1048576);  // rtmax (1MB)
    rpcNfsBuffer.putInt(1048576);  // rtpref (1MB)
    rpcNfsBuffer.putInt(4096);     // rtmult (4KB)
    rpcNfsBuffer.putInt(1048576);  // wtmax (1MB)
    rpcNfsBuffer.putInt(1048576);  // wtpref (1MB)
    rpcNfsBuffer.putInt(512);      // wtmult (512 bytes)
    rpcNfsBuffer.putInt(1048576);  // dtpref (1MB)
    rpcNfsBuffer.putLong(0x00000FFFFFFFF000L);  // maxfilesize
    rpcNfsBuffer.putInt(1);        // time_delta (seconds)
    rpcNfsBuffer.putInt(0);        // time_delta (nseconds)
    rpcNfsBuffer.putInt(0x0000001b);        // extra field

    // Record marking
    int recordMarkValue = 0x80000000 | (rpcMessageBodyLength + rpcNfsLength);

    ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcMessageBodyLength + rpcNfsLength);
    fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);
    fullResponseBuffer.putInt(recordMarkValue);
    fullResponseBuffer.put(rpcBodyBuffer.array());
    fullResponseBuffer.put(rpcNfsBuffer.array());

    return fullResponseBuffer.array();
  }

  private byte[] createNfsPathConfReply(int xid, Buffer request, int startOffset) {
    // Parse file handle from request
    int fhandleLength = request.getInt(startOffset);
    byte[] fhandle = request.slice(startOffset + 4, startOffset + 4 + fhandleLength).getBytes();
    
    // Create reply
    final int rpcMessageBodyLength = 24;
    ByteBuffer rpcBodyBuffer = ByteBuffer.allocate(rpcMessageBodyLength);
    rpcBodyBuffer.order(ByteOrder.BIG_ENDIAN);

    // Standard RPC reply header
    rpcBodyBuffer.putInt(xid);
    rpcBodyBuffer.putInt(MSG_TYPE_REPLY);
    rpcBodyBuffer.putInt(REPLY_STAT_MSG_ACCEPTED);
    rpcBodyBuffer.putInt(VERF_FLAVOR_AUTH_NONE);
    rpcBodyBuffer.putInt(VERF_LENGTH_ZERO);
    rpcBodyBuffer.putInt(ACCEPT_STAT_SUCCESS);

    // NFS PATHCONF reply
    // Structure:
    // status (4 bytes)
    // post_op_attr present flag (4 bytes)
    // linkmax (4 bytes)
    // name_max (4 bytes)
    // no_trunc (4 bytes)
    // chown_restricted (4 bytes)
    // case_insensitive (4 bytes)
    // case_preserving (4 bytes)
    int rpcNfsLength = 4 + // status
        4 + // post_op_attr present flag
        4 + // linkmax
        4 + // name_max
        4 + // no_trunc
        4 + // chown_restricted
        4 + // case_insensitive
        4;  // case_preserving

    ByteBuffer rpcNfsBuffer = ByteBuffer.allocate(rpcNfsLength);
    rpcNfsBuffer.order(ByteOrder.BIG_ENDIAN);

    // Status (NFS_OK = 0)
    rpcNfsBuffer.putInt(0);
    
    // post_op_attr
    rpcNfsBuffer.putInt(0); // present = false
    
    // PATHCONF specific fields
    rpcNfsBuffer.putInt(32000);     // linkmax (maximum number of hard links)
    rpcNfsBuffer.putInt(255);       // name_max (maximum file name length)
    rpcNfsBuffer.putInt(1);         // no_trunc (1 = true, names are truncated)
    rpcNfsBuffer.putInt(0);         // chown_restricted (0 = false, chown is allowed)
    rpcNfsBuffer.putInt(0);         // case_insensitive (0 = false, case sensitive)
    rpcNfsBuffer.putInt(1);         // case_preserving (1 = true, case is preserved)

    // Record marking
    int recordMarkValue = 0x80000000 | (rpcMessageBodyLength + rpcNfsLength);

    ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcMessageBodyLength + rpcNfsLength);
    fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);
    fullResponseBuffer.putInt(recordMarkValue);
    fullResponseBuffer.put(rpcBodyBuffer.array());
    fullResponseBuffer.put(rpcNfsBuffer.array());

    return fullResponseBuffer.array();
  }

  private void handleNFSACLRequest(Buffer buffer, NetSocket socket) {
    try {
      // Parse RPC header
      int recordMakerRaw = buffer.getInt(0);
      int xid = buffer.getInt(4);
      int msgType = buffer.getInt(8); // Should be CALL (0)
      int rpcVersion = buffer.getInt(12); // Should be 2
      int programNumber = buffer.getInt(16);
      int programVersion = buffer.getInt(20);
      int procedureNumber = buffer.getInt(24);
      
      log.info("NFS_ACL Request - XID: 0x{}, Procedure: {}", 
          Integer.toHexString(xid), procedureNumber);

      // Parse credentials and verifier
      int credentialsFlavor = buffer.getInt(28);
      int credentialsBodyLength = buffer.getInt(32);
      int startOffset = 36 + credentialsBodyLength;
      int verifierFlavor = buffer.getInt(startOffset);
      int verifierLength = buffer.getInt(startOffset + 4);
      
      // Parse NFS_ACL procedure specific data
      startOffset += 8; // Skip verifier
      
      byte[] xdrReplyBytes = null;
      switch (procedureNumber) {
        case NFSPROC_ACL_NULL:
          xdrReplyBytes = createNfsNullReply(xid);
          break;
        case NFSPROC_ACL_GETACL:
          xdrReplyBytes = createNfsACLGetACLReply(xid, buffer, startOffset);
          break;
        case NFSPROC_ACL_SETACL:
          xdrReplyBytes = createNfsACLSetACLReply(xid, buffer, startOffset);
          break;
        default:
          log.error("Unsupported NFS_ACL procedure: {}", procedureNumber);
          return;
      }

      if (xdrReplyBytes != null) {
        log.info("Sending NFS_ACL response - XID: 0x{}, Size: {} bytes", 
            Integer.toHexString(xid), xdrReplyBytes.length);
        socket.write(Buffer.buffer(xdrReplyBytes));
      }
    } catch (Exception e) {
      log.error("Error processing NFS_ACL request", e);
    }
  }

  private byte[] createNfsACLGetACLReply(int xid, Buffer request, int startOffset) {
    // Parse file handle from request
    int fhandleLength = request.getInt(startOffset);
    byte[] fhandle = request.slice(startOffset + 4, startOffset + 4 + fhandleLength).getBytes();
    
    // Create reply
    final int rpcMessageBodyLength = 24;
    ByteBuffer rpcBodyBuffer = ByteBuffer.allocate(rpcMessageBodyLength);
    rpcBodyBuffer.order(ByteOrder.BIG_ENDIAN);

    // Standard RPC reply header
    rpcBodyBuffer.putInt(xid);
    rpcBodyBuffer.putInt(MSG_TYPE_REPLY);
    rpcBodyBuffer.putInt(REPLY_STAT_MSG_ACCEPTED);
    rpcBodyBuffer.putInt(VERF_FLAVOR_AUTH_NONE);
    rpcBodyBuffer.putInt(VERF_LENGTH_ZERO);
    rpcBodyBuffer.putInt(ACCEPT_STAT_SUCCESS);

    // NFS_ACL GETACL reply
    // Structure:
    // status (4 bytes)
    // post_op_attr present flag (4 bytes)
    // acl_count (4 bytes)
    // acl_entries (variable length)
    int rpcNfsLength = 4 + // status
        4 + // post_op_attr present flag
        4;  // acl_count (0 for now)

    ByteBuffer rpcNfsBuffer = ByteBuffer.allocate(rpcNfsLength);
    rpcNfsBuffer.order(ByteOrder.BIG_ENDIAN);

    // Status (NFS_OK = 0)
    rpcNfsBuffer.putInt(0);
    
    // post_op_attr
    rpcNfsBuffer.putInt(0); // present = false
    
    // ACL count (0 for now)
    rpcNfsBuffer.putInt(0);

    // Record marking
    int recordMarkValue = 0x80000000 | (rpcMessageBodyLength + rpcNfsLength);

    ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcMessageBodyLength + rpcNfsLength);
    fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);
    fullResponseBuffer.putInt(recordMarkValue);
    fullResponseBuffer.put(rpcBodyBuffer.array());
    fullResponseBuffer.put(rpcNfsBuffer.array());

    return fullResponseBuffer.array();
  }

  private byte[] createNfsACLSetACLReply(int xid, Buffer request, int startOffset) {
    // Create reply
    final int rpcMessageBodyLength = 24;
    ByteBuffer rpcBodyBuffer = ByteBuffer.allocate(rpcMessageBodyLength);
    rpcBodyBuffer.order(ByteOrder.BIG_ENDIAN);

    // Standard RPC reply header
    rpcBodyBuffer.putInt(xid);
    rpcBodyBuffer.putInt(MSG_TYPE_REPLY);
    rpcBodyBuffer.putInt(REPLY_STAT_MSG_ACCEPTED);
    rpcBodyBuffer.putInt(VERF_FLAVOR_AUTH_NONE);
    rpcBodyBuffer.putInt(VERF_LENGTH_ZERO);
    rpcBodyBuffer.putInt(ACCEPT_STAT_SUCCESS);

    // NFS_ACL SETACL reply
    // Structure:
    // status (4 bytes)
    // post_op_attr present flag (4 bytes)
    int rpcNfsLength = 4 + // status
        4;  // post_op_attr present flag

    ByteBuffer rpcNfsBuffer = ByteBuffer.allocate(rpcNfsLength);
    rpcNfsBuffer.order(ByteOrder.BIG_ENDIAN);

    // Status (NFS_OK = 0)
    rpcNfsBuffer.putInt(0);
    
    // post_op_attr
    rpcNfsBuffer.putInt(0); // present = false

    // Record marking
    int recordMarkValue = 0x80000000 | (rpcMessageBodyLength + rpcNfsLength);

    ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcMessageBodyLength + rpcNfsLength);
    fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);
    fullResponseBuffer.putInt(recordMarkValue);
    fullResponseBuffer.put(rpcBodyBuffer.array());
    fullResponseBuffer.put(rpcNfsBuffer.array());

    return fullResponseBuffer.array();
  }

  private byte[] createNfsSetAttrReply(int xid, Buffer request, int startOffset) {
    // Create reply
    final int rpcMessageBodyLength = 24;
    ByteBuffer rpcBodyBuffer = ByteBuffer.allocate(rpcMessageBodyLength);
    rpcBodyBuffer.order(ByteOrder.BIG_ENDIAN);

    // Standard RPC reply header
    rpcBodyBuffer.putInt(xid);
    rpcBodyBuffer.putInt(MSG_TYPE_REPLY);
    rpcBodyBuffer.putInt(REPLY_STAT_MSG_ACCEPTED);
    rpcBodyBuffer.putInt(VERF_FLAVOR_AUTH_NONE);
    rpcBodyBuffer.putInt(VERF_LENGTH_ZERO);
    rpcBodyBuffer.putInt(ACCEPT_STAT_SUCCESS);

    // NFS SETATTR reply
    // Structure:
    // status (4 bytes)
    // pre_op_attr present flag (4 bytes)
    // post_op_attr present flag (4 bytes)
    int rpcNfsLength = 4 + // status
        4 + // pre_op_attr present flag
        4;  // post_op_attr present flag

    ByteBuffer rpcNfsBuffer = ByteBuffer.allocate(rpcNfsLength);
    rpcNfsBuffer.order(ByteOrder.BIG_ENDIAN);

    // Status (NFS_OK = 0)
    rpcNfsBuffer.putInt(0);
    
    // pre_op_attr
    rpcNfsBuffer.putInt(0); // present = false
    
    // post_op_attr
    rpcNfsBuffer.putInt(0); // present = false

    // Record marking
    int recordMarkValue = 0x80000000 | (rpcMessageBodyLength + rpcNfsLength);

    ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcMessageBodyLength + rpcNfsLength);
    fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);
    fullResponseBuffer.putInt(recordMarkValue);
    fullResponseBuffer.put(rpcBodyBuffer.array());
    fullResponseBuffer.put(rpcNfsBuffer.array());

    return fullResponseBuffer.array();
  }

  private byte[] createNfsAccessReply(int xid, Buffer request, int startOffset) {
    // Parse file handle from request
    int fhandleLength = request.getInt(startOffset);
    byte[] fhandle = request.slice(startOffset + 4, startOffset + 4 + fhandleLength).getBytes();
    
    // Parse access request flags
    int accessRequest = request.getInt(startOffset + 4 + fhandleLength);
    log.info("ACCESS request - handle length: {}, access request: 0x{}", 
        fhandleLength, Integer.toHexString(accessRequest));
    
    // Create reply
    final int rpcMessageBodyLength = 24;
    ByteBuffer rpcBodyBuffer = ByteBuffer.allocate(rpcMessageBodyLength);
    rpcBodyBuffer.order(ByteOrder.BIG_ENDIAN);

    // Standard RPC reply header
    rpcBodyBuffer.putInt(xid);
    rpcBodyBuffer.putInt(MSG_TYPE_REPLY);
    rpcBodyBuffer.putInt(REPLY_STAT_MSG_ACCEPTED);
    rpcBodyBuffer.putInt(VERF_FLAVOR_AUTH_NONE);
    rpcBodyBuffer.putInt(VERF_LENGTH_ZERO);
    rpcBodyBuffer.putInt(ACCEPT_STAT_SUCCESS);

    // Calculate size for attributes
    int attrSize = 4 + // present flag
        4 + // type
        4 + // mode
        4 + // nlink
        4 + // uid
        4 + // gid
        8 + // size
        8 + // used
        8 + // rdev
        4 + // fsid (major)
        4 + // fsid (minor)
        8 + // fileid
        4 + // atime (seconds)
        4 + // atime (nseconds)
        4 + // mtime (seconds)
        4 + // mtime (nseconds)
        4 + // ctime (seconds)
        4;  // ctime (nseconds)

    // NFS ACCESS reply
    // Structure:
    // status (4 bytes)
    // post_op_attr present flag (4 bytes)
    // post_op_attr (attrSize bytes)
    // access (4 bytes)
    int rpcNfsLength = 4 + // status
        4 + // post_op_attr present flag
        attrSize + // post_op_attr
        4;  // access

    ByteBuffer rpcNfsBuffer = ByteBuffer.allocate(rpcNfsLength);
    rpcNfsBuffer.order(ByteOrder.BIG_ENDIAN);

    // Status (NFS_OK = 0)
    rpcNfsBuffer.putInt(0);
    
    // post_op_attr present flag (1 = true)
    rpcNfsBuffer.putInt(1);
    
    // Determine file type from handle
    int fileType = fhandle[4]; // We stored file type in the 5th byte of handle
    
    // File attributes
    rpcNfsBuffer.putInt(fileType);  // type (NF3DIR = 2, NF3REG = 1)
    rpcNfsBuffer.putInt(fileType == 2 ? 0x000001ED : 0x000001A4); // mode (rwxr-xr-x for dir, rw-r--r-- for file)
    rpcNfsBuffer.putInt(1);  // nlink
    rpcNfsBuffer.putInt(0);  // uid (root)
    rpcNfsBuffer.putInt(0);  // gid (root)
    rpcNfsBuffer.putLong(0);  // size
    rpcNfsBuffer.putLong(0);  // used
    rpcNfsBuffer.putLong(0);  // rdev
    rpcNfsBuffer.putInt(0x08c60040);  // fsid (major)
    rpcNfsBuffer.putInt(0x2b5cd8a8);  // fsid (minor)
    rpcNfsBuffer.putLong(0x0000000002000002L);  // fileid
    
    // Current time in seconds and nanoseconds
    long currentTimeMillis = System.currentTimeMillis();
    int seconds = (int)(currentTimeMillis / 1000);
    int nseconds = (int)((currentTimeMillis % 1000) * 1_000_000);
    
    // atime
    rpcNfsBuffer.putInt(seconds);  // atime (seconds)
    rpcNfsBuffer.putInt(nseconds);  // atime (nseconds)
    
    // mtime
    rpcNfsBuffer.putInt(seconds);  // mtime (seconds)
    rpcNfsBuffer.putInt(nseconds);  // mtime (nseconds)
    
    // ctime
    rpcNfsBuffer.putInt(seconds);  // ctime (seconds)
    rpcNfsBuffer.putInt(nseconds);  // ctime (nseconds)
    
    // ACCESS flags
    // ACCESS_READ     = 0x0001
    // ACCESS_LOOKUP   = 0x0002
    // ACCESS_MODIFY   = 0x0004
    // ACCESS_EXTEND   = 0x0008
    // ACCESS_DELETE   = 0x0010
    // ACCESS_EXECUTE  = 0x0020
    
    int accessFlags = 0;
    
    if (fileType == 2) { // Directory
        // For directories, allow READ, LOOKUP, MODIFY, EXTEND, DELETE
        accessFlags = 0x0001 | // ACCESS_READ
                     0x0002 | // ACCESS_LOOKUP
                     0x0004 | // ACCESS_MODIFY
                     0x0008 | // ACCESS_EXTEND
                     0x0010;  // ACCESS_DELETE
    } else { // Regular file
        // For regular files, allow READ, MODIFY, EXTEND, DELETE, EXECUTE
        accessFlags = 0x0001 | // ACCESS_READ
                     0x0004 | // ACCESS_MODIFY
                     0x0008 | // ACCESS_EXTEND
                     0x0010 | // ACCESS_DELETE
                     0x0020;  // ACCESS_EXECUTE
    }
    
    // Only return the flags that were requested
    accessFlags &= accessRequest;
    
    log.info("ACCESS response - file type: {}, granted access: 0x{}", 
        fileType, Integer.toHexString(accessFlags));
    
    rpcNfsBuffer.putInt(accessFlags);

    // Record marking
    int recordMarkValue = 0x80000000 | (rpcMessageBodyLength + rpcNfsLength);

    ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcMessageBodyLength + rpcNfsLength);
    fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);
    fullResponseBuffer.putInt(recordMarkValue);
    fullResponseBuffer.put(rpcBodyBuffer.array());
    fullResponseBuffer.put(rpcNfsBuffer.array());

    return fullResponseBuffer.array();
  }

  private byte[] createNfsReadLinkReply(int xid, Buffer request, int startOffset) {
    // Create reply
    final int rpcMessageBodyLength = 24;
    ByteBuffer rpcBodyBuffer = ByteBuffer.allocate(rpcMessageBodyLength);
    rpcBodyBuffer.order(ByteOrder.BIG_ENDIAN);

    // Standard RPC reply header
    rpcBodyBuffer.putInt(xid);
    rpcBodyBuffer.putInt(MSG_TYPE_REPLY);
    rpcBodyBuffer.putInt(REPLY_STAT_MSG_ACCEPTED);
    rpcBodyBuffer.putInt(VERF_FLAVOR_AUTH_NONE);
    rpcBodyBuffer.putInt(VERF_LENGTH_ZERO);
    rpcBodyBuffer.putInt(ACCEPT_STAT_SUCCESS);

    // NFS READLINK reply
    // Structure:
    // status (4 bytes)
    // post_op_attr present flag (4 bytes)
    // data length (4 bytes)
    // data (variable length)
    int rpcNfsLength = 4 + // status
        4 + // post_op_attr present flag
        4 + // data length
        0;  // data (empty for now)

    ByteBuffer rpcNfsBuffer = ByteBuffer.allocate(rpcNfsLength);
    rpcNfsBuffer.order(ByteOrder.BIG_ENDIAN);

    // Status (NFS_OK = 0)
    rpcNfsBuffer.putInt(0);
    
    // post_op_attr
    rpcNfsBuffer.putInt(0); // present = false
    
    // data length
    rpcNfsBuffer.putInt(0); // empty data

    // Record marking
    int recordMarkValue = 0x80000000 | (rpcMessageBodyLength + rpcNfsLength);

    ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcMessageBodyLength + rpcNfsLength);
    fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);
    fullResponseBuffer.putInt(recordMarkValue);
    fullResponseBuffer.put(rpcBodyBuffer.array());
    fullResponseBuffer.put(rpcNfsBuffer.array());

    return fullResponseBuffer.array();
  }

  private byte[] createNfsFSStatReply(int xid, Buffer request, int startOffset) {
    // Create reply
    final int rpcMessageBodyLength = 24;
    ByteBuffer rpcBodyBuffer = ByteBuffer.allocate(rpcMessageBodyLength);
    rpcBodyBuffer.order(ByteOrder.BIG_ENDIAN);

    // Standard RPC reply header
    rpcBodyBuffer.putInt(xid);
    rpcBodyBuffer.putInt(MSG_TYPE_REPLY);
    rpcBodyBuffer.putInt(REPLY_STAT_MSG_ACCEPTED);
    rpcBodyBuffer.putInt(VERF_FLAVOR_AUTH_NONE);
    rpcBodyBuffer.putInt(VERF_LENGTH_ZERO);
    rpcBodyBuffer.putInt(ACCEPT_STAT_SUCCESS);

    // NFS FSSTAT reply
    // Structure:
    // status (4 bytes)
    // post_op_attr present flag (4 bytes)
    // tbytes (8 bytes)
    // fbytes (8 bytes)
    // abytes (8 bytes)
    // tfiles (8 bytes)
    // ffiles (8 bytes)
    // afiles (8 bytes)
    // invarsec (4 bytes)
    int rpcNfsLength = 4 + // status
        4 + // post_op_attr present flag
        8 + // tbytes
        8 + // fbytes
        8 + // abytes
        8 + // tfiles
        8 + // ffiles
        8 + // afiles
        4;  // invarsec

    ByteBuffer rpcNfsBuffer = ByteBuffer.allocate(rpcNfsLength);
    rpcNfsBuffer.order(ByteOrder.BIG_ENDIAN);

    // Status (NFS_OK = 0)
    rpcNfsBuffer.putInt(0);
    
    // post_op_attr
    rpcNfsBuffer.putInt(0); // present = false
    
    // FSSTAT specific fields
    rpcNfsBuffer.putLong(0x0000000000000000L);  // tbytes (total bytes)
    rpcNfsBuffer.putLong(0x0000000000000000L);  // fbytes (free bytes)
    rpcNfsBuffer.putLong(0x0000000000000000L);  // abytes (available bytes)
    rpcNfsBuffer.putLong(0x0000000000000000L);  // tfiles (total files)
    rpcNfsBuffer.putLong(0x0000000000000000L);  // ffiles (free files)
    rpcNfsBuffer.putLong(0x0000000000000000L);  // afiles (available files)
    rpcNfsBuffer.putInt(0);                     // invarsec (invariant seconds)

    // Record marking
    int recordMarkValue = 0x80000000 | (rpcMessageBodyLength + rpcNfsLength);

    ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcMessageBodyLength + rpcNfsLength);
    fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);
    fullResponseBuffer.putInt(recordMarkValue);
    fullResponseBuffer.put(rpcBodyBuffer.array());
    fullResponseBuffer.put(rpcNfsBuffer.array());

    return fullResponseBuffer.array();
  }

  private byte[] createNfsCommitReply(int xid, Buffer request, int startOffset) {
    // Create reply
    final int rpcMessageBodyLength = 24;
    ByteBuffer rpcBodyBuffer = ByteBuffer.allocate(rpcMessageBodyLength);
    rpcBodyBuffer.order(ByteOrder.BIG_ENDIAN);

    // Standard RPC reply header
    rpcBodyBuffer.putInt(xid);
    rpcBodyBuffer.putInt(MSG_TYPE_REPLY);
    rpcBodyBuffer.putInt(REPLY_STAT_MSG_ACCEPTED);
    rpcBodyBuffer.putInt(VERF_FLAVOR_AUTH_NONE);
    rpcBodyBuffer.putInt(VERF_LENGTH_ZERO);
    rpcBodyBuffer.putInt(ACCEPT_STAT_SUCCESS);

    // NFS COMMIT reply
    // Structure:
    // status (4 bytes)
    // verf (8 bytes)
    // wcc_data
    //   pre_op_attr present flag (4 bytes)
    //   post_op_attr present flag (4 bytes)
    int rpcNfsLength = 4 + // status
        8 + // verf
        4 + // pre_op_attr present flag
        4;  // post_op_attr present flag

    ByteBuffer rpcNfsBuffer = ByteBuffer.allocate(rpcNfsLength);
    rpcNfsBuffer.order(ByteOrder.BIG_ENDIAN);

    // Status (NFS_OK = 0)
    rpcNfsBuffer.putInt(0);
    
    // verf
    rpcNfsBuffer.putLong(0); // write verifier
    
    // wcc_data
    rpcNfsBuffer.putInt(0); // pre_op_attr present = false
    rpcNfsBuffer.putInt(0); // post_op_attr present = false

    // Record marking
    int recordMarkValue = 0x80000000 | (rpcMessageBodyLength + rpcNfsLength);

    ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcMessageBodyLength + rpcNfsLength);
    fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);
    fullResponseBuffer.putInt(recordMarkValue);
    fullResponseBuffer.put(rpcBodyBuffer.array());
    fullResponseBuffer.put(rpcNfsBuffer.array());

    return fullResponseBuffer.array();
  }

  private byte[] createNfsCreateReply(int xid, Buffer request, int startOffset) {
    // Create reply
    final int rpcMessageBodyLength = 24;
    ByteBuffer rpcBodyBuffer = ByteBuffer.allocate(rpcMessageBodyLength);
    rpcBodyBuffer.order(ByteOrder.BIG_ENDIAN);

    // Standard RPC reply header
    rpcBodyBuffer.putInt(xid);
    rpcBodyBuffer.putInt(MSG_TYPE_REPLY);
    rpcBodyBuffer.putInt(REPLY_STAT_MSG_ACCEPTED);
    rpcBodyBuffer.putInt(VERF_FLAVOR_AUTH_NONE);
    rpcBodyBuffer.putInt(VERF_LENGTH_ZERO);
    rpcBodyBuffer.putInt(ACCEPT_STAT_SUCCESS);

    // NFS CREATE reply
    // Structure:
    // status (4 bytes)
    // file handle (32 bytes)
    // post_op_attr present flag (4 bytes)
    // wcc_data
    //   pre_op_attr present flag (4 bytes)
    //   post_op_attr present flag (4 bytes)
    int rpcNfsLength = 4 + // status
        32 + // file handle
        4 + // post_op_attr present flag
        4 + // pre_op_attr present flag
        4;  // post_op_attr present flag

    ByteBuffer rpcNfsBuffer = ByteBuffer.allocate(rpcNfsLength);
    rpcNfsBuffer.order(ByteOrder.BIG_ENDIAN);

    // Status (NFS_OK = 0)
    rpcNfsBuffer.putInt(0);
    
    // file handle (using the same format as MNT reply)
    byte[] fileHandle = "00000000000000010000000000000001000000010000000100000004".getBytes();
    rpcNfsBuffer.put(fileHandle);
    
    // post_op_attr
    rpcNfsBuffer.putInt(0); // present = false
    
    // wcc_data
    rpcNfsBuffer.putInt(0); // pre_op_attr present = false
    rpcNfsBuffer.putInt(0); // post_op_attr present = false

    // Record marking
    int recordMarkValue = 0x80000000 | (rpcMessageBodyLength + rpcNfsLength);

    ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcMessageBodyLength + rpcNfsLength);
    fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);
    fullResponseBuffer.putInt(recordMarkValue);
    fullResponseBuffer.put(rpcBodyBuffer.array());
    fullResponseBuffer.put(rpcNfsBuffer.array());

    return fullResponseBuffer.array();
  }

  private byte[] createNfsMkdirReply(int xid, Buffer request, int startOffset) {
    // Create reply
    final int rpcMessageBodyLength = 24;
    ByteBuffer rpcBodyBuffer = ByteBuffer.allocate(rpcMessageBodyLength);
    rpcBodyBuffer.order(ByteOrder.BIG_ENDIAN);

    // Standard RPC reply header
    rpcBodyBuffer.putInt(xid);
    rpcBodyBuffer.putInt(MSG_TYPE_REPLY);
    rpcBodyBuffer.putInt(REPLY_STAT_MSG_ACCEPTED);
    rpcBodyBuffer.putInt(VERF_FLAVOR_AUTH_NONE);
    rpcBodyBuffer.putInt(VERF_LENGTH_ZERO);
    rpcBodyBuffer.putInt(ACCEPT_STAT_SUCCESS);

    // NFS MKDIR reply
    // Structure:
    // status (4 bytes)
    // file handle (32 bytes)
    // post_op_attr present flag (4 bytes)
    // wcc_data
    //   pre_op_attr present flag (4 bytes)
    //   post_op_attr present flag (4 bytes)
    int rpcNfsLength = 4 + // status
        32 + // file handle
        4 + // post_op_attr present flag
        4 + // pre_op_attr present flag
        4;  // post_op_attr present flag

    ByteBuffer rpcNfsBuffer = ByteBuffer.allocate(rpcNfsLength);
    rpcNfsBuffer.order(ByteOrder.BIG_ENDIAN);

    // Status (NFS_OK = 0)
    rpcNfsBuffer.putInt(0);
    
    // file handle (using the same format as MNT reply)
    byte[] fileHandle = "00000000000000010000000000000001000000010000000100000004".getBytes();
    rpcNfsBuffer.put(fileHandle);
    
    // post_op_attr
    rpcNfsBuffer.putInt(0); // present = false
    
    // wcc_data
    rpcNfsBuffer.putInt(0); // pre_op_attr present = false
    rpcNfsBuffer.putInt(0); // post_op_attr present = false

    // Record marking
    int recordMarkValue = 0x80000000 | (rpcMessageBodyLength + rpcNfsLength);

    ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcMessageBodyLength + rpcNfsLength);
    fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);
    fullResponseBuffer.putInt(recordMarkValue);
    fullResponseBuffer.put(rpcBodyBuffer.array());
    fullResponseBuffer.put(rpcNfsBuffer.array());

    return fullResponseBuffer.array();
  }

  private byte[] createNfsSymlinkReply(int xid, Buffer request, int startOffset) {
    // Create reply
    final int rpcMessageBodyLength = 24;
    ByteBuffer rpcBodyBuffer = ByteBuffer.allocate(rpcMessageBodyLength);
    rpcBodyBuffer.order(ByteOrder.BIG_ENDIAN);

    // Standard RPC reply header
    rpcBodyBuffer.putInt(xid);
    rpcBodyBuffer.putInt(MSG_TYPE_REPLY);
    rpcBodyBuffer.putInt(REPLY_STAT_MSG_ACCEPTED);
    rpcBodyBuffer.putInt(VERF_FLAVOR_AUTH_NONE);
    rpcBodyBuffer.putInt(VERF_LENGTH_ZERO);
    rpcBodyBuffer.putInt(ACCEPT_STAT_SUCCESS);

    // NFS SYMLINK reply
    // Structure:
    // status (4 bytes)
    // file handle (32 bytes)
    // post_op_attr present flag (4 bytes)
    // wcc_data
    //   pre_op_attr present flag (4 bytes)
    //   post_op_attr present flag (4 bytes)
    int rpcNfsLength = 4 + // status
        32 + // file handle
        4 + // post_op_attr present flag
        4 + // pre_op_attr present flag
        4;  // post_op_attr present flag

    ByteBuffer rpcNfsBuffer = ByteBuffer.allocate(rpcNfsLength);
    rpcNfsBuffer.order(ByteOrder.BIG_ENDIAN);

    // Status (NFS_OK = 0)
    rpcNfsBuffer.putInt(0);
    
    // file handle (using the same format as MNT reply)
    byte[] fileHandle = "00000000000000010000000000000001000000010000000100000004".getBytes();
    rpcNfsBuffer.put(fileHandle);
    
    
    // post_op_attr
    rpcNfsBuffer.putInt(0); // present = false
    
    // wcc_data
    rpcNfsBuffer.putInt(0); // pre_op_attr present = false
    rpcNfsBuffer.putInt(0); // post_op_attr present = false

    // Record marking
    int recordMarkValue = 0x80000000 | (rpcMessageBodyLength + rpcNfsLength);

    ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcMessageBodyLength + rpcNfsLength);
    fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);
    fullResponseBuffer.putInt(recordMarkValue);
    fullResponseBuffer.put(rpcBodyBuffer.array());
    fullResponseBuffer.put(rpcNfsBuffer.array());

    return fullResponseBuffer.array();
  }

  private byte[] createNfsMknodReply(int xid, Buffer request, int startOffset) {
    // Create reply
    final int rpcMessageBodyLength = 24;
    ByteBuffer rpcBodyBuffer = ByteBuffer.allocate(rpcMessageBodyLength);
    rpcBodyBuffer.order(ByteOrder.BIG_ENDIAN);

    // Standard RPC reply header
    rpcBodyBuffer.putInt(xid);
    rpcBodyBuffer.putInt(MSG_TYPE_REPLY);
    rpcBodyBuffer.putInt(REPLY_STAT_MSG_ACCEPTED);
    rpcBodyBuffer.putInt(VERF_FLAVOR_AUTH_NONE);
    rpcBodyBuffer.putInt(VERF_LENGTH_ZERO);
    rpcBodyBuffer.putInt(ACCEPT_STAT_SUCCESS);

    // NFS MKNOD reply
    // Structure:
    // status (4 bytes)
    // file handle (32 bytes)
    // post_op_attr present flag (4 bytes)
    // wcc_data
    //   pre_op_attr present flag (4 bytes)
    //   post_op_attr present flag (4 bytes)
    int rpcNfsLength = 4 + // status
        32 + // file handle
        4 + // post_op_attr present flag
        4 + // pre_op_attr present flag
        4;  // post_op_attr present flag

    ByteBuffer rpcNfsBuffer = ByteBuffer.allocate(rpcNfsLength);
    rpcNfsBuffer.order(ByteOrder.BIG_ENDIAN);

    // Status (NFS_OK = 0)
    rpcNfsBuffer.putInt(0);
    
    // file handle (using the same format as MNT reply)
    byte[] fileHandle = "00000000000000010000000000000001000000010000000100000004".getBytes();
    rpcNfsBuffer.put(fileHandle);
    
    // post_op_attr
    rpcNfsBuffer.putInt(0); // present = false
    
    // wcc_data
    rpcNfsBuffer.putInt(0); // pre_op_attr present = false
    rpcNfsBuffer.putInt(0); // post_op_attr present = false

    // Record marking
    int recordMarkValue = 0x80000000 | (rpcMessageBodyLength + rpcNfsLength);

    ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcMessageBodyLength + rpcNfsLength);
    fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);
    fullResponseBuffer.putInt(recordMarkValue);
    fullResponseBuffer.put(rpcBodyBuffer.array());
    fullResponseBuffer.put(rpcNfsBuffer.array());

    return fullResponseBuffer.array();
  }

  private byte[] createNfsRemoveReply(int xid, Buffer request, int startOffset) {
    // Create reply
    final int rpcMessageBodyLength = 24;
    ByteBuffer rpcBodyBuffer = ByteBuffer.allocate(rpcMessageBodyLength);
    rpcBodyBuffer.order(ByteOrder.BIG_ENDIAN);

    // Standard RPC reply header
    rpcBodyBuffer.putInt(xid);
    rpcBodyBuffer.putInt(MSG_TYPE_REPLY);
    rpcBodyBuffer.putInt(REPLY_STAT_MSG_ACCEPTED);
    rpcBodyBuffer.putInt(VERF_FLAVOR_AUTH_NONE);
    rpcBodyBuffer.putInt(VERF_LENGTH_ZERO);
    rpcBodyBuffer.putInt(ACCEPT_STAT_SUCCESS);

    // NFS REMOVE reply
    // Structure:
    // status (4 bytes)
    // wcc_data
    //   pre_op_attr present flag (4 bytes)
    //   post_op_attr present flag (4 bytes)
    int rpcNfsLength = 4 + // status
        4 + // pre_op_attr present flag
        4;  // post_op_attr present flag

    ByteBuffer rpcNfsBuffer = ByteBuffer.allocate(rpcNfsLength);
    rpcNfsBuffer.order(ByteOrder.BIG_ENDIAN);

    // Status (NFS_OK = 0)
    rpcNfsBuffer.putInt(0);
    
    // wcc_data
    rpcNfsBuffer.putInt(0); // pre_op_attr present = false
    rpcNfsBuffer.putInt(0); // post_op_attr present = false

    // Record marking
    int recordMarkValue = 0x80000000 | (rpcMessageBodyLength + rpcNfsLength);

    ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcMessageBodyLength + rpcNfsLength);
    fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);
    fullResponseBuffer.putInt(recordMarkValue);
    fullResponseBuffer.put(rpcBodyBuffer.array());
    fullResponseBuffer.put(rpcNfsBuffer.array());

    return fullResponseBuffer.array();
  }

  private byte[] createNfsRmdirReply(int xid, Buffer request, int startOffset) {
    // Create reply
    final int rpcMessageBodyLength = 24;
    ByteBuffer rpcBodyBuffer = ByteBuffer.allocate(rpcMessageBodyLength);
    rpcBodyBuffer.order(ByteOrder.BIG_ENDIAN);

    // Standard RPC reply header
    rpcBodyBuffer.putInt(xid);
    rpcBodyBuffer.putInt(MSG_TYPE_REPLY);
    rpcBodyBuffer.putInt(REPLY_STAT_MSG_ACCEPTED);
    rpcBodyBuffer.putInt(VERF_FLAVOR_AUTH_NONE);
    rpcBodyBuffer.putInt(VERF_LENGTH_ZERO);
    rpcBodyBuffer.putInt(ACCEPT_STAT_SUCCESS);

    // NFS RMDIR reply
    // Structure:
    // status (4 bytes)
    // wcc_data
    //   pre_op_attr present flag (4 bytes)
    //   post_op_attr present flag (4 bytes)
    int rpcNfsLength = 4 + // status
        4 + // pre_op_attr present flag
        4;  // post_op_attr present flag

    ByteBuffer rpcNfsBuffer = ByteBuffer.allocate(rpcNfsLength);
    rpcNfsBuffer.order(ByteOrder.BIG_ENDIAN);

    // Status (NFS_OK = 0)
    rpcNfsBuffer.putInt(0);
    
    // wcc_data
    rpcNfsBuffer.putInt(0); // pre_op_attr present = false
    rpcNfsBuffer.putInt(0); // post_op_attr present = false

    // Record marking
    int recordMarkValue = 0x80000000 | (rpcMessageBodyLength + rpcNfsLength);

    ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcMessageBodyLength + rpcNfsLength);
    fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);
    fullResponseBuffer.putInt(recordMarkValue);
    fullResponseBuffer.put(rpcBodyBuffer.array());
    fullResponseBuffer.put(rpcNfsBuffer.array());

    return fullResponseBuffer.array();
  }

  private byte[] createNfsRenameReply(int xid, Buffer request, int startOffset) {
    // Create reply
    final int rpcMessageBodyLength = 24;
    ByteBuffer rpcBodyBuffer = ByteBuffer.allocate(rpcMessageBodyLength);
    rpcBodyBuffer.order(ByteOrder.BIG_ENDIAN);

    // Standard RPC reply header
    rpcBodyBuffer.putInt(xid);
    rpcBodyBuffer.putInt(MSG_TYPE_REPLY);
    rpcBodyBuffer.putInt(REPLY_STAT_MSG_ACCEPTED);
    rpcBodyBuffer.putInt(VERF_FLAVOR_AUTH_NONE);
    rpcBodyBuffer.putInt(VERF_LENGTH_ZERO);
    rpcBodyBuffer.putInt(ACCEPT_STAT_SUCCESS);

    // NFS RENAME reply
    // Structure:
    // status (4 bytes)
    // wcc_data
    //   pre_op_attr present flag (4 bytes)
    //   post_op_attr present flag (4 bytes)
    //   pre_op_attr present flag (4 bytes)
    //   post_op_attr present flag (4 bytes)
    int rpcNfsLength = 4 + // status
        4 + // pre_op_attr present flag (from)
        4 + // post_op_attr present flag (from)
        4 + // pre_op_attr present flag (to)
        4;  // post_op_attr present flag (to)

    ByteBuffer rpcNfsBuffer = ByteBuffer.allocate(rpcNfsLength);
    rpcNfsBuffer.order(ByteOrder.BIG_ENDIAN);

    // Status (NFS_OK = 0)
    rpcNfsBuffer.putInt(0);
    
    // wcc_data (from)
    rpcNfsBuffer.putInt(0); // pre_op_attr present = false
    rpcNfsBuffer.putInt(0); // post_op_attr present = false
    
    // wcc_data (to)
    rpcNfsBuffer.putInt(0); // pre_op_attr present = false
    rpcNfsBuffer.putInt(0); // post_op_attr present = false

    // Record marking
    int recordMarkValue = 0x80000000 | (rpcMessageBodyLength + rpcNfsLength);

    ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcMessageBodyLength + rpcNfsLength);
    fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);
    fullResponseBuffer.putInt(recordMarkValue);
    fullResponseBuffer.put(rpcBodyBuffer.array());
    fullResponseBuffer.put(rpcNfsBuffer.array());

    return fullResponseBuffer.array();
  }

  private byte[] createNfsLinkReply(int xid, Buffer request, int startOffset) {
    // Create reply
    final int rpcMessageBodyLength = 24;
    ByteBuffer rpcBodyBuffer = ByteBuffer.allocate(rpcMessageBodyLength);
    rpcBodyBuffer.order(ByteOrder.BIG_ENDIAN);

    // Standard RPC reply header
    rpcBodyBuffer.putInt(xid);
    rpcBodyBuffer.putInt(MSG_TYPE_REPLY);
    rpcBodyBuffer.putInt(REPLY_STAT_MSG_ACCEPTED);
    rpcBodyBuffer.putInt(VERF_FLAVOR_AUTH_NONE);
    rpcBodyBuffer.putInt(VERF_LENGTH_ZERO);
    rpcBodyBuffer.putInt(ACCEPT_STAT_SUCCESS);

    // NFS LINK reply
    // Structure:
    // status (4 bytes)
    // post_op_attr present flag (4 bytes)
    // wcc_data
    //   pre_op_attr present flag (4 bytes)
    //   post_op_attr present flag (4 bytes)
    int rpcNfsLength = 4 + // status
        4 + // post_op_attr present flag
        4 + // pre_op_attr present flag
        4;  // post_op_attr present flag

    ByteBuffer rpcNfsBuffer = ByteBuffer.allocate(rpcNfsLength);
    rpcNfsBuffer.order(ByteOrder.BIG_ENDIAN);

    // Status (NFS_OK = 0)
    rpcNfsBuffer.putInt(0);
    
    // post_op_attr
    rpcNfsBuffer.putInt(0); // present = false
    
    // wcc_data
    rpcNfsBuffer.putInt(0); // pre_op_attr present = false
    rpcNfsBuffer.putInt(0); // post_op_attr present = false

    // Record marking
    int recordMarkValue = 0x80000000 | (rpcMessageBodyLength + rpcNfsLength);

    ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcMessageBodyLength + rpcNfsLength);
    fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);
    fullResponseBuffer.putInt(recordMarkValue);
    fullResponseBuffer.put(rpcBodyBuffer.array());
    fullResponseBuffer.put(rpcNfsBuffer.array());

    return fullResponseBuffer.array();
  }

  private byte[] createNfsReadDirReply(int xid, Buffer request, int startOffset) {
    // Create reply
    final int rpcMessageBodyLength = 24;
    ByteBuffer rpcBodyBuffer = ByteBuffer.allocate(rpcMessageBodyLength);
    rpcBodyBuffer.order(ByteOrder.BIG_ENDIAN);

    // Standard RPC reply header
    rpcBodyBuffer.putInt(xid);
    rpcBodyBuffer.putInt(MSG_TYPE_REPLY);
    rpcBodyBuffer.putInt(REPLY_STAT_MSG_ACCEPTED);
    rpcBodyBuffer.putInt(VERF_FLAVOR_AUTH_NONE);
    rpcBodyBuffer.putInt(VERF_LENGTH_ZERO);
    rpcBodyBuffer.putInt(ACCEPT_STAT_SUCCESS);

    // NFS READDIR reply
    // Structure:
    // status (4 bytes)
    // post_op_attr present flag (4 bytes)
    // cookieverf (8 bytes)
    // reply
    //   entries present flag (4 bytes)
    //   entries (variable length)
    //   eof flag (4 bytes)
    int rpcNfsLength = 4 + // status
        4 + // post_op_attr present flag
        8 + // cookieverf
        4 + // entries present flag
        0 + // entries (empty for now)
        4;  // eof flag

    ByteBuffer rpcNfsBuffer = ByteBuffer.allocate(rpcNfsLength);
    rpcNfsBuffer.order(ByteOrder.BIG_ENDIAN);

    // Status (NFS_OK = 0)
    rpcNfsBuffer.putInt(0);
    
    // post_op_attr
    rpcNfsBuffer.putInt(0); // present = false
    
    // cookieverf
    rpcNfsBuffer.putLong(0x1234L); // cookie verifier
    
    // entries
    rpcNfsBuffer.putInt(0); // entries present = false
    
    // eof
    rpcNfsBuffer.putInt(1); // eof = true

    // Record marking
    int recordMarkValue = 0x80000000 | (rpcMessageBodyLength + rpcNfsLength);

    ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcMessageBodyLength + rpcNfsLength);
    fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);
    fullResponseBuffer.putInt(recordMarkValue);
    fullResponseBuffer.put(rpcBodyBuffer.array());
    fullResponseBuffer.put(rpcNfsBuffer.array());

    return fullResponseBuffer.array();
  }

  private byte[] createNfsReadDirPlusReply(int xid, Buffer request, int startOffset) {
    // Parse directory file handle from request
    int dirFhandleLength = request.getInt(startOffset);
    log.info("Directory handle length: {}", dirFhandleLength);
    byte[] dirFhandle = request.slice(startOffset + 4, startOffset + 4 + dirFhandleLength).getBytes();
    
    // Parse cookie from request (we'll use this to determine the page)
    int cookieOffset = startOffset + 4 + dirFhandleLength;
    log.info("Reading cookie at offset: {}, dirFhandleLength: {}", cookieOffset, dirFhandleLength);
    
    // Print raw bytes around cookie position for debugging
    log.info("Raw bytes around cookie position:");
    for (int i = cookieOffset - 4; i < cookieOffset + 12; i++) {
        if (i >= 0 && i < request.length()) {
            log.info("Byte at offset {}: 0x{}", i, String.format("%02X", request.getByte(i)));
        }
    }
    
    long cookie = request.getLong(cookieOffset);
    log.info("Received READDIRPLUS request with cookie: {}", cookie);

    int cookieVeriferOffset = cookieOffset + 8;
    
    // Create reply
    final int rpcMessageBodyLength = 24;
    ByteBuffer rpcBodyBuffer = ByteBuffer.allocate(rpcMessageBodyLength);
    rpcBodyBuffer.order(ByteOrder.BIG_ENDIAN);

    // Standard RPC reply header
    rpcBodyBuffer.putInt(xid);
    rpcBodyBuffer.putInt(MSG_TYPE_REPLY);
    rpcBodyBuffer.putInt(REPLY_STAT_MSG_ACCEPTED);
    rpcBodyBuffer.putInt(VERF_FLAVOR_AUTH_NONE);
    rpcBodyBuffer.putInt(VERF_LENGTH_ZERO);
    rpcBodyBuffer.putInt(ACCEPT_STAT_SUCCESS);

    // Define directory entries (simulating a large directory)
    String[] allEntries = {
        "test.txt",    // Regular file
        "docs",        // Directory
        "readme.md",   // Regular file
        "config.json", // Regular file
        "src",         // Directory
        "bin",         // Directory
        "lib",         // Directory
        "include",     // Directory
        "share",       // Directory
        "etc",         // Directory
        "var",         // Directory
        "tmp",         // Directory
        "usr",         // Directory
        "home",        // Directory
        "root",        // Directory
        "boot",        // Directory
        "dev",         // Directory
        "proc",        // Directory
        "sys",         // Directory
        "mnt"          // Directory
    };
    
    // Calculate size for attributes
    int attrSize = 4 + // present flag
        4 + // type
        4 + // mode
        4 + // nlink
        4 + // uid
        4 + // gid
        8 + // size
        8 + // used
        8 + // rdev
        4 + // fsid (major)
        4 + // fsid (minor)
        8 + // fileid
        4 + // atime (seconds)
        4 + // atime (nseconds)
        4 + // mtime (seconds)
        4 + // mtime (nseconds)
        4 + // ctime (seconds)
        4;  // ctime (nseconds)

    int dircount = request.getInt(cookieVeriferOffset + 8);
    int maxcount = request.getInt(cookieVeriferOffset + 12);
    log.info("READDIRPLUS request parameters - dircount: {} bytes, maxcount: {} bytes", dircount, maxcount);
    
    int startIndex = (int) cookie;
    int currentSize = 0;
    int entriesToReturn = 0;

    // Calculate how many entries we can fit within dircount bytes
    for (int i = startIndex; i < allEntries.length; i++) {
        String entryName = allEntries[i];
        int entrySize = 4 + // fileid
                       4 + // name length
                       entryName.length() + // name
                       8 + // cookie
                       4 + // name present
                       attrSize + // attributes
                       4;  // next entry present
        
        log.info("Entry '{}' size: {} bytes, current total: {} bytes (dircount limit: {} bytes)", 
                entryName, entrySize, currentSize + entrySize, dircount);
        
        if (currentSize + entrySize > dircount) {
            log.info("Stopping at entry '{}' as it would exceed dircount limit of {} bytes", entryName, dircount);
            break;
        }
        
        currentSize += entrySize;
        entriesToReturn++;
    }
    
    log.info("Will return {} entries starting from index {} (total size: {} bytes)", 
            entriesToReturn, startIndex, currentSize);
    
    // Check if we've reached the end of the directory
    if (startIndex >= allEntries.length) {
        // Return empty response with eof=1
        int rpcNfsLength = 4 + // status
            4 + // dir_attributes present flag
            attrSize + // dir_attributes
            8 + // cookieverf
            4 + // entries present flag
            4;  // eof flag

        ByteBuffer rpcNfsBuffer = ByteBuffer.allocate(rpcNfsLength);
        rpcNfsBuffer.order(ByteOrder.BIG_ENDIAN);

        // Status (NFS_OK = 0)
        rpcNfsBuffer.putInt(0);
        
        // dir_attributes present flag (1 = true)
        rpcNfsBuffer.putInt(1);
        
        // Directory attributes
        rpcNfsBuffer.putInt(2);  // type (NF3DIR = 2, directory)
        rpcNfsBuffer.putInt(0x000001ED); // mode (rwxr-xr-x)
        rpcNfsBuffer.putInt(1);  // nlink
        rpcNfsBuffer.putInt(0);  // uid (root)
        rpcNfsBuffer.putInt(0);  // gid (root)
        rpcNfsBuffer.putLong(4096);  // size (4KB)
        rpcNfsBuffer.putLong(4096);  // used (4KB)
        rpcNfsBuffer.putLong(0);  // rdev (0 for directories)
        rpcNfsBuffer.putInt(0x08c60040);  // fsid (major)
        rpcNfsBuffer.putInt(0x2b5cd8a8);  // fsid (minor)
        rpcNfsBuffer.putLong(0x0000000002000002L);  // fileid
        
        // Current time in seconds and nanoseconds
        long currentTimeMillis = System.currentTimeMillis();
        int seconds = (int)(currentTimeMillis / 1000);
        int nseconds = (int)((currentTimeMillis % 1000) * 1_000_000);
        
        // atime
        rpcNfsBuffer.putInt(seconds);  // atime (seconds)
        rpcNfsBuffer.putInt(nseconds);  // atime (nseconds)
        
        // mtime
        rpcNfsBuffer.putInt(seconds);  // mtime (seconds)
        rpcNfsBuffer.putInt(nseconds);  // mtime (nseconds)
        
        // ctime
        rpcNfsBuffer.putInt(seconds);  // ctime (seconds)
        rpcNfsBuffer.putInt(nseconds);  // ctime (nseconds)
        
        // cookieverf
        rpcNfsBuffer.putLong(currentTimeMillis);
        
        // entries present flag (0 = false, no entries)
        rpcNfsBuffer.putInt(0);
        
        // eof flag (1 = true, we're at the end)
        rpcNfsBuffer.putInt(1);

        // Record marking
        int recordMarkValue = 0x80000000 | (rpcMessageBodyLength + rpcNfsLength);

        ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcMessageBodyLength + rpcNfsLength);
        fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);
        fullResponseBuffer.putInt(recordMarkValue);
        fullResponseBuffer.put(rpcBodyBuffer.array());
        fullResponseBuffer.put(rpcNfsBuffer.array());

        return fullResponseBuffer.array();
    }

    boolean isLastPage = (startIndex + entriesToReturn) >= allEntries.length;

    // Calculate total size for entries to return
    int totalEntriesSize = 0;
    for (int i = startIndex; i < startIndex + entriesToReturn; i++) {
        String entry = allEntries[i];
        int entryNameLength = entry.length();
        totalEntriesSize += 8 + // fileid
            4 + // name length
            ((entryNameLength + 3) & ~3) + // name (padded to 4 bytes)
            8 + // cookie
            4 + // name_attributes present flag
            attrSize + // name_attributes
            4 + // name_handle present flag
            4 + // handle length
            dirFhandleLength + // handle data
            4;  // nextentry present flag
    }

    int rpcNfsLength = 4 + // status
        4 + // dir_attributes present flag
        attrSize + // dir_attributes
        8 + // cookieverf
        4 + // entries present flag
        totalEntriesSize + // directory entries
        4;  // eof flag

    ByteBuffer rpcNfsBuffer = ByteBuffer.allocate(rpcNfsLength);
    rpcNfsBuffer.order(ByteOrder.BIG_ENDIAN);

    // Status (NFS_OK = 0)
    rpcNfsBuffer.putInt(0);
    
    // dir_attributes present flag (1 = true)
    rpcNfsBuffer.putInt(1);
    
    // Directory attributes
    rpcNfsBuffer.putInt(2);  // type (NF3DIR = 2, directory)
    rpcNfsBuffer.putInt(0x000001ED); // mode (rwxr-xr-x)
    rpcNfsBuffer.putInt(1);  // nlink
    rpcNfsBuffer.putInt(0);  // uid (root)
    rpcNfsBuffer.putInt(0);  // gid (root)
    rpcNfsBuffer.putLong(4096);  // size (4KB)
    rpcNfsBuffer.putLong(4096);  // used (4KB)
    rpcNfsBuffer.putLong(0);  // rdev (0 for directories)
    rpcNfsBuffer.putInt(0x08c60040);  // fsid (major)
    rpcNfsBuffer.putInt(0x2b5cd8a8);  // fsid (minor)
    rpcNfsBuffer.putLong(0x0000000002000002L);  // fileid
    
    // Current time in seconds and nanoseconds
    long currentTimeMillis = System.currentTimeMillis();
    int seconds = (int)(currentTimeMillis / 1000);
    int nseconds = (int)((currentTimeMillis % 1000) * 1_000_000);
    
    // atime
    rpcNfsBuffer.putInt(seconds);  // atime (seconds)
    rpcNfsBuffer.putInt(nseconds);  // atime (nseconds)
    
    // mtime
    rpcNfsBuffer.putInt(seconds);  // mtime (seconds)
    rpcNfsBuffer.putInt(nseconds);  // mtime (nseconds)
    
    // ctime
    rpcNfsBuffer.putInt(seconds);  // ctime (seconds)
    rpcNfsBuffer.putInt(nseconds);  // ctime (nseconds)
    
    // cookieverf (use current time as verifier)
    rpcNfsBuffer.putLong(currentTimeMillis);
    
    // entries present flag (1 = true if we have entries to return)
    rpcNfsBuffer.putInt(entriesToReturn > 0 ? 1 : 0);
    
    // Add directory entries for this page
    for (int i = 0; i < entriesToReturn; i++) {
        int entryIndex = startIndex + i;
        String entryName = allEntries[entryIndex];
        int entryNameLength = entryName.length();
        
        log.info("Sending entry {}: '{}' at index {}", i + 1, entryName, entryIndex);
        
        // fileid (unique for each entry)
        rpcNfsBuffer.putLong(entryIndex + 1);
        
        // name length and name
        rpcNfsBuffer.putInt(entryNameLength);
        byte[] nameBytes = entryName.getBytes();
        rpcNfsBuffer.put(nameBytes);
        // Pad name to 4 bytes
        int padding = ((entryNameLength + 3) & ~3) - entryNameLength;
        for (int j = 0; j < padding; j++) {
            rpcNfsBuffer.put((byte)0);
        }
        
        // cookie (use the next entry's index as cookie)
        // For the last entry in the page, use 0 if it's the last page
        long nextCookie;
        if (i == entriesToReturn - 1 && isLastPage) {
            nextCookie = 0;
        } else {
            nextCookie = entryIndex + 1;
        }
        rpcNfsBuffer.putLong(nextCookie);
        log.info("Entry '{}' cookie: {} (isLastPage: {}, isLastEntry: {})", 
                entryName, nextCookie, isLastPage, i == entriesToReturn - 1);
        
        // name_attributes present flag (1 = true)
        rpcNfsBuffer.putInt(1);
        
        // File attributes
        if (entryName.equals("docs") || entryName.equals("src") || 
            entryName.equals("bin") || entryName.equals("lib") || 
            entryName.equals("include") || entryName.equals("share") || 
            entryName.equals("etc") || entryName.equals("var") || 
            entryName.equals("tmp") || entryName.equals("usr") || 
            entryName.equals("home") || entryName.equals("root") || 
            entryName.equals("boot") || entryName.equals("dev") || 
            entryName.equals("proc") || entryName.equals("sys") || 
            entryName.equals("mnt")) {
            rpcNfsBuffer.putInt(2);  // type (NF3DIR = 2, directory)
            rpcNfsBuffer.putInt(0x000001ED); // mode (rwxr-xr-x)
        } else {
            rpcNfsBuffer.putInt(1);  // type (NF3REG = 1, regular file)
            rpcNfsBuffer.putInt(0x000001A4); // mode (rw-r--r--)
        }
        rpcNfsBuffer.putInt(1);  // nlink
        rpcNfsBuffer.putInt(0);  // uid (root)
        rpcNfsBuffer.putInt(0);  // gid (root)
        rpcNfsBuffer.putLong(0);  // size
        rpcNfsBuffer.putLong(0);  // used
        rpcNfsBuffer.putLong(0);  // rdev
        rpcNfsBuffer.putInt(0x08c60040);  // fsid (major)
        rpcNfsBuffer.putInt(0x2b5cd8a8);  // fsid (minor)
        rpcNfsBuffer.putLong(entryIndex + 1);  // fileid
        
        // atime
        rpcNfsBuffer.putInt(seconds);  // atime (seconds)
        rpcNfsBuffer.putInt(nseconds);  // atime (nseconds)
        
        // mtime
        rpcNfsBuffer.putInt(seconds);  // mtime (seconds)
        rpcNfsBuffer.putInt(nseconds);  // mtime (nseconds)
        
        // ctime
        rpcNfsBuffer.putInt(seconds);  // ctime (seconds)
        rpcNfsBuffer.putInt(nseconds);  // ctime (nseconds)
        
        // name_handle present flag (1 = true)
        rpcNfsBuffer.putInt(1);
        
        // File handle
        rpcNfsBuffer.putInt(dirFhandleLength); // handle length
        rpcNfsBuffer.put(dirFhandle); // handle data
        
        // nextentry present flag (1 if not last entry in this page, 0 if last entry)
        rpcNfsBuffer.putInt(i < entriesToReturn - 1 ? 1 : 0);
    }
    
    // eof flag (1 = true if this is the last page)
    rpcNfsBuffer.putInt(isLastPage ? 1 : 0);

    // Record marking
    int recordMarkValue = 0x80000000 | (rpcMessageBodyLength + rpcNfsLength);

    ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcMessageBodyLength + rpcNfsLength);
    fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);
    fullResponseBuffer.putInt(recordMarkValue);
    fullResponseBuffer.put(rpcBodyBuffer.array());
    fullResponseBuffer.put(rpcNfsBuffer.array());

    return fullResponseBuffer.array();
  }

}
