package com.example.netclient;

import com.example.netclient.enums.*;
import com.example.netclient.httpclient.AwsSignerCreater;
import com.example.netclient.httpclient.UpDownHttpClient;
import com.example.netclient.model.*;
import com.example.netclient.model.acl.*;
import com.example.netclient.utils.ByteArrayKeyWrapper;
import com.example.netclient.utils.EnumUtil;
import com.example.netclient.utils.NetTool;
import com.example.netclient.utils.RpcUtil;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import io.reactivex.Single;
import io.reactivex.schedulers.Schedulers;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.file.FileSystem;
import io.vertx.reactivex.core.http.HttpClient;
import io.vertx.reactivex.core.net.NetServer;
import io.vertx.reactivex.core.net.NetSocket;
import io.vertx.reactivex.core.parsetools.RecordParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.DecoderException;
import org.reactivestreams.Subscriber;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.example.netclient.RequestState.CACHE_THRESHOLD;


@Slf4j
public class TcpServerVerticle extends AbstractVerticle {

  private static final int PORT = 12345; // 服务器监听的端口
  private static final String HOST = "0.0.0.0"; // 监听所有网络接口

  private RpcParseState currentState = RpcParseState.READING_MARKER;
  private int expectedFragmentLength;
  private boolean isLastFragment;
  private List<Buffer> messageFragments = new ArrayList<>();

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

  private static final Map<String, ByteArrayKeyWrapper> fileNameTofileHandle = new ConcurrentHashMap<>();
  private static final Map<String, Long> fileNameTofileId = new ConcurrentHashMap<>();
  private static final Map<ByteArrayKeyWrapper, Long> fileHandleToFileId = new ConcurrentHashMap<>();
  private static final Map<ByteArrayKeyWrapper, String> fileHandleToFileName = new ConcurrentHashMap<>();
  private static final Map<Long, String> fileIdToFileName = new ConcurrentHashMap<>();
  private static final Map<Long, FAttr3> fileIdToFAttr3 = new ConcurrentHashMap<>();
  private static final Map<ByteArrayKeyWrapper, FAttr3> fileHandleToFAttr3 = new ConcurrentHashMap<>();
  private static final Map<ByteArrayKeyWrapper, ByteArrayKeyWrapper> fileHandleToParentFileHandle = new ConcurrentHashMap<>();
  private static final Map<ByteArrayKeyWrapper, List<ByteArrayKeyWrapper>> fileHandleToChildrenFileHandle = new ConcurrentHashMap<>();

  private static final Map<ByteArrayKeyWrapper, AtomicLong> fileHandleNextAppendingPosition = new ConcurrentHashMap<>();

  private static final String STATIC_FILES_ROOT = "public";
  private static final String S3HOST = "172.20.123.124";
  private static final String BUCKET = "mybucket";

  private UpDownHttpClient upDownHttpClient;
  private FileSystem fs;

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    init();

    // 创建 NetServerOptions (可选，用于配置服务器)
    NetServerOptions options = new NetServerOptions()
      .setPort(PORT)
      .setHost(HOST)
      .setTcpKeepAlive(true); // 示例：启用 TCP KeepAlive

    // 创建 TCP 服务器
    NetServer server = vertx.createNetServer(options);

    // 设置连接处理器
    server.connectHandler(socket -> {
      Flowable<Buffer> response = convertToFlowableRequest(socket)
        .doOnNext(rpcFragment -> {
          Buffer buffer = rpcFragment.getData();
          String receivedData = buffer.toString("UTF-8");
          log.info("从客户端 [" + socket.remoteAddress() + "] 收到数据大小: " + receivedData.length());

          log.info("Raw request buffer (" + buffer.length() + " bytes):");
          // 简单的十六进制打印
//          for (int i = 0; i < buffer.length(); i++) {
//            System.out.printf("%02X ", buffer.getByte(i));
//            if ((i + 1) % 16 == 0 || i == buffer.length() - 1) {
//              System.out.println();
//            }
//          }
//          log.info("---- End of Raw Buffer ----");
        })
        .flatMap(rpcFragment -> {
          Buffer buffer = rpcFragment.getData();
          //int recordMakerRaw = fullMessage.getInt(0);
          int xid = buffer.getInt(0);
          int msgType = buffer.getInt(4); // Should be CALL (0)
          int rpcVersion = buffer.getInt(8); // Should be 2
          int programNumber = buffer.getInt(12);
          int programVersion = buffer.getInt(16);
          int procedureNumber = buffer.getInt(20);

          return processCompleteMessage(buffer);
        });

      // 订阅启动整个链
      //Subscriber<Buffer> socketSubscriber = socket.toSubscriber();
      response.subscribe(socket.toSubscriber());
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

  private Flowable<RpcFragment> convertToFlowableRequest(NetSocket socket) {
    return Flowable.create(flowableEmitter -> {
      AtomicReference<RpcParseState> currentStateRef = new AtomicReference<>(RpcParseState.READING_MARKER);
      final boolean[] isLastFragmentHolder = {false};
      final int[] expectedFragmentLengthHolder = {0};

      log.info("客户端连接成功: " + socket.remoteAddress());
      // RecordParser 会替我们处理 TCP 分片问题
      final RecordParser parser = RecordParser.newFixed(4); // Start by reading 4-byte marker

      parser.handler(buffer -> {
        if (currentStateRef.get() == RpcParseState.READING_MARKER) {
          // 我们得到了4字节的记录标记
          long recordMarkerRaw = buffer.getUnsignedInt(0); // 读取为无符号整数
          isLastFragmentHolder[0] = (recordMarkerRaw & 0x80000000L) != 0;
          expectedFragmentLengthHolder[0] = (int) (recordMarkerRaw & 0x7FFFFFFF); // 低31位是长度

          System.out.println("Parsed Marker: last=" + isLastFragmentHolder[0] + ", length=" + expectedFragmentLengthHolder[0]);

          if (expectedFragmentLengthHolder[0] == 0) { // 可能是心跳或空片段
            // 重置为读取下一个标记 (RecordParser 自动回到 fixed(4))
            parser.fixedSizeMode(4);
            currentStateRef.set(RpcParseState.READING_MARKER);
          } else {
            parser.fixedSizeMode(expectedFragmentLengthHolder[0]); // 切换到读取片段数据模式
            currentStateRef.set(RpcParseState.READING_FRAGMENT_DATA);
          }

        } else if (currentStateRef.get() == RpcParseState.READING_FRAGMENT_DATA) {
          // 我们得到了片段数据
          System.out.println("Received fragment data of length: " + buffer.length());
          flowableEmitter.onNext(new RpcFragment(buffer, isLastFragmentHolder[0]));

          // 无论是不是最后一个片段，下一个都应该是记录标记
          parser.fixedSizeMode(4); // 重置为读取下一个标记
          currentStateRef.set(RpcParseState.READING_MARKER);
        }
      });

      parser.exceptionHandler(Throwable::printStackTrace); // 处理解析器可能抛出的异常

      // 为每个连接的 socket 设置数据处理器
      socket.handler(parser);

      // 设置关闭处理器
      socket.closeHandler(v -> {
        log.info("客户端断开连接: " + socket.remoteAddress());
        if (!flowableEmitter.isCancelled()) {
          flowableEmitter.onComplete();
        }
      });

      // 设置异常处理器
      socket.exceptionHandler(throwable -> {
        System.err.println("客户端 [" + socket.remoteAddress() + "] 发生错误: " + throwable.getMessage());
        socket.close(); // 发生错误时关闭连接
        if (!flowableEmitter.isCancelled()) {
          flowableEmitter.onError(throwable);
        }
      });

    }, BackpressureStrategy.BUFFER);

  }

  private void init() throws DecoderException, URISyntaxException {
    // Current time in seconds and nanoseconds
    long currentTimeMillis = System.currentTimeMillis();
    int seconds = (int)(currentTimeMillis / 1000);
    int nseconds = (int)((currentTimeMillis % 1000) * 1_000_000);
    FAttr3 objAttributes = FAttr3.builder()
      .type(2)
      .mode(0755)
      .nlink(1)
      .uid(0)
      .gid(0)
      .size(4096)
      .used(4096)
      .rdev(0)
      .fsidMajor(0x08c60040)
      .fsidMinor(0x2b5cd8a8)
      .fileid(33554434)
      .atimeSeconds(seconds)
      .atimeNseconds(nseconds)
      .mtimeSeconds(seconds)
      .mtimeNseconds(nseconds)
      .ctimeSeconds(seconds)
      .ctimeNseconds(nseconds)
      .build();

    String dataLiteral = "0100070002000002000000003e3e7dae34c9471896e6218574c98110";
    byte[] filehandle = NetTool.hexStringToByteArray(dataLiteral);
    ByteArrayKeyWrapper byteArrayKeyWrapper = new ByteArrayKeyWrapper(filehandle);
    fileHandleToFAttr3.put(byteArrayKeyWrapper, objAttributes);

    Path staticRootPath = Paths.get(STATIC_FILES_ROOT);
    if (!Files.exists(staticRootPath)) {
      try {
        Files.createDirectories(staticRootPath);
      } catch (Exception e) {
        return;
      }
    }

    Vertx vertx = Vertx.vertx();
    HttpClient client = vertx.createHttpClient();
    String host = "172.20.123.124";
    String bucket = "mybucket";
    String key = "hello";
    String targetUrl = String.format("http://%s/%s/%s", host, bucket, key);
    String resourcePath = String.format("/%s/%s", bucket, key);
    String accessKey = "MAKIC8SGQQS8424CZT07";
    String secretKey = "tDFRk9bGzS0j5JXPrdS0qSXL40zSn3xbBRZsPfEH";
    String regionNmae = "us-east-1";
    String serviceName = "s3";

    fs = vertx.fileSystem();

    upDownHttpClient = UpDownHttpClient.builder()
      .client(client)
      .accessKey(accessKey)
      .secretKey(secretKey)
      .region(regionNmae)
      .service(serviceName)
      .signerType(AwsSignerCreater.SignerType.AWS_V4)
      .build();

    upDownHttpClient.put(targetUrl, Flowable.just(Buffer.buffer("hello,world!"))).subscribeOn(Schedulers.single()).observeOn(Schedulers.single()).subscribe();
  }

  private Flowable<Buffer> processCompleteMessage(Buffer buffer) {

      // Parse RPC header
      //int recordMakerRaw = fullMessage.getInt(0);
      int xid = buffer.getInt(0);
      int msgType = buffer.getInt(4); // Should be CALL (0)
      int rpcVersion = buffer.getInt(8); // Should be 2
      int programNumber = buffer.getInt(12);
      int programVersion = buffer.getInt(16);
      int procedureNumber = buffer.getInt(20);

      // Handle NFS requests
      if (programNumber == NFS_PROGRAM && programVersion == NFS_VERSION) {
        return handleNFSRequest(buffer);
      }
      // Handle NFS_ACL requests
      else if (programNumber == NFS_ACL_PROGRAM && programVersion == NFS_ACL_VERSION) {
        return handleNFSACLRequest(buffer);
      }
      else {
        log.error("Unsupported program: program={}, version={}", programNumber, programVersion);
      }

      return Flowable.just(Buffer.buffer());
    // isLastFragment 和 expectedFragmentLength 会在下一次读取标记时重置
  }

  @Override
  public void stop(Promise<Void> stopPromise) throws Exception {
    log.info("TCP 服务器正在关闭...");
    // 可以在这里添加关闭服务器的逻辑，但通常 Vert.x 会自动处理
    stopPromise.complete();
  }

  // RPC Constants (values are in decimal for Java int literals)
  private static final int MSG_TYPE_REPLY = 1;          // 0x00000001
  private static final int REPLY_STAT_MSG_ACCEPTED = 0; // 0x00000000
  private static final int VERF_FLAVOR_AUTH_NONE = 0;   // 0x00000000
  private static final int VERF_LENGTH_ZERO = 0;        // 0x00000000
  private static final int ACCEPT_STAT_SUCCESS = 0;     // 0x00000000

  public static Flowable<Buffer> createNfsNullReply(int requestXid) {
    // --- Calculate RPC Message Body Length ---
    // XID (4 bytes)
    // Message Type (4 bytes)
    // Reply Status (4 bytes)
    // Verifier Flavor (4 bytes)
    // Verifier Length (4 bytes)
    // Acceptance Status (4 bytes)
    // Total = 6 * 4 = 24 bytes
    final int rpcHeaderLength = RpcConstants.RPC_ACCEPTED_REPLY_HEADER_LENGTH;
    // --- Create ByteBuffer for the RPC Message Body ---
    // We will fill this first, then prepend the record mark.
    Flowable<Buffer> rpcHeaderBuffer = RpcUtil.writeAcceptedSuccessReplyHeader(requestXid);
    //        3.2.3. Results (for NFSPROC3_NULL, this is void, so no data)

    // --- Construct Record Marking ---
    // Highest bit set (0x80000000) ORed with the length of the RPC message body.
    // In Java, an int is 32-bit.
    int recordMarkValue = 0x80000000 | rpcHeaderLength;

    // --- Create ByteBuffer for the Full XDR Response ---
    // Record Mark (4 bytes) + RPC Message Body (rpcMessageBodyLength bytes)
    Buffer recordMarkBuffer = Buffer.buffer(4).appendInt(recordMarkValue);
    Flowable<Buffer> fullResponseBuffer = Flowable.concat(Flowable.just(recordMarkBuffer), rpcHeaderBuffer);

    // Return the complete byte array
    return fullResponseBuffer;
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

  private Flowable<Buffer> handleNFSRequest(Buffer buffer) {
    try {
      // Parse RPC header
      //int recordMakerRaw = buffer.getInt(0);
      int xid = buffer.getInt(0);
      int msgType = buffer.getInt(4); // Should be CALL (0)
      int rpcVersion = buffer.getInt(8); // Should be 2
      int programNumber = buffer.getInt(12);
      int programVersion = buffer.getInt(16);
      int procedureNumber = buffer.getInt(20);

      log.info("NFS Request - XID: 0x{}, Program: {}, Version: {}, Procedure: {}",
          Integer.toHexString(xid), programNumber, programVersion, procedureNumber);

      // Verify this is an NFS request
      if (programNumber != NFS_PROGRAM || programVersion != NFS_VERSION) {
        log.error("Invalid NFS program number or version: program={}, version={}",
            programNumber, programVersion);
        return Flowable.just(Buffer.buffer()) ;
      }

      // Parse credentials and verifier
      int credentialsFlavor = buffer.getInt(24);
      int credentialsBodyLength = buffer.getInt(28);
      int startOffset = 32 + credentialsBodyLength;
      int verifierFlavor = buffer.getInt(startOffset);
      int verifierLength = buffer.getInt(startOffset + 4);

      // Parse NFS procedure specific data
      startOffset += 8; // Skip verifier

      Nfs3Procedure procedureNumberEnum = EnumUtil.fromCode(Nfs3Procedure.class, procedureNumber);

      byte[] xdrReplyBytes = null;
      Flowable<Buffer> flowableXdrReplyBytes = null;
      switch (procedureNumberEnum) {
        case NFSPROC_NULL:
          flowableXdrReplyBytes = createNfsNullReply(xid);
          break;
        case NFSPROC_GETATTR:
          flowableXdrReplyBytes = createNfsGetAttrReply(xid, buffer, startOffset);
          break;
        case NFSPROC_SETATTR:
          flowableXdrReplyBytes = createNfsSetAttrReply(xid, buffer, startOffset);
          break;
        case NFSPROC_LOOKUP:
          flowableXdrReplyBytes = createNfsLookupReply(xid, buffer, startOffset);
          break;
        case NFSPROC_ACCESS:
          flowableXdrReplyBytes = createNfsAccessReply(xid, buffer, startOffset);
          break;
//        case NFSPROC_READLINK:
//          xdrReplyBytes = createNfsReadLinkReply(xid, buffer, startOffset);
//          break;
        case NFSPROC_READ:
          flowableXdrReplyBytes = createNfsReadReply(xid, buffer, startOffset);
          break;
        case NFSPROC_WRITE:
          flowableXdrReplyBytes = createNfsWriteReply(xid, buffer, startOffset);
          break;
        case NFSPROC_CREATE:
          flowableXdrReplyBytes = createNfsCreateReply(xid, buffer, startOffset);
          break;
        case NFSPROC_MKDIR:
          flowableXdrReplyBytes = createNfsMkdirReply(xid, buffer, startOffset);
          break;
//        case NFSPROC_SYMLINK:
//          xdrReplyBytes = createNfsSymlinkReply(xid, buffer, startOffset);
//          break;
//        case NFSPROC_MKNOD:
//          xdrReplyBytes = createNfsMknodReply(xid, buffer, startOffset);
//          break;
        case NFSPROC_REMOVE:
          flowableXdrReplyBytes = createNfsRemoveReply(xid, buffer, startOffset);
          break;
//        case NFSPROC_RMDIR:
//          xdrReplyBytes = createNfsRmdirReply(xid, buffer, startOffset);
//          break;
//        case NFSPROC_RENAME:
//          xdrReplyBytes = createNfsRenameReply(xid, buffer, startOffset);
//          break;
//        case NFSPROC_LINK:
//          xdrReplyBytes = createNfsLinkReply(xid, buffer, startOffset);
//          break;
//        case NFSPROC_READDIR:
//          xdrReplyBytes = createNfsReadDirReply(xid, buffer, startOffset);
//          break;
        case NFSPROC_READDIRPLUS:
          flowableXdrReplyBytes = createNfsReadDirPlusReply(xid, buffer, startOffset);
          break;
//        case NFSPROC_FSSTAT:
//          xdrReplyBytes = createNfsFSStatReply(xid, buffer, startOffset);
//          break;
        case NFSPROC_FSINFO:
          flowableXdrReplyBytes = createNfsFSInfoReply(xid);
          break;
        case NFSPROC_PATHCONF:
          flowableXdrReplyBytes = createNfsPathConfReply(xid, buffer, startOffset);
          break;
        case NFSPROC_COMMIT:
          flowableXdrReplyBytes = createNfsCommitReply(xid, buffer, startOffset);
          break;
        default:
          log.error("Unsupported NFS procedure: {}", procedureNumber);
          return Flowable.just(Buffer.buffer());
      }

      if (flowableXdrReplyBytes != null) {

        return flowableXdrReplyBytes;
      }
    } catch (Exception e) {
      log.error("Error processing NFS request", e);
    }

    return Flowable.just(Buffer.buffer());
  }

  private Flowable<Buffer> createNfsGetAttrReply(int xid, Buffer request, int startOffset) throws IOException {
    // Parse file handle from request
    int fhandleLength = request.getInt(startOffset);
    byte[] fhandle = request.slice(startOffset + 4, startOffset + 4 + fhandleLength).getBytes();

    // Create reply
    final int rpcHeaderLength = RpcConstants.RPC_ACCEPTED_REPLY_HEADER_LENGTH;
    Flowable<Buffer> rpcHeaderBuffer = RpcUtil.writeAcceptedSuccessReplyHeader(xid);

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

    long fileId = fileHandleToFileId.getOrDefault(new ByteArrayKeyWrapper(fhandle), 0x0000000002000002L);
    String filename = fileIdToFileName.getOrDefault(fileId, "/");
    int fileType = getFileType(filename);

    FAttr3 objAttributes = fileHandleToFAttr3.getOrDefault(new ByteArrayKeyWrapper(fhandle), null);
    GETATTR3res getattr3res = null;
    if (objAttributes != null) {
      GETATTR3resok getattr3resok = GETATTR3resok.builder()
        .objAttributes(objAttributes)
        .build();

      getattr3res = GETATTR3res.createOk(getattr3resok);
    } else {
      GETATTR3resfail getattr3resfail = GETATTR3resfail.builder().build();
      getattr3res = GETATTR3res.createFail(NfsStat3.NFS3ERR_SERVERFAULT, getattr3resfail);
    }


    int rpcNfsLength = getattr3res.getSerializedSize();
    Flowable<Buffer> rpcNfsBuffer = getattr3res.serializeToFlowable();

    // Record marking
    int recordMarkValue = 0x80000000 | (rpcHeaderLength + rpcNfsLength);

    Buffer recordMarkBuffer = Buffer.buffer(4).appendInt(recordMarkValue);
    Flowable<Buffer> fullResponseBuffer = Flowable.concat(Flowable.just(recordMarkBuffer), rpcHeaderBuffer, rpcNfsBuffer);

    return fullResponseBuffer;
  }

  private Flowable<Buffer> createNfsLookupReply(int xid, Buffer request, int startOffset) throws IOException {
    // Parse directory file handle and name from request
    int dirFhandleLength = request.getInt(startOffset);
    byte[] dirFhandle = request.slice(startOffset + 4, startOffset + 4 + dirFhandleLength).getBytes();
    int nameLength = request.getInt(startOffset + 4 + dirFhandleLength);
    String name = request.slice(startOffset + 4 + dirFhandleLength + 4,
        startOffset + 4 + dirFhandleLength + 4 + nameLength).toString("UTF-8");

    log.info("LOOKUP request - directory handle length: {}, name: {}", dirFhandleLength, name);

    // Create reply
    int rpcHeaderLength = RpcConstants.RPC_ACCEPTED_REPLY_HEADER_LENGTH;
    Flowable<Buffer> rpcHeaderBuffer = RpcUtil.writeAcceptedSuccessReplyHeader(xid);

    // Generate a unique file handle based on the name
    byte[] fileHandle = getFileHandle(name, false);

    int fileHandleLength = fileHandle.length;

    log.info("Generated file handle for '{}': {}", name, bytesToHex(fileHandle));

    LOOKUP3res lookup3res = null;
    if (fileHandleLength > 0) {
      FAttr3 fAttr3 = fileHandleToFAttr3.getOrDefault(new ByteArrayKeyWrapper(fileHandle), null);
      PostOpAttr objAttributes = PostOpAttr.builder()
        .attributesFollow(fAttr3 != null ? 1 : 0)
        .attributes(fAttr3)
        .build();

      PostOpAttr dirAttributes = PostOpAttr.builder()
        .attributesFollow(0)
        .build();

      LOOKUP3resok lookup3resok = LOOKUP3resok.builder()
        .objHandlerLength(fileHandleLength)
        .objectHandleData(fileHandle)
        .objAttributes(objAttributes)
        .dirAttributes(dirAttributes)
        .build();

      lookup3res = LOOKUP3res.createOk(lookup3resok);
    } else {

      FAttr3 dirAttr3 = fileHandleToFAttr3.getOrDefault(new ByteArrayKeyWrapper(dirFhandle), null);
      PostOpAttr dirAttributes = PostOpAttr.builder()
        .attributesFollow(dirAttr3 != null ? 1 : 0)
        .attributes(dirAttr3)
        .build();

      LOOKUP3resfail lookup3resfail = LOOKUP3resfail.builder()
        .dirAttributes(dirAttributes)
        .build();

      lookup3res = LOOKUP3res.createFail(NfsStat3.NFS3ERR_NOENT, lookup3resfail);
    }

    int rpcNfsLength = lookup3res.getSerializedSize();

    Flowable<Buffer> rpcNfsBuffer = lookup3res.serializeToFlowable();

    // Record marking
    int recordMarkValue = 0x80000000 | (rpcHeaderLength + rpcNfsLength);
    Buffer buffer = Buffer.buffer(4).appendInt(recordMarkValue);
    Flowable<Buffer> fullResponseBuffer = Flowable.concat(Flowable.just(buffer), rpcHeaderBuffer, rpcNfsBuffer);

    return fullResponseBuffer;
  }

  private byte[] getFileHandle(String name, boolean createFlag) {
    if(createFlag && !fileNameTofileHandle.containsKey(name)) {
      fileNameTofileHandle.put(name, generateFileHandle(name));
    }

    ByteArrayKeyWrapper fileHandleByteArrayWrapper = fileNameTofileHandle.get(name);
    if (fileHandleByteArrayWrapper == null) {
      return new byte[0];
    }
    return fileHandleByteArrayWrapper.getData();
  }

  private long getFileId(String name) {
    if(!fileNameTofileId.containsKey(name)) {
      long fileId = generateFileId(name);
      fileNameTofileId.put(name, fileId);
    }

    long fileId = fileNameTofileId.get(name);
    return fileId;
  }

  private static int getFileType(String name) {
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
    return fileType;
  }

  // Helper method to generate a unique file handle based on the file name
  private ByteArrayKeyWrapper generateFileHandle(String name) {
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

    return new ByteArrayKeyWrapper(handle);
  }

  // Helper method to generate a unique file ID based on the file name
  private long generateFileId(String name) {
    // Use a combination of name hash and timestamp to create a unique ID
    int hash = name.hashCode();
    long timestamp = System.currentTimeMillis();
    return ((long)hash << 32) | (timestamp & 0xFFFFFFFFL);
  }

  private Flowable<Buffer> createNfsReadReply(int xid, Buffer request, int startOffset) throws IOException, URISyntaxException {
    // Parse file handle, offset, and count from request
    int fhandleLength = request.getInt(startOffset);
    byte[] fhandle = request.slice(startOffset + 4, startOffset + 4 + fhandleLength).getBytes();
    long offset = request.getLong(startOffset + 4 + fhandleLength);
    int count = request.getInt(startOffset + 4 + fhandleLength + 8);

    ByteBuffer buffer = ByteBuffer.allocate(count);

    // Create reply
    final int rpcHeaderLength = RpcConstants.RPC_ACCEPTED_REPLY_HEADER_LENGTH;
    Flowable<Buffer> rpcHeaderBuffer = RpcUtil.writeAcceptedSuccessReplyHeader(xid);

    long fileId = fileHandleToFileId.getOrDefault(new ByteArrayKeyWrapper(fhandle), 0x0000000002000002L);
    String filename = fileIdToFileName.getOrDefault(fileId, "/");
    int fileType =  getFileType(filename);
//    byte[] payload = "hello,world\n".getBytes(StandardCharsets.UTF_8);
//    int dataLength = payload.length;

    byte[] data = new byte[0];
    String targetUrl = String.format("http://%s/%s/%s", S3HOST, BUCKET, filename);

    Single<Buffer> bufferSingle =  upDownHttpClient.get(targetUrl).subscribeOn(Schedulers.io()).observeOn(Schedulers.io());
    int dataLength = bufferSingle.map(Buffer::length).blockingGet();
    data = bufferSingle.blockingGet().getBytes();

    FAttr3 fAttr3 = fileHandleToFAttr3.getOrDefault(new ByteArrayKeyWrapper(fhandle), null);
    PostOpAttr fileAttributes = PostOpAttr.builder()
      .attributesFollow(fAttr3 != null ? 1 : 0)
      .attributes(fAttr3)
      .build();

    READ3resok read3resok = READ3resok.builder()
      .fileAttributes(fileAttributes)
      .count(dataLength)
      .eof(1)
      .dataOfLength(dataLength)
      .data(data)
      .build();

    READ3res read3res = READ3res.createOk(read3resok);

    int rpcNfsLength = read3res.getSerializedSize();
    Flowable<Buffer> rpcNfsBuffer = read3res.serializeToFlowable();

    // Record marking
    int recordMarkValue = 0x80000000 | (rpcHeaderLength + rpcNfsLength);
    Buffer recordMarkBuffer = Buffer.buffer(4).appendInt(recordMarkValue);
    Flowable<Buffer> fullResponseBuffer = Flowable.concat(Flowable.just(recordMarkBuffer), rpcHeaderBuffer, rpcNfsBuffer);

    return fullResponseBuffer;
  }

  private static final ConcurrentHashMap<ByteArrayKeyWrapper, List<Buffer>> requestCache = new ConcurrentHashMap<>();

  private Flowable<Buffer> createNfsWriteReply(int xid, Buffer request, int startOffset) throws IOException, URISyntaxException {
    // Parse file handle, offset, and data from request
    int fhandleLength = request.getInt(startOffset);
    byte[] fhandle = request.slice(startOffset + 4, startOffset + 4 + fhandleLength).getBytes();
    long offset = request.getLong(startOffset + 4 + fhandleLength);
    int count = request.getInt(startOffset + 4 + fhandleLength + 8);
    int stable = request.getInt(startOffset + 4 + fhandleLength + 8 + 4);
    int dataOfLength = request.getInt(startOffset + 4 + fhandleLength + 8);
    Buffer data = request.slice(startOffset + 4 + fhandleLength + 8 + 12,
        startOffset + 4 + fhandleLength + 8 + 12 + dataOfLength);

    ByteArrayKeyWrapper byteArrayKeyWrapper = new ByteArrayKeyWrapper(fhandle);
    // 检查是否需要缓存数据
    //Single<WRITE3res> write3res = Single.never();
    //if (data.length() >= RequestState.CACHE_THRESHOLD && RequestState.shouldCache(byteArrayKeyWrapper)) {
      requestCache.compute(byteArrayKeyWrapper, (key, existingList) -> {
        if (existingList == null) {
          existingList = new ArrayList<>(); // Consider Collections.synchronizedList or CopyOnWriteArrayList if accessed outside compute
        }
        existingList.add(data);
        return existingList;
      });
      FAttr3 attributes = fileHandleToFAttr3.computeIfPresent(byteArrayKeyWrapper, (key, value) -> {
        int used = (dataOfLength + 4096 - 1) / 4096 * 4096;
        value.setSize(dataOfLength);
        value.setUsed(used);
        return value;
      });
      PreOpAttr before = PreOpAttr.builder().attributesFollow(0).build();
      PostOpAttr after = PostOpAttr.builder().attributesFollow(0).build();

      WccData fileWcc = WccData.builder().before(before).after(after).build();
      WRITE3resok write3resok = WRITE3resok.builder().fileWcc(fileWcc).count(count).committed(WRITE3resok.StableHow.UNSTABLE).verifier(0L).build();

      Single<WRITE3res> write3res = Single.just(WRITE3res.createOk(write3resok));
//    } else {
//      write3res = handleNfsWriteAsync(fhandle, data, dataOfLength, count);
//    }

    // Create reply
    final int rpcHeaderLength = RpcConstants.RPC_ACCEPTED_REPLY_HEADER_LENGTH;
    Flowable<Buffer> rpcHeaderBuffer = RpcUtil.writeAcceptedSuccessReplyHeader(xid);

    return write3res.flatMapPublisher(write3res1 -> {
      int rpcNfsLength = write3res1.getSerializedSize();

      Flowable<Buffer> rpcNfsBuffer = write3res1.serializeToFlowable();

      int recordMarkValue = 0x80000000 | (rpcHeaderLength + rpcNfsLength);
      Buffer buffer = Buffer.buffer(4).appendInt(recordMarkValue);
      Flowable<Buffer> fullResponseBuffer = Flowable.concat(Flowable.just(buffer), rpcHeaderBuffer, rpcNfsBuffer);

      return fullResponseBuffer;
    });
  }

  private Single<WRITE3res> handleNfsWriteAsync(byte[] fhandle, Buffer data, int dataOfLength, int count) throws URISyntaxException {
    ByteArrayKeyWrapper byteArrayKeyWrapper = new ByteArrayKeyWrapper(fhandle);
    FAttr3 attributes = fileHandleToFAttr3.getOrDefault(byteArrayKeyWrapper, null);

    if (attributes != null) {
      String filename = fileHandleToFileName.getOrDefault(byteArrayKeyWrapper,"");
      String targetUrl = String.format("http://%s/%s/%s", S3HOST, BUCKET, filename);
      Flowable<Buffer> fileBodyFlowable = Flowable.just(data);
      Single<WRITE3res> response = upDownHttpClient.put(targetUrl, fileBodyFlowable).subscribeOn(Schedulers.io()).observeOn(Schedulers.io())
          .flatMap(buffer1 -> {
            log.info("S3 Upload Response: {}", buffer1.toString());
            fileHandleToFAttr3.computeIfPresent(byteArrayKeyWrapper, (key, value) -> {
              int used = (dataOfLength + 4096 - 1) / 4096 * 4096;
              value.setSize(dataOfLength);
              value.setUsed(used);
              return value;
            });
            PreOpAttr before = PreOpAttr.builder().attributesFollow(0).build();
            PostOpAttr after = PostOpAttr.builder().attributesFollow(1).attributes(attributes).build();

            WccData fileWcc = WccData.builder().before(before).after(after).build();
            WRITE3resok write3resok = WRITE3resok.builder()
              .fileWcc(fileWcc)
              .count(count)
              .committed(WRITE3resok.StableHow.UNSTABLE)
              .verifier(0L)
              .build();

            return Single.just(WRITE3res.createOk(write3resok));
          })
        .onErrorResumeNext(error -> {
          PreOpAttr before = PreOpAttr.builder().attributesFollow(0).build();
          PostOpAttr after = PostOpAttr.builder().attributesFollow(0).build();
          WccData fileWcc = WccData.builder().before(before).after(after).build();
          WRITE3resfail failData = WRITE3resfail.builder().fileWcc(fileWcc).build();

          return Single.just(WRITE3res.createFail(NfsStat3.NFS3ERR_SERVERFAULT, failData));
        });

      return response;
    }

    PreOpAttr before = PreOpAttr.builder().attributesFollow(0).build();
    PostOpAttr after = PostOpAttr.builder().attributesFollow(0).build();
    WccData fileWcc = WccData.builder().before(before).after(after).build();
    WRITE3resfail failData = WRITE3resfail.builder().fileWcc(fileWcc).build();

    return Single.just(WRITE3res.createFail(NfsStat3.NFS3ERR_BADHANDLE, failData));
  }

  private Flowable<Buffer> createNfsFSInfoReply(int xid) throws IOException {
    // Standard ONC RPC reply header
    int rpcHeaderLength = RpcConstants.RPC_ACCEPTED_REPLY_HEADER_LENGTH;
    Flowable<Buffer> rpcHeaderBuffer = RpcUtil.writeAcceptedSuccessReplyHeader(xid);

    // NFS FSINFO reply
    NfsStat3 status = NfsStat3.NFS3_OK;
    FSINFO3resok fsinfo3resok = FSINFO3resok.builder()
      .rtmax(1048576)
      .rtpref(1048576)
      .rtmult(4096)
      .wtmax(1048576)
      .wtpref(1048576)
      .wtmult(512)
      .dtpref(1048576)
      .maxFilesize(0x00000FFFFFFFF000L)
      .seconds(1)
      .nseconds(0)
      .extraField(0x0000001b)
      .build();

    FSINFO3res fsinfo3res = FSINFO3res.createOk(fsinfo3resok);

    int rpcNfsLength = fsinfo3res.getSerializedSize();

    Flowable<Buffer> rpcNfsBuffer = fsinfo3res.serializeToFlowable();

    // Record marking
    int recordMarkValue = 0x80000000 | (rpcHeaderLength + rpcNfsLength);
    Buffer buffer = Buffer.buffer(4).appendInt(recordMarkValue);
    Flowable<Buffer> fullResponseBuffer = Flowable.concat(Flowable.just(buffer), rpcHeaderBuffer, rpcNfsBuffer);

    return fullResponseBuffer;
  }

  private Flowable<Buffer> createNfsPathConfReply(int xid, Buffer request, int startOffset) throws IOException {
    // Parse file handle from request
    int fhandleLength = request.getInt(startOffset);
    byte[] fhandle = request.slice(startOffset + 4, startOffset + 4 + fhandleLength).getBytes();

    // Create reply
    final int rpcHeaderLength = RpcConstants.RPC_ACCEPTED_REPLY_HEADER_LENGTH;
    Flowable<Buffer> rpcHeaderBuffer = RpcUtil.writeAcceptedSuccessReplyHeader(xid);

    // NFS PATHCONF reply
    PostOpAttr objAttributes = PostOpAttr.builder().attributesFollow(0).build();

    PATHCONF3resok pathconf3resok = PATHCONF3resok.builder()
      .objAttributes(objAttributes)
      .linkmax(32000)
      .nameMax(255)
      .noTrunc(1)
      .chownRestricted(0)
      .caseInsensitive(0)
      .casePreserving(1)
      .build();
    PATHCONF3res pathconf3res = PATHCONF3res.createOk(pathconf3resok);

    int rpcNfsLength = pathconf3res.getSerializedSize();
    Flowable<Buffer> rpcNfsBuffer = pathconf3res.serializeToFlowable();

    // Record marking
    int recordMarkValue = 0x80000000 | (rpcHeaderLength + rpcNfsLength);
    Buffer buffer = Buffer.buffer(4).appendInt(recordMarkValue);
    Flowable<Buffer> fullResponseBuffer = Flowable.concat(Flowable.just(buffer), rpcHeaderBuffer, rpcNfsBuffer);

    return fullResponseBuffer;
  }

  private Flowable<Buffer> handleNFSACLRequest(Buffer buffer) {
    try {
      // Parse RPC header
      //int recordMakerRaw = buffer.getInt(0);
      int xid = buffer.getInt(0);
      int msgType = buffer.getInt(4); // Should be CALL (0)
      int rpcVersion = buffer.getInt(8); // Should be 2
      int programNumber = buffer.getInt(12);
      int programVersion = buffer.getInt(16);
      int procedureNumber = buffer.getInt(20);

      log.info("NFS_ACL Request - XID: 0x{}, Procedure: {}",
          Integer.toHexString(xid), procedureNumber);

      // Parse credentials and verifier
      int credentialsFlavor = buffer.getInt(24);
      int credentialsBodyLength = buffer.getInt(28);
      int startOffset = 32 + credentialsBodyLength;
      int verifierFlavor = buffer.getInt(startOffset);
      int verifierLength = buffer.getInt(startOffset + 4);

      // Parse NFS_ACL procedure specific data
      startOffset += 8; // Skip verifier

      byte[] xdrReplyBytes = null;
      Flowable<Buffer> flowableXdrReplyBytes = null;
      switch (procedureNumber) {
        case NFSPROC_ACL_NULL:
          flowableXdrReplyBytes = createNfsNullReply(xid);
          break;
        case NFSPROC_ACL_GETACL:
          flowableXdrReplyBytes = createNfsACLGetACLReply(xid, buffer, startOffset);
          break;
//        case NFSPROC_ACL_SETACL:
//          xdrReplyBytes = createNfsACLSetACLReply(xid, buffer, startOffset);
//          break;
        default:
          log.error("Unsupported NFS_ACL procedure: {}", procedureNumber);
          return Flowable.just(Buffer.buffer());
      }

      if (flowableXdrReplyBytes != null) {
        return flowableXdrReplyBytes;
      }
    } catch (Exception e) {
      log.error("Error processing NFS_ACL request", e);
    }

    return Flowable.just(Buffer.buffer());
  }

  private Flowable<Buffer> createNfsACLGetACLReply(int xid, Buffer request, int startOffset) throws IOException {
    // Parse file handle from request
    int fhandleLength = request.getInt(startOffset);
    byte[] fhandle = request.slice(startOffset + 4, startOffset + 4 + fhandleLength).getBytes();

    // Create reply
    final int rpcHeaderLength = RpcConstants.RPC_ACCEPTED_REPLY_HEADER_LENGTH;
    Flowable<Buffer> rpcHeaderBuffer = RpcUtil.writeAcceptedSuccessReplyHeader(xid);

    PostOpAttr postOpAttr = PostOpAttr.builder().attributesFollow(0).build();
    GETACL3resfail getacl3resfail = GETACL3resfail.builder().objAttributes(postOpAttr).build();

    GETACL3res getacl3res = GETACL3res.createFailure(NfsStat3.NFS3ERR_NOTSUPP, getacl3resfail);

    int rpcNfsLength = getacl3res.getSerializedSize();
    Flowable<Buffer> rpcNfsBuffer = getacl3res.serializeToFlowable();

    // Record marking
    int recordMarkValue = 0x80000000 | (rpcHeaderLength + rpcNfsLength);

    Buffer recordMarkBuffer = Buffer.buffer(4).appendInt(recordMarkValue);
    Flowable<Buffer> fullResponseBuffer = Flowable.concat(Flowable.just(recordMarkBuffer), rpcHeaderBuffer, rpcNfsBuffer);

    return fullResponseBuffer;
  }

  private byte[] createNfsACLSetACLReply(int xid, Buffer request, int startOffset) throws IOException {
    // Create reply
    final int rpcHeaderLength = RpcConstants.RPC_ACCEPTED_REPLY_HEADER_LENGTH;
    ByteBuffer rpcBodyBuffer = RpcUtil.createAcceptedSuccessReplyHeaderBuffer(xid);

    // NFS_ACL GETACL reply
    // Structure:
    // status (4 bytes)
    // post_op_attr present flag (4 bytes)
    // acl_count (4 bytes)
    // acl_entries (variable length)
//    int rpcNfsLength = 4 + // status
//        4 + // post_op_attr present flag
//        4;  // acl_count (0 for now)

    PostOpAttr postOpAttr = PostOpAttr.builder().attributesFollow(0).build();
    GETACL3resfail getacl3resfail = GETACL3resfail.builder().objAttributes(postOpAttr).build();

    GETACL3res getacl3res = GETACL3res.createFailure(NfsStat3.NFS3ERR_NOTSUPP, getacl3resfail);

    int rpcNfsLength = getacl3res.getSerializedSize();
    ByteBuffer rpcNfsBuffer = ByteBuffer.allocate(rpcNfsLength);
    rpcNfsBuffer.order(ByteOrder.BIG_ENDIAN);

    getacl3res.serialize(rpcNfsBuffer);

    // Record marking
    int recordMarkValue = 0x80000000 | (rpcHeaderLength + rpcNfsLength);

    ByteBuffer fullResponseBuffer = ByteBuffer.allocate(4 + rpcHeaderLength + rpcNfsLength);
    fullResponseBuffer.order(ByteOrder.BIG_ENDIAN);
    fullResponseBuffer.putInt(recordMarkValue);
    fullResponseBuffer.put(rpcBodyBuffer.array());
    fullResponseBuffer.put(rpcNfsBuffer.array());

    return fullResponseBuffer.array();
  }

  private Flowable<Buffer> createNfsSetAttrReply(int xid, Buffer request, int startOffset) throws IOException, IllegalAccessException {
    // Parse file handle from request
    //int fhandleLength = request.getInt(startOffset);
    //byte[] fhandle = request.slice(startOffset + 4, startOffset + 4 + fhandleLength).getBytes();
    byte[] requestByteData = request.slice(startOffset, request.length()).getBytes();
    SETATTR3args setattr3args = SETATTR3args.deserialize(requestByteData);

    // Create reply
    final int rpcHeaderLength = RpcConstants.RPC_ACCEPTED_REPLY_HEADER_LENGTH;
    Flowable<Buffer> rpcHeaderBuffer = RpcUtil.writeAcceptedSuccessReplyHeader(xid);

    // NFS SETATTR reply
    // Structure:
    // status (4 bytes)
    // pre_op_attr present flag (4 bytes)
    // post_op_attr present flag (4 bytes)
    //FAttr3 attributes = fileHandleToFAttr3.get();
    ByteArrayKeyWrapper byteArrayKeyWrapper = new ByteArrayKeyWrapper(setattr3args.getObject().getFileHandle());
    fileHandleToFAttr3.computeIfPresent(byteArrayKeyWrapper, (key, value) -> {
      SetAttr3 newAttributes = setattr3args.getNewAttributes();
      int modeSetIt = newAttributes.getMode().getSetIt();
      if (modeSetIt != 0) {
        int mode = newAttributes.getMode().getMode();
        value.setMode(mode);
      }
      int uidSetIt = newAttributes.getUid().getSetIt();
      if (uidSetIt != 0) {
        int uid = newAttributes.getUid().getUid();
        value.setUid(uid);
      }
      int gidSetIt = newAttributes.getGid().getSetIt();
      if (gidSetIt != 0) {
        int gid = newAttributes.getGid().getGid();
        value.setGid(gid);
      }
      int sizeSetIt = newAttributes.getSize().getSetIt();
      if (sizeSetIt != 0) {
        long size = newAttributes.getSize().getSize();
        value.setSize(size);
      }
      int atimeSetToServerTime = newAttributes.getAtime();
      int mtimeSetToServerTIme = newAttributes.getMtime();
      long currentTimeMillis = System.currentTimeMillis();
      int seconds = (int)(currentTimeMillis / 1000);
      int nseconds = (int)((currentTimeMillis % 1000) * 1_000_000);
      if (atimeSetToServerTime != 0) {
        value.setAtimeSeconds(seconds);
        value.setAtimeNseconds(nseconds);
      }
      if (mtimeSetToServerTIme != 0) {
        value.setMtimeSeconds(seconds);
        value.setMtimeNseconds(nseconds);
      }

      return value;
    });
    FAttr3 attributes = fileHandleToFAttr3.get(byteArrayKeyWrapper);

    PreOpAttr before = PreOpAttr.builder().attributesFollow(0).build();
    PostOpAttr after = PostOpAttr.builder()
      .attributesFollow(1)
      .attributes(attributes)
      .build();
    WccData objWcc = WccData.builder()
      .before(before)
      .after(after)
      .build();
    SETATTR3resok setattr3resok = SETATTR3resok.builder()
      .objWcc(objWcc)
      .build();

    SETATTR3res setattr3res = SETATTR3res.createSuccess(setattr3resok);

    int rpcNfsLength = setattr3res.getSerializedSize();
    Flowable<Buffer> rpcNfsBuffer = setattr3res.serializeToFlowable();

    // Record marking
    int recordMarkValue = 0x80000000 | (rpcHeaderLength + rpcNfsLength);
    Buffer buffer = Buffer.buffer(4).appendInt(recordMarkValue);
    Flowable<Buffer> fullResponseBuffer = Flowable.concat(Flowable.just(buffer), rpcHeaderBuffer, rpcNfsBuffer);

    return fullResponseBuffer;
  }

  private Flowable<Buffer> createNfsAccessReply(int xid, Buffer request, int startOffset) throws IOException {
    // Parse file handle from request
    int fhandleLength = request.getInt(startOffset);
    byte[] fhandle = request.slice(startOffset + 4, startOffset + 4 + fhandleLength).getBytes();

    // Parse access request flags
    int accessRequest = request.getInt(startOffset + 4 + fhandleLength);
    log.info("ACCESS request - handle length: {}, access request: 0x{}",
        fhandleLength, Integer.toHexString(accessRequest));

    // Create reply
    int rpcHeaderLength = RpcConstants.RPC_ACCEPTED_REPLY_HEADER_LENGTH;
    Flowable<Buffer> rpcHeaderBuffer = RpcUtil.writeAcceptedSuccessReplyHeader(xid);

    // NFS ACCESS reply
    // Determine file type from handle
    long fileId = fileHandleToFileId.getOrDefault(new ByteArrayKeyWrapper(fhandle), 0x0000000002000002L);
    String filename = fileIdToFileName.getOrDefault(fileId, "/");
    // File attributes
    int fileType =  getFileType(filename);
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

    FAttr3 dirAttr3 = fileHandleToFAttr3.getOrDefault(new ByteArrayKeyWrapper(fhandle), null);
    PostOpAttr objAttributes = PostOpAttr.builder()
      .attributesFollow(dirAttr3 != null ? 1 : 0)
      .attributes(dirAttr3)
      .build();

    ACCESS3resok access3resok = ACCESS3resok.builder()
      .objAttributes(objAttributes)
      .accessFlags(accessFlags)
      .build();

    ACCESS3res access3res = ACCESS3res.createOk(access3resok);

    int rpcNfsLength = access3res.getSerializedSize();
    Flowable<Buffer> rpcNfsBuffer = access3res.serializeToFlowable();

    // Record marking
    int recordMarkValue = 0x80000000 | (rpcHeaderLength + rpcNfsLength);
    Buffer buffer = Buffer.buffer(4).appendInt(recordMarkValue);
    Flowable<Buffer> fullResponseBuffer = Flowable.concat(Flowable.just(buffer), rpcHeaderBuffer, rpcNfsBuffer);

    return fullResponseBuffer;
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

  private final Map<ByteArrayKeyWrapper, AtomicLong> fileHandleAdder = new ConcurrentHashMap<>();

  private Flowable<Buffer> createNfsCommitReply(int xid, Buffer request, int startOffset) throws IOException, URISyntaxException {
    int fileHandleLength = request.getInt(startOffset);
    byte[] fileHandle = request.slice(startOffset + 4, startOffset + 4 + fileHandleLength).getBytes();
    int padding = (fileHandleLength + 4 - 1) / 4 * 4;
    int offset = request.getInt(startOffset + 4 + padding);
    int count = request.getInt(startOffset + 4 + padding + 4);

    // Create reply
    final int rpcHeaderLength = RpcConstants.RPC_ACCEPTED_REPLY_HEADER_LENGTH;
    Flowable<Buffer> rpcHeaderBuffer = RpcUtil.writeAcceptedSuccessReplyHeader(xid);

    // NFS COMMIT reply
    // Structure:
    // status (4 bytes)
    // verf (8 bytes)
    // wcc_data
    //   pre_op_attr present flag (4 bytes)
    //   post_op_attr present flag (4 bytes)

    ByteArrayKeyWrapper keyWrapper = new ByteArrayKeyWrapper(fileHandle);
    AtomicLong nextAppendingPosition = fileHandleNextAppendingPosition.compute(keyWrapper, (k, v) -> {
      if (v == null) return new AtomicLong(0);
      return v;
    });

    Single<COMMIT3res> response = Single.never();
    List<Buffer> currentDataList = requestCache.remove(keyWrapper);
    if (currentDataList != null) {
      log.info("commit -- synchronized (currentDataList) begin");

      synchronized (currentDataList) {
//        Buffer combinedBuffer = Buffer.buffer();
//        for (Buffer currentData : currentDataList) {
//          combinedBuffer.appendBuffer(currentData);
//        }
//        currentDataList.clear();
        //long bufferLength = combinedBuffer.length();
//g.info("commit -- upload buffer length: {}, offset: {}, count: {}", combinedBuffer.length(), nextAppendingPosition.getAndAdd(bufferLength), bufferLength);
        String filename = fileHandleToFileName.getOrDefault(keyWrapper,"");
        //long position = adder.addAndGet(combinedBuffer.length());
        int bufferLength = currentDataList.stream().mapToInt(Buffer::length).sum();
        Flowable<Buffer> fileBodyFlowable = Flowable.fromIterable(currentDataList);

        String targetUrl = String.format("http://%s/%s/%s?append=&position=%s", S3HOST, BUCKET, filename, nextAppendingPosition.getAndAdd(bufferLength));

        log.info("commit start -- upload buffer length: {}, offset: {}, count: {}", bufferLength, nextAppendingPosition.get(), bufferLength);
        response = upDownHttpClient.post(targetUrl, fileBodyFlowable, bufferLength).subscribeOn(Schedulers.newThread()).observeOn(Schedulers.newThread())
          .doOnSuccess(buffer -> {
            log.info("upload buffer length: {} -- commit end ---", buffer.toString());
          })
          .flatMap(buffer -> {
            FAttr3 attritbutes = fileHandleToFAttr3.computeIfPresent(keyWrapper, (key, value) -> {
              long used = (nextAppendingPosition.get() + 4096 - 1) / 4096 * 4096;
              value.setSize(nextAppendingPosition.get());
              value.setUsed(used);
              return value;
            });

            PreOpAttr befor = PreOpAttr.builder().attributesFollow(0).build();
            PostOpAttr after = PostOpAttr.builder().attributesFollow(attritbutes != null ? 1 : 0).attributes(attritbutes).build();

            WccData fileWcc =  WccData.builder().before(befor).after(after).build();
            COMMIT3resok commit3resok = COMMIT3resok.builder().fileWcc(fileWcc).verifier(0L).build();

            return Single.just(COMMIT3res.createSuccess(commit3resok));
          });
      }
    } else {
      log.info("commit -- else branch, offset: {}, count: {}", offset, count);

      FAttr3 attritbutes = fileHandleToFAttr3.getOrDefault(keyWrapper, null);

      PreOpAttr befor = PreOpAttr.builder().attributesFollow(0).build();
      PostOpAttr after = PostOpAttr.builder().attributesFollow(attritbutes != null ? 1 : 0).attributes(attritbutes).build();

      WccData fileWcc =  WccData.builder().before(befor).after(after).build();
      COMMIT3resok commit3resok = COMMIT3resok.builder().fileWcc(fileWcc).verifier(0L).build();

      response = Single.just(COMMIT3res.createSuccess(commit3resok));
    }

    return response
      .flatMapPublisher(commit3res -> {
        int rpcNfsLength = commit3res.getSerializedSize();

        Flowable<Buffer> rpcNfsBuffer = commit3res.serializeToFlowable();
        // Record marking
        int recordMarkValue = 0x80000000 | (rpcHeaderLength + rpcNfsLength);
        Buffer buffer = Buffer.buffer(4).appendInt(recordMarkValue);

        return Flowable.concat(Flowable.just(buffer), rpcHeaderBuffer, rpcNfsBuffer);
      });
  }

  private Flowable<Buffer> createNfsCreateReply(int xid, Buffer request, int startOffset) throws IOException {
    int dirFhandleLength = request.getInt(startOffset);
    byte[] dirFhandle = request.slice(startOffset + 4, startOffset + 4 + dirFhandleLength).getBytes();
    int nameLength = request.getInt(startOffset + 4 + dirFhandleLength);
    nameLength = (nameLength + 4 - 1 ) / 4 * 4;
    String name = request.slice(startOffset + 4 + dirFhandleLength + 4,
      startOffset + 4 + dirFhandleLength + 4 + nameLength).toString("UTF-8").trim();
    int createMode = request.getInt(startOffset + 4 + dirFhandleLength + 4 + nameLength);
    long verifier = request.getLong(startOffset + 4 + dirFhandleLength + 4 + nameLength + 4);

    // Create reply
    final int rpcHeaderLength = RpcConstants.RPC_ACCEPTED_REPLY_HEADER_LENGTH;
    Flowable<Buffer> rpcHeaderBuffer = RpcUtil.writeAcceptedSuccessReplyHeader(xid);

    byte[] fileHandle = getFileHandle(name, true);
    int fileHandleLength = fileHandle.length;

    // NFS CREATE reply
    // Structure:
    // status (4 bytes)
    // file handle (32 bytes)
    // post_op_attr present flag (4 bytes)
    // wcc_data
    //   pre_op_attr present flag (4 bytes)
    //   post_op_attr present flag (4 bytes)

    // Object attributes
    // Determine file type based on name
    int fileType;
    fileType = getFileType(name);
    long currentTimeMillis = System.currentTimeMillis();
    int seconds = (int) (currentTimeMillis / 1000);
    int nseconds = (int) ((currentTimeMillis % 1000) * 1_000_000);
    long fileId = getFileId(name);

    NfsFileHandle3 nfsFileHandle = NfsFileHandle3.builder()
      .handleOfLength(fileHandleLength)
      .fileHandle(fileHandle)
      .build();

    PostOpFileHandle3 obj = PostOpFileHandle3.builder()
      .handleFollows(1)
      .nfsFileHandle(nfsFileHandle)
      .build();

    FAttr3 attributes = FAttr3.builder()
      .type(fileType)
      .mode(0)
      .nlink(0)
      .uid(0)
      .gid(0)
      .size(0)
      .used(0)
      .rdev(0)
      .fsidMajor(0x08c60040)
      .fsidMinor(0x2b5cd8a8)
      .fileid(fileId)
      .ctimeSeconds(seconds)
      .ctimeNseconds(nseconds)
      .build();

    int nlink = 0;
    fileHandleToFileId.put(new ByteArrayKeyWrapper(fileHandle), fileId);
    fileHandleToFileName.put(new ByteArrayKeyWrapper(fileHandle), name);
    fileIdToFileName.put(fileId, name);
    fileIdToFAttr3.put(fileId, attributes);
    fileHandleToFAttr3.put(new ByteArrayKeyWrapper(fileHandle), attributes);
    if (fileType == 2) {
      fileHandleToParentFileHandle.put(new ByteArrayKeyWrapper(fileHandle), new ByteArrayKeyWrapper(dirFhandle));

      fileHandleToChildrenFileHandle.compute(new ByteArrayKeyWrapper(fileHandle), (key, value) -> {
        if (value == null) {
          value = new ArrayList<>();
        }
        boolean add = value.add(new ByteArrayKeyWrapper(fileHandle));
        value.add(new ByteArrayKeyWrapper(dirFhandle));
        return value;
      });
      nlink++;
    }
    fileHandleToChildrenFileHandle.compute(new ByteArrayKeyWrapper(dirFhandle), (key, value) -> {
      if (value == null) {
        value = new ArrayList<>();
      }
      boolean add = value.add(new ByteArrayKeyWrapper(fileHandle));
      return value;
    });
    nlink++;


    attributes.setNlink(nlink);
    PostOpAttr ojbAttributes = PostOpAttr.builder().attributesFollow(1).attributes(attributes).build();
    PreOpAttr before = PreOpAttr.builder().attributesFollow(0).build();
    PostOpAttr after = PostOpAttr.builder().attributesFollow(0).build();
    WccData dirWcc = WccData.builder().before(before).after(after).build();

    CREATE3resok create3resok = CREATE3resok.builder()
      .obj(obj)
      .ojbAttributes(ojbAttributes)
      .dirWcc(dirWcc)
      .build();

    CREATE3res create3res = CREATE3res.createSuccess(create3resok);

    int rpcNfsLength = create3res.getSerializedSize();
    Flowable<Buffer> rpcNfsBuffer = create3res.serializeToFlowable();

    // Record marking
    int recordMarkValue = 0x80000000 | (rpcHeaderLength + rpcNfsLength);
    Buffer buffer = Buffer.buffer(4).appendInt(recordMarkValue);
    Flowable<Buffer> fullResponseBuffer = Flowable.concat(Flowable.just(buffer), rpcHeaderBuffer, rpcNfsBuffer);

    return fullResponseBuffer;
  }

  private Flowable<Buffer> createNfsMkdirReply(int xid, Buffer request, int startOffset) throws IOException {
    MKDIR3args mkdir3args = new MKDIR3args();
    mkdir3args.deserialize(request, startOffset);

    // Create reply
    final int rpcHeaderLength = RpcConstants.RPC_ACCEPTED_REPLY_HEADER_LENGTH;
    Flowable<Buffer> rpcHeaderBuffer = RpcUtil.writeAcceptedSuccessReplyHeader(xid);

    // NFS MKDIR reply
    // Structure:
    // status (4 bytes)
    // file handle (32 bytes)
    // post_op_attr present flag (4 bytes)
    // wcc_data
    //   pre_op_attr present flag (4 bytes)
    //   post_op_attr present flag (4 bytes)
    long currentTimeMillis = System.currentTimeMillis();
    int seconds = (int) (currentTimeMillis / 1000);
    int nseconds = (int) ((currentTimeMillis % 1000) * 1_000_000);

    String dirName = new String(mkdir3args.where.filename, StandardCharsets.UTF_8);
    byte[] dirHandle = getFileHandle(dirName, true);
    long fileId = getFileId(dirName);
    int parentDirLength = mkdir3args.where.dir.handleOfLength;
    byte[] parentDirHandle = mkdir3args.where.dir.fileHandle;

    NfsFileHandle3 nfsFileHandle3 = NfsFileHandle3.builder().handleOfLength(dirHandle.length).fileHandle(dirHandle).build();
    PostOpFileHandle3 obj = PostOpFileHandle3.builder().handleFollows(1).nfsFileHandle(nfsFileHandle3).build();
    FAttr3 dirAttr = generateDirFAttr3(fileId, seconds, nseconds, mkdir3args);

    fileHandleToChildrenFileHandle.compute(new ByteArrayKeyWrapper(dirHandle), (key, value) -> {
      if (value == null) {
        value = new ArrayList<>();
      }
      value.add(new ByteArrayKeyWrapper(dirHandle));
      value.add(new ByteArrayKeyWrapper(parentDirHandle));
      return value;
    });
    dirAttr.setNlink(2);

    PostOpAttr objAttr = PostOpAttr.builder().attributesFollow(1).attributes(dirAttr).build();
    WccAttr wccAttr = WccAttr.builder().size(dirAttr.getSize()).ctimeSeconds(dirAttr.getCtimeSeconds()).ctimeNSeconds(dirAttr.getCtimeNseconds())
      .mtimeSeconds(dirAttr.getMtimeSeconds()).mtimeNSeconds(dirAttr.getMtimeNseconds()).build();
    PreOpAttr preOpAttr = PreOpAttr.builder().attributesFollow(1).attributes(wccAttr).build();

    fileHandleToChildrenFileHandle.compute(new ByteArrayKeyWrapper(parentDirHandle), (key, value) -> {
      if (value == null) {
        value = new ArrayList<>();
      }
      value.add(new ByteArrayKeyWrapper(dirHandle));
      return value;
    });
    fileHandleToFileName.put(new ByteArrayKeyWrapper(dirHandle), dirName);
    fileHandleToFAttr3.put(new ByteArrayKeyWrapper(dirHandle), dirAttr);
    fileHandleToFileId.put(new ByteArrayKeyWrapper(dirHandle), fileId);

    AtomicReference<FAttr3> fAttr3AtomicReference = new AtomicReference<>();
    FAttr3 fAttr3 = fileHandleToFAttr3.get(new ByteArrayKeyWrapper(parentDirHandle));
    fAttr3AtomicReference.set(fAttr3);
    fAttr3AtomicReference.get().setNlink(fAttr3AtomicReference.get().getNlink() + 1);
    PostOpAttr postOpAttr = PostOpAttr.builder().attributesFollow(1).attributes(fAttr3AtomicReference.get()).build();
    WccData wccData = WccData.builder().before(preOpAttr).after(postOpAttr).build();
    MKDIR3resok mkdir3resok = MKDIR3resok.builder().obj(obj).objAttributes(objAttr).dirWcc(wccData).build();
    MKDIR3res mkdir3res = MKDIR3res.createOk(mkdir3resok);
    int rpcNfsLength = mkdir3res.getSerializedSize();
    Flowable<Buffer> rpcNfsBuffer = mkdir3res.serializeToFlowable();

    // Record marking
    int recordMarkValue = 0x80000000 | (rpcHeaderLength + rpcNfsLength);
    Buffer buffer = Buffer.buffer(4).appendInt(recordMarkValue);
    Flowable<Buffer> fullResponseBuffer = Flowable.concat(Flowable.just(buffer), rpcHeaderBuffer, rpcNfsBuffer);

    return fullResponseBuffer;
  }

  private static FAttr3 generateDirFAttr3(long fileId, int seconds, int nseconds, MKDIR3args mkdir3args) {
    if (mkdir3args == null) {
      throw new IllegalArgumentException("mkdir3args must not be null");
    }

    FAttr3 dirAttr = FAttr3.builder().type(2).mode(0755).nlink(0).uid(0).gid(0).size(4096).used(4096).rdev(0L)
      .fsidMajor(0x08c60040).fsidMinor(0x2b5cd8a8).fileid(fileId).ctimeSeconds(seconds).ctimeNseconds(nseconds)
      .atimeSeconds(seconds).atimeNseconds(nseconds).mtimeSeconds(seconds).mtimeNseconds(nseconds).build();

    SetAttr3 attr3 = mkdir3args.attributes;
    int modeSetIt = attr3.getMode().getSetIt();
    if (modeSetIt != 0) {
      int mode = attr3.getMode().getMode();
      dirAttr.setMode(mode);
    }
    int uidSetIt = attr3.getUid().getSetIt();
    if (uidSetIt != 0) {
      int uid = attr3.getUid().getUid();
      dirAttr.setUid(uid);
    }
    int gidSetIt = attr3.getGid().getSetIt();
    if (gidSetIt != 0) {
      int gid = attr3.getGid().getGid();
      dirAttr.setGid(gid);
    }
    int sizeSetIt = attr3.getSize().getSetIt();
    if (sizeSetIt != 0) {
      long size = attr3.getSize().getSize();
      dirAttr.setSize(size);
    }
    int atimeSetToServerTime = attr3.getAtime();
    int mtimeSetToServerTIme = attr3.getMtime();

    if (atimeSetToServerTime != 0) {
      dirAttr.setAtimeSeconds(seconds);
      dirAttr.setAtimeNseconds(nseconds);
    }
    if (mtimeSetToServerTIme != 0) {
      dirAttr.setMtimeSeconds(seconds);
      dirAttr.setMtimeNseconds(nseconds);
    }

    return dirAttr;
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

  private Flowable<Buffer> createNfsRemoveReply(int xid, Buffer request, int startOffset) throws IOException, URISyntaxException {
    int dirFhandleLength = request.getInt(startOffset);
    byte[] dirFhandle = request.slice(startOffset + 4, startOffset + 4 + dirFhandleLength).getBytes();
    int nameLength = request.getInt(startOffset + 4 + dirFhandleLength);
    String name = request.slice(startOffset + 4 + dirFhandleLength + 4,
      startOffset + 4 + dirFhandleLength + 4 + nameLength).toString("UTF-8");

    // Create reply
    final int rpcHeaderLength = 24;
    Flowable<Buffer> rpcHeaderBuffer = RpcUtil.writeAcceptedSuccessReplyHeader(xid);

    ByteArrayKeyWrapper byteArrayKeyWrapper = new ByteArrayKeyWrapper(dirFhandle);
    FAttr3 fAttr3 = fileHandleToFAttr3.getOrDefault(byteArrayKeyWrapper, null);

    REMOVE3res remove3res = null;
    if (fAttr3 != null) {
      int euid = fAttr3.getUid();
      if (euid == 0) {
        List<ByteArrayKeyWrapper> subEntries = fileHandleToChildrenFileHandle.getOrDefault(byteArrayKeyWrapper, null);
        AtomicReference<ByteArrayKeyWrapper> shouldDeletedSubEntry = new AtomicReference<>();
        subEntries.forEach(subEntry -> {
          String filename = fileHandleToFileName.getOrDefault(subEntry, null);
          if (filename != null && filename.equals(name)) {
            shouldDeletedSubEntry.set(subEntry);
          }
        });
        if (shouldDeletedSubEntry.get() != null) {
          String filename = fileHandleToFileName.getOrDefault(shouldDeletedSubEntry.get(),"");
          String targetUrl = String.format("http://%s/%s/%s", S3HOST, BUCKET, filename);
          Single<Buffer> bufferSingle = upDownHttpClient.delete(targetUrl).subscribeOn(Schedulers.single()).observeOn(Schedulers.single());
          bufferSingle.subscribe();

          subEntries.remove(shouldDeletedSubEntry.get());
          fileHandleToParentFileHandle.remove(shouldDeletedSubEntry.get());
          fileHandleToFileId.remove(shouldDeletedSubEntry.get());
          fileHandleToFAttr3.remove(shouldDeletedSubEntry.get());
          fileHandleToFileName.remove(shouldDeletedSubEntry.get());

          PreOpAttr preOpAttr = PreOpAttr.builder().attributesFollow(0).build();
          PostOpAttr postOpAttr = PostOpAttr.builder().attributesFollow(1).attributes(fAttr3).build();
          WccData wccData = WccData.builder().before(preOpAttr).after(postOpAttr).build();
          REMOVE3resok remove3resok = REMOVE3resok.builder().dirWcc(wccData).build();
          remove3res = REMOVE3res.createOk(remove3resok);
        } else {
          PreOpAttr preOpAttr = PreOpAttr.builder().attributesFollow(0).build();
          PostOpAttr postOpAttr = PostOpAttr.builder().attributesFollow(1).attributes(fAttr3).build();
          WccData wccData = WccData.builder().before(preOpAttr).after(postOpAttr).build();
          REMOVE3resfail remove3resfail = REMOVE3resfail.builder().dirWcc(wccData).build();
          remove3res = REMOVE3res.createFail(NfsStat3.NFS3ERR_NOENT, remove3resfail);
        }
      } else {
        PreOpAttr preOpAttr = PreOpAttr.builder().attributesFollow(0).build();
        PostOpAttr postOpAttr = PostOpAttr.builder().attributesFollow(1).attributes(fAttr3).build();
        WccData wccData = WccData.builder().before(preOpAttr).after(postOpAttr).build();
        REMOVE3resfail remove3resfail = REMOVE3resfail.builder().dirWcc(wccData).build();
        remove3res = REMOVE3res.createFail(NfsStat3.NFS3ERR_PERM, remove3resfail);
      }
    } else {
      PreOpAttr preOpAttr = PreOpAttr.builder().attributesFollow(0).build();
      PostOpAttr postOpAttr = PostOpAttr.builder().attributesFollow(1).attributes(fAttr3).build();
      WccData wccData = WccData.builder().before(preOpAttr).after(postOpAttr).build();
      REMOVE3resfail remove3resfail = REMOVE3resfail.builder().dirWcc(wccData).build();
      remove3res = REMOVE3res.createFail(NfsStat3.NFS3ERR_BADHANDLE, remove3resfail);
    }

    if (remove3res == null) {
      throw new RuntimeException();
    }

    // NFS REMOVE reply
    // Structure:
    // status (4 bytes)
    // wcc_data
    //   pre_op_attr present flag (4 bytes)
    //   post_op_attr present flag (4 bytes)
    int rpcNfsLength = remove3res.getSerializedSize();
    Flowable<Buffer> rpcNfsBuffer = remove3res.serializeToFlowable();

    // Record marking
    int recordMarkValue = 0x80000000 | (rpcHeaderLength + rpcNfsLength);
    Buffer buffer = Buffer.buffer(4).appendInt(recordMarkValue);
    Flowable<Buffer> fullResponseBuffer = Flowable.concat(Flowable.just(buffer), rpcHeaderBuffer, rpcNfsBuffer);

    return fullResponseBuffer;
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
    rpcNfsBuffer.putLong(0L); // cookie verifier

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

//  private boolean checkWriteAndExecutePermission(int euid, int dirid, int mode, int dirMode) {
//    // 1. Check write and execute permission on parent directory for the current user
//    boolean can_write_dir = false;
//    boolean can_execute_dir = false;
//  }

  private Flowable<Buffer> createNfsReadDirPlusReply(int xid, Buffer request, int startOffset) throws IOException {
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
    int dircount = request.getInt(cookieVeriferOffset + 8);
    int maxcount = request.getInt(cookieVeriferOffset + 12);
    log.info("READDIRPLUS request parameters - dircount: {} bytes, maxcount: {} bytes", dircount, maxcount);

    // Create reply
    int rpcHeaderLength = RpcConstants.RPC_ACCEPTED_REPLY_HEADER_LENGTH;
    Flowable<Buffer> rpcHeaderBuffer = RpcUtil.writeAcceptedSuccessReplyHeader(xid);

    // Define directory entries (simulating a large directory)
//    List<String> allEntries = fileHandleToChildrenFileHandle.getOrDefault(new ByteArrayKeyWrapper(dirFhandle), new ArrayList<>())
//      .stream()
//      .map(key -> fileHandleToFileName.getOrDefault(key, ""))
//      .filter(s -> !s.isEmpty())
//      .collect(Collectors.toList());

   // List<ByteArrayKeyWrapper> allEntries = fileHandleToChildrenFileHandle.getOrDefault(byteArrayKeyWrapper, new ArrayList<>());
//    allEntries.add(byteArrayKeyWrapper);
//    ByteArrayKeyWrapper parentDir = fileHandleToParentFileHandle.getOrDefault(byteArrayKeyWrapper, null);
//    if (parentDir != null) {
//      allEntries.add(parentDir);
//    }
    // Calculate size for attributes
    int startIndex = (int) cookie;
    int currentSize = 0;
    int entriesToReturn = 0;
    int nameAttrSize = Nfs3Constant.NAME_ATTR_SIZE;

    long currentTimeMillis = System.currentTimeMillis();
    int seconds = (int)(currentTimeMillis / 1000);
    int nseconds = (int)((currentTimeMillis % 1000) * 1_000_000);

    List<Entryplus3> entries = new ArrayList<>();
    ByteArrayKeyWrapper byteArrayKeyWrapper = new ByteArrayKeyWrapper(dirFhandle);
    List<ByteArrayKeyWrapper> allEntries = fileHandleToChildrenFileHandle.getOrDefault(byteArrayKeyWrapper, new ArrayList<>());
    int totalEntrySize = allEntries.size();
//    Flowable<Entryplus3> entryplus3Flowable = Flowable.fromIterable(allEntries)
//      .zipWith(Flowable.range(0, totalEntrySize > 0 ? totalEntrySize : 0),
//        ((byteArrayKeyWrapper1, index) -> {
//          return new IndexedItem<ByteArrayKeyWrapper>(index, byteArrayKeyWrapper1);
//        }))
//      .map(byteArrayKeyWrapperIndexedItem -> {
//        ByteArrayKeyWrapper keyWrapper = byteArrayKeyWrapperIndexedItem.getValue();
//        int index = byteArrayKeyWrapperIndexedItem.getIndex();
//        String entryName = fileHandleToFileName.getOrDefault(keyWrapper, ".");
//        int entryNameLength = entryName.length();
//        long fileId = getFileId(entryName);
//        byte[] nameBytes = entryName.getBytes(StandardCharsets.UTF_8);
//        long nextCookie = 0;
//        if (index == allEntries.size() - 1) {
//          // 注意：
//          // cookie 不返回这个会导致无限循环
//          nextCookie = 0x7fffffffffffffffL;
//        } else {
//          nextCookie = index + 1;
//        }
//
//        log.info("Entry '{}' size: {} bytes, current total: {} bytes (dircount limit: {} bytes)",
//          entryName, entryNameLength, currentSize, dircount);
//
//        FAttr3 nameAttr = fileHandleToFAttr3.getOrDefault(keyWrapper, null);
//        Entryplus3 entryplus3 = Entryplus3.builder()
//          .fileid(fileId)
//          .fileNameLength(entryNameLength)
//          .fileName(nameBytes)
//          .cookie(nextCookie)
//          .nameAttrPresent(nameAttr != null ? 1 : 0)
//          .nameAttr(nameAttr)
//          .nameHandlePresent(1)
//          .nameHandleLength(keyWrapper.getData().length)
//          .nameHandle(keyWrapper.getData())
//          .nextEntryPresent(index == allEntries.size() - 1 ? 0 : 1)
//          .build();
//
//        return entryplus3;
//      });
//
//    Single<READDIRPLUS3res> readdirplus3resSingle = entryplus3Flowable.collect(
//      ArrayList::new, // 初始收集器 (supplier)
//      List::add        // 如何将项添加到收集器 (accumulator)
//    ).flatMap(listEntryplus3 -> {
//      if (listEntryplus3.isEmpty()) {
//        FAttr3 dirAttr3 = fileHandleToFAttr3.getOrDefault(new ByteArrayKeyWrapper(dirFhandle), null);
//        PostOpAttr dirAttributes = PostOpAttr.builder()
//          .attributesFollow(dirAttr3 != null ? 1 : 0)
//          .attributes(dirAttr3)
//          .build();
//        READDIRPLUS3resfail readdirplus3resfail = READDIRPLUS3resfail.builder().dirAttributes(dirAttributes).build();
//
//        return Single.just(READDIRPLUS3res.createFail(NfsStat3.NFS3ERR_NOENT, readdirplus3resfail));
//      }
//
//      FAttr3 dirAttr = fileHandleToFAttr3.getOrDefault(byteArrayKeyWrapper, null);
//      PostOpAttr dirAttributes = PostOpAttr.builder().attributesFollow(1).attributes(dirAttr).build();
//      READDIRPLUS3resok readdirplus3resok = READDIRPLUS3resok.builder()
//        .dirAttributes(dirAttributes)
//        .cookieverf(0L)
//        .entriesPresentFlag(1)
//        .entries(entries)
//        .eof(1)
//        .build();
//      return Single.just(READDIRPLUS3res.createOk(readdirplus3resok));
//    });
//
//    readdirplus3resSingle.subscribe();
    ByteArrayKeyWrapper parentDirKey = new ByteArrayKeyWrapper(dirFhandle);

    for (int i = 0; i < allEntries.size(); i++) {
      ByteArrayKeyWrapper keyWrapper = allEntries.get(i);
      String entryName = "";
      if (keyWrapper.equals(parentDirKey)) {
        entryName = "..";
      } else if (keyWrapper.equals(byteArrayKeyWrapper)) {
        entryName = ".";
      } else {
        entryName = fileHandleToFileName.getOrDefault(keyWrapper, ".");
      }

      int entryNameLength = entryName.length();
      long fileId = getFileId(entryName);
      byte[] nameBytes = entryName.getBytes(StandardCharsets.UTF_8);
      long nextCookie = 0;
      if (i == allEntries.size() - 1) {
        // 注意：
        // cookie 不返回这个会导致无限循环
        nextCookie = 0x7fffffffffffffffL;
      } else {
        nextCookie = i + 1;
      }

      log.info("Entry '{}' size: {} bytes, current total: {} bytes (dircount limit: {} bytes)",
        entryName, entryNameLength, currentSize, dircount);

      FAttr3 nameAttr = fileHandleToFAttr3.getOrDefault(keyWrapper, null);
      Entryplus3 entryplus3 = Entryplus3.builder()
        .fileid(fileId)
        .fileNameLength(entryNameLength)
        .fileName(nameBytes)
        .cookie(nextCookie)
        .nameAttrPresent(nameAttr != null ? 1 : 0)
        .nameAttr(nameAttr)
        .nameHandlePresent(1)
        .nameHandleLength(keyWrapper.getData().length)
        .nameHandle(keyWrapper.getData())
        .nextEntryPresent(i == allEntries.size() - 1 ? 0 : 1)
        .build();

      entries.add(entryplus3);
    }

    int entriesPresentFlag = entries.isEmpty() ? 0 : 1;

    READDIRPLUS3res readdirplus3res = null;
    if (entriesPresentFlag != 0) {
      FAttr3 dirAttr = fileHandleToFAttr3.getOrDefault(byteArrayKeyWrapper, null);
      PostOpAttr dirAttributes = PostOpAttr.builder().attributesFollow(1).attributes(dirAttr).build();
      READDIRPLUS3resok readdirplus3resok = READDIRPLUS3resok.builder()
        .dirAttributes(dirAttributes)
        .cookieverf(0L)
        .entriesPresentFlag(entriesPresentFlag)
        .entries(entries)
        .eof(1)
        .build();
      readdirplus3res = READDIRPLUS3res.createOk(readdirplus3resok);
    } else {
      FAttr3 dirAttr3 = fileHandleToFAttr3.getOrDefault(new ByteArrayKeyWrapper(dirFhandle), null);
      PostOpAttr dirAttributes = PostOpAttr.builder()
        .attributesFollow(dirAttr3 != null ? 1 : 0)
        .attributes(dirAttr3)
        .build();
      READDIRPLUS3resfail readdirplus3resfail = READDIRPLUS3resfail.builder().dirAttributes(dirAttributes).build();

      readdirplus3res = READDIRPLUS3res.createFail(NfsStat3.NFS3ERR_NOENT, readdirplus3resfail);
    }

    int rpcNfsLength = readdirplus3res.getSerializedSize();

    Flowable<Buffer> rpcNfsBuffer2 =  readdirplus3res.serializeToFlowable();

    // Record marking
    int recordMarkValue = 0x80000000 | (rpcHeaderLength + rpcNfsLength);
    Buffer buffer = Buffer.buffer(4).appendInt(recordMarkValue);
    Flowable<Buffer> fullResponseBuffer = Flowable.concat(Flowable.just(buffer), rpcHeaderBuffer, rpcNfsBuffer2);

    return fullResponseBuffer;
  }

}
