package com.example.netclient.utils;

import com.example.netclient.enums.RpcConstants;
import io.reactivex.Flowable;
import io.vertx.core.buffer.Buffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class RpcUtil {
  /**
   * 将标准的RPC成功响应头部写入给定的ByteBuffer。
   * 调用者需要确保ByteBuffer有足够的剩余空间，并且其字节序已设置。
   *
   * @param buffer 要写入的ByteBuffer
   * @param xid    事务ID
   */
  public static void writeAcceptedSuccessReplyHeader(ByteBuffer buffer, int xid) {
    // 确保字节序 (如果调用者尚未设置，可以在这里设置，但通常由外部控制)
     if (buffer.order() != ByteOrder.BIG_ENDIAN) {
         buffer.order(ByteOrder.BIG_ENDIAN);
     }

    buffer.putInt(xid);                                  // 事务ID
    buffer.putInt(RpcConstants.MSG_TYPE_REPLY);          // 消息类型: 回复 (1)
    buffer.putInt(RpcConstants.REPLY_STAT_MSG_ACCEPTED); // 回复状态: 接受 (0)
    buffer.putInt(RpcConstants.VERF_FLAVOR_AUTH_NONE);   // 认证机制: 无 (0)
    buffer.putInt(RpcConstants.VERF_LENGTH_ZERO);        // 认证数据长度: 0
    buffer.putInt(RpcConstants.ACCEPT_STAT_SUCCESS);     // 接受状态: 成功 (0)
  }

  public static Flowable<Buffer> writeAcceptedSuccessReplyHeader(int xid) {
    // 确保字节序 (如果调用者尚未设置，可以在这里设置，但通常由外部控制)
    int rpcHeaderLength = RpcConstants.RPC_ACCEPTED_REPLY_HEADER_LENGTH;
    Buffer rpcHeaderBuffer = Buffer.buffer(rpcHeaderLength);

    rpcHeaderBuffer.appendInt(xid);                                  // 事务ID
    rpcHeaderBuffer.appendInt(RpcConstants.MSG_TYPE_REPLY);          // 消息类型: 回复 (1)
    rpcHeaderBuffer.appendInt(RpcConstants.REPLY_STAT_MSG_ACCEPTED); // 回复状态: 接受 (0)
    rpcHeaderBuffer.appendInt(RpcConstants.VERF_FLAVOR_AUTH_NONE);   // 认证机制: 无 (0)
    rpcHeaderBuffer.appendInt(RpcConstants.VERF_LENGTH_ZERO);        // 认证数据长度: 0
    rpcHeaderBuffer.appendInt(RpcConstants.ACCEPT_STAT_SUCCESS);     // 接受状态: 成功 (0)

    return Flowable.just(rpcHeaderBuffer);
  }

  /**
   * 创建并返回一个包含标准RPC成功响应头部的ByteBuffer。
   *
   * @param xid 事务ID
   * @return 一个新的ByteBuffer，包含了头部数据，并已准备好读取 (flipped)
   */
  public static ByteBuffer createAcceptedSuccessReplyHeaderBuffer(int xid) {
    ByteBuffer headerBuffer = ByteBuffer.allocate(RpcConstants.RPC_ACCEPTED_REPLY_HEADER_LENGTH);
    headerBuffer.order(ByteOrder.BIG_ENDIAN);
    writeAcceptedSuccessReplyHeader(headerBuffer, xid);
    headerBuffer.flip(); // 准备好被读取或发送
    return headerBuffer;
  }
}
