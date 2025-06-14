package com.example.netclient.utils;

import com.example.netclient.enums.RpcConstants;
import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;
import io.vertx.core.buffer.Buffer;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

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

  /**
   * A FlowableTransformer that buffers Vert.x Buffer objects until the accumulated size
   * reaches maxSize, then emits the aggregated Buffer.
   *
   * @param maxSize The maximum size in bytes for the buffer before emitting.
   * @return A FlowableTransformer.
   */
  public static FlowableTransformer<Buffer, Buffer> bufferBySize(final int maxSize) {
    return upstream -> new Flowable<Buffer>() {
      @Override
      protected void subscribeActual(Subscriber<? super Buffer> downstream) {
        upstream.subscribe(new Subscriber<Buffer>() {
          private Subscription subscription;
          private Buffer currentAccumulatedBuffer = Buffer.buffer(); // Use Vert.x Buffer for accumulation
          private volatile boolean done = false;

          @Override
          public void onSubscribe(Subscription s) {
            this.subscription = s;
            downstream.onSubscribe(new Subscription() {
              @Override
              public void request(long n) {
                // Simple propagation. Can be optimized if downstream requests
                // less than potential buffered items that could be emitted.
                s.request(n);
              }

              @Override
              public void cancel() {
                s.cancel();
                done = true;
                currentAccumulatedBuffer = Buffer.buffer(); // Clear buffer on cancellation
              }
            });
          }

          @Override
          public void onNext(Buffer incomingBuffer) {
            if (done) return;

            currentAccumulatedBuffer.appendBuffer(incomingBuffer);

            // Emit chunks as long as the buffer is larger than maxSize
            // This handles cases where a single incomingBuffer might be much larger than maxSize
            // or multiple small buffers push it over.
            while (currentAccumulatedBuffer.length() >= maxSize) {
              Buffer partToEmit;
              // If the current buffer is exactly maxSize or slightly larger but we want to emit only maxSize
              // we need to be careful. For simplicity, we emit what we have if it's >= maxSize.
              // A more precise implementation might slice the buffer.
              // However, for S3 parts, it's often okay if a part is slightly larger than the minimum.
              // If strict slicing is needed:
              if (currentAccumulatedBuffer.length() > maxSize) {
                partToEmit = currentAccumulatedBuffer.getBuffer(0, maxSize);
                Buffer remaining = currentAccumulatedBuffer.getBuffer(maxSize, currentAccumulatedBuffer.length());
                currentAccumulatedBuffer = remaining;
              } else { // Exactly maxSize
                partToEmit = currentAccumulatedBuffer;
                currentAccumulatedBuffer = Buffer.buffer(); // Reset
              }
              downstream.onNext(partToEmit);

              // If after emitting a part, the remaining buffer is empty, break the loop
              if (currentAccumulatedBuffer.length() == 0) {
                break;
              }
            }
            // Request more if needed by the specific backpressure strategy.
            // Often handled by the initial/ongoing requests from downstream.
          }

          @Override
          public void onError(Throwable t) {
            if (done) return;
            done = true;
            currentAccumulatedBuffer = Buffer.buffer(); // Clear buffer
            downstream.onError(t);
          }

          @Override
          public void onComplete() {
            if (done) return;
            done = true;
            if (currentAccumulatedBuffer.length() > 0) {
              downstream.onNext(currentAccumulatedBuffer); // Emit any remaining data
            }
            currentAccumulatedBuffer = Buffer.buffer(); // Clear buffer
            downstream.onComplete();
          }
        });
      }
    };
  }


}
