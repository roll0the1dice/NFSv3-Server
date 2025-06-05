package com.example.netclient;

import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;
import io.reactivex.schedulers.Schedulers;
import io.vertx.core.buffer.Buffer; // Vert.x Buffer
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.TimeUnit;

public class FlowableVertxBufferBySize {

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

  // --- 示例使用 ---
  public static void main(String[] args) throws InterruptedException {
    final int PART_SIZE = 10; // S3 Part size (example, usually 5MB+)

    // 模拟NFS WRITE calls (数据块)
    Flowable<Buffer> nfsWriteCalls = Flowable.create(emitter -> {
      System.out.println("Emitter: Emitting 3 bytes");
      emitter.onNext(Buffer.buffer(new byte[]{1, 2, 3}));        // 3 bytes
      Thread.sleep(100);
      System.out.println("Emitter: Emitting 4 bytes");
      emitter.onNext(Buffer.buffer(new byte[]{4, 5, 6, 7}));    // 4 bytes (total 7)
      Thread.sleep(100);
      System.out.println("Emitter: Emitting 4 bytes (will trigger flush)");
      emitter.onNext(Buffer.buffer(new byte[]{8, 9, 10, 11}));  // 4 bytes (total 11 -> emit [1..10], remaining [11])
      Thread.sleep(100);
      System.out.println("Emitter: Emitting 2 bytes");
      emitter.onNext(Buffer.buffer(new byte[]{12, 13}));       // 2 bytes (total 3 with [11] -> [11,12,13])
      Thread.sleep(100);
      System.out.println("Emitter: Emitting 8 bytes (will trigger flush)");
      emitter.onNext(Buffer.buffer(new byte[]{14,15,16,17,18,19,20,21})); // 8 bytes (total 11 with [11,12,13] -> emit [11..20], remaining [21])
      Thread.sleep(100);
      System.out.println("Emitter: Emitting 1 byte");
      emitter.onNext(Buffer.buffer(new byte[]{22}));           // 1 byte (total 2 with [21] -> [21,22])
      System.out.println("Emitter: Completing");
      emitter.onComplete();
    }, io.reactivex.BackpressureStrategy.BUFFER);

    System.out.println("Subscribing to buffered S3 parts (Vert.x Buffer)...");

    nfsWriteCalls
      .subscribeOn(Schedulers.computation()) // Simulate NFS calls on a different thread
      .compose(bufferBySize(PART_SIZE))    // 应用我们的自定义转换器
      .observeOn(Schedulers.single())        // Observe results on a single thread for ordered printing
      .doOnNext(s3Part -> {
        System.out.println(Thread.currentThread().getName() +
          " - Uploading S3 Part (Vert.x Buffer): " + s3Part.getBytes().length + " bytes -> " + s3Part.toString());
        // 在这里执行实际的 S3 UploadPart 操作
        // InputStream inputStream = new BufferInputStream(s3Part);
        // s3Client.uploadPart(..., inputStream, s3Part.length(), ...);
        try {
          Thread.sleep(50); // 模拟上传延迟
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      })
      .doOnComplete(() -> System.out.println(Thread.currentThread().getName() + " - All S3 parts uploaded and MultipartUpload completed."))
      .doOnError(throwable -> System.err.println(Thread.currentThread().getName() + " - Error during S3 part processing: " + throwable))
      .blockingSubscribe(); // For main thread to wait in this example

    System.out.println("Main thread finished.");
  }
}
