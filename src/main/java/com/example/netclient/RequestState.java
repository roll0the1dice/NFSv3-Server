package com.example.netclient;

import com.example.netclient.utils.ByteArrayKeyWrapper;
import io.vertx.core.buffer.Buffer;
import io.reactivex.subjects.PublishSubject;
import io.reactivex.subjects.Subject;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.disposables.Disposable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RequestState {
  private static final Logger log = LoggerFactory.getLogger(RequestState.class);
  private static final int CACHE_THRESHOLD = 1024 * 1024; // 1MB
  private static final int EMITTER_THRESHOLD = 5 * 1024 * 1024; // 5MB
  private static final long INACTIVITY_TIMEOUT = 5000; // 5 seconds

  private static final ConcurrentHashMap<ByteArrayKeyWrapper, List<Buffer>> requestCache = new ConcurrentHashMap<>();
  private static final ConcurrentHashMap<ByteArrayKeyWrapper, Subject<Buffer>> requestSubjects = new ConcurrentHashMap<>();
  private static final ConcurrentHashMap<ByteArrayKeyWrapper, Disposable> timeoutDisposables = new ConcurrentHashMap<>();

  public static Single<Buffer> addData(ByteArrayKeyWrapper byteKeyWrapper, Buffer data) {
    return Single.create(singleEmitter -> {

      List<Buffer> currentDataList = requestCache.compute(byteKeyWrapper, (key, existingList) -> {
        if (existingList == null) {
          existingList = new ArrayList<>(); // Consider Collections.synchronizedList or CopyOnWriteArrayList if accessed outside compute
        }
        existingList.add(data);
        return existingList;
      });

      // 2. Calculate total size and combine buffer
      Buffer combinedBuffer = Buffer.buffer();
      int totalSize = 0;
      // It's safer to operate on a copy if currentDataList can be modified elsewhere concurrently,
      // or ensure all modifications are synchronized/atomic.
      // For compute, the list reference is updated atomically.
      synchronized (currentDataList) { // Synchronize if ArrayList is used and modified by timeout thread later
        for (Buffer buf : currentDataList) {
          combinedBuffer.appendBuffer(buf);
          totalSize += buf.length();
        }
      }

      // 3. Check threshold
      if (totalSize >= EMITTER_THRESHOLD) {
        requestCache.remove(byteKeyWrapper);
        // Cancel and remove previous timeout
        Disposable oldTimeout = timeoutDisposables.remove(byteKeyWrapper);
        if (oldTimeout != null && !oldTimeout.isDisposed()) {
          oldTimeout.dispose();
        }
        // No longer need a subject per xid for this Single model
        // Subject<Buffer> subject = requestSubjects.remove(xid);
        // if (subject != null) {
        //     subject.onComplete(); // Clean up old subject
        // }
        if (!singleEmitter.isDisposed()) {
          singleEmitter.onSuccess(combinedBuffer);
        }
        return; // Emission successful, no timeout needed for this specific Single
      }

      // 4. Setup/Reset Timeout if threshold not met
      Disposable oldTimeout = timeoutDisposables.remove(byteKeyWrapper);
      if (oldTimeout != null && !oldTimeout.isDisposed()) {
        oldTimeout.dispose();
      }

      Disposable newTimeoutDisposable = Observable.timer(INACTIVITY_TIMEOUT, TimeUnit.MILLISECONDS)
        .subscribe(
          tick -> {
            if (!singleEmitter.isDisposed()) {
              // On timeout, decide what to do.
              // Option 1: Emit current data
//              List<Buffer> dataOnTimeout = requestCache.remove(byteKeyWrapper);
//              timeoutDisposables.remove(byteKeyWrapper); // remove self
              // requestSubjects.remove(xid); // if subjects map is still used

//              if (dataOnTimeout != null && !dataOnTimeout.isEmpty()) {
//                Buffer finalBuffer = Buffer.buffer();
//                for (Buffer buf : dataOnTimeout) {
//                  finalBuffer.appendBuffer(buf);
//                }
//                singleEmitter.onSuccess(finalBuffer);
//              } else {
                // Option 2: Signal timeout error or complete empty
                singleEmitter.onError(new TimeoutException("Inactivity timeout for xid: "));
                // or emitter.onComplete(); if empty completion is valid
              //}
            }
          },
          error -> {
            if (!singleEmitter.isDisposed()) {
              log.error("Timeout mechanism error for xid", error);
              singleEmitter.onError(error); // Propagate the error
            }
            // Cleanup in case of error during timeout setup/observation
            timeoutDisposables.remove(byteKeyWrapper);
            // requestSubjects.remove(xid);
          }
        );

    });
  }

  public static Single<Buffer> getAndProcessData(ByteArrayKeyWrapper byteArrayKeyWrapper) {
    return Single.defer(() -> {
      List<Buffer> dataList = requestCache.get(byteArrayKeyWrapper);
      if (dataList != null) {
        int totalSize = dataList.stream()
          .mapToInt(Buffer::length)
          .sum();

        if (totalSize >= EMITTER_THRESHOLD) {
          // 合并所有Buffer
          Buffer combinedBuffer = Buffer.buffer();
          for (Buffer buf : dataList) {
            combinedBuffer.appendBuffer(buf);
          }

          cleanup(byteArrayKeyWrapper);
          return Single.just(combinedBuffer);
        }
      }
      return Single.error(new IllegalStateException("Data not ready for processing"));
    });
  }

  public static boolean shouldProcess(int xid) {
    List<Buffer> dataList = requestCache.get(xid);
    if (dataList != null) {
      int totalSize = dataList.stream()
        .mapToInt(Buffer::length)
        .sum();
      return totalSize >= EMITTER_THRESHOLD;
    }
    return false;
  }

  public static boolean shouldCache(ByteArrayKeyWrapper byteArrayKeyWrapper) {
    List<Buffer> dataList = requestCache.get(byteArrayKeyWrapper);
    if (dataList != null) {
      int totalSize = dataList.stream()
        .mapToInt(Buffer::length)
        .sum();
      return totalSize < CACHE_THRESHOLD;
    }
    return true;
  }

  public static Observable<Buffer> observeData(ByteArrayKeyWrapper byteArrayKeyWrapper) {
    return Observable.defer(() -> {
      Subject<Buffer> subject = requestSubjects.computeIfAbsent(byteArrayKeyWrapper, k -> PublishSubject.create());
      List<Buffer> dataList = requestCache.get(byteArrayKeyWrapper);
      if (dataList != null) {
        // 合并所有Buffer
        Buffer combinedBuffer = Buffer.buffer();
        for (Buffer buf : dataList) {
          combinedBuffer.appendBuffer(buf);
        }
        return subject.startWith(combinedBuffer);
      }
      return subject;
    });
  }

  public static Observable<Buffer> observeDataWithTimeout(ByteArrayKeyWrapper byteArrayKeyWrapper, long timeout, TimeUnit unit) {
    return observeData(byteArrayKeyWrapper)
      .timeout(timeout, unit)
      .doOnComplete(() -> cleanup(byteArrayKeyWrapper))
      .doOnError(e -> cleanup(byteArrayKeyWrapper));
  }

  private static void cleanup(ByteArrayKeyWrapper byteArrayKeyWrapper) {
    // 取消超时计时器
    Disposable timeoutDisposable = timeoutDisposables.remove(byteArrayKeyWrapper);
    if (timeoutDisposable != null && !timeoutDisposable.isDisposed()) {
      timeoutDisposable.dispose();
    }

    requestCache.remove(byteArrayKeyWrapper);
    Subject<Buffer> subject = requestSubjects.remove(byteArrayKeyWrapper);
    if (subject != null) {
      subject.onComplete();
    }
  }

  public static void clearCache(ByteArrayKeyWrapper byteArrayKeyWrapper) {
    cleanup(byteArrayKeyWrapper);
  }
}
