package com.example.netclient;

import io.reactivex.Observable;
import io.reactivex.disposables.Disposable;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.subjects.PublishSubject;
import io.reactivex.subjects.Subject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

class MyDataItem {
  int id;
  String value;

  public MyDataItem(int id, String value) {
    this.id = id;
    this.value = value;
  }

  @Override
  public String toString() {
    return "MyDataItem{" +
      "id=" + id +
      ", value='" + value + '\'' +
      '}';
  }
}

class ThresholdBufferCache<T> {
  private final List<T> buffer = new ArrayList<>();
  private final int threshold;
  private final Subject<List<T>> outputSubject = PublishSubject.<List<T>>create().toSerialized(); // toSerialized for thread-safety
  private final Object lock = new Object(); // For synchronizing access to buffer

  public ThresholdBufferCache(int threshold) {
    if (threshold <= 0) {
      throw new IllegalArgumentException("Threshold must be a positive number.");
    }
    this.threshold = threshold;
    System.out.println(Thread.currentThread().getName() + " - ThresholdBufferCache initialized with threshold: " + threshold);
  }

  /**
   * Adds an item to the buffer.
   * If the buffer reaches the threshold, it emits the buffered items (as a new list)
   * and clears the buffer. This method is thread-safe.
   *
   * @param item The item to add.
   */
  public void add(T item) {
    List<T> batchToEmit = null;

    synchronized (lock) {
      if (outputSubject.hasComplete() || outputSubject.hasThrowable()) {
        System.out.println(Thread.currentThread().getName() + " - Cache is completed or has error, cannot add item: " + item);
        return;
      }

      buffer.add(item);
      System.out.println(Thread.currentThread().getName() + " - Item added. Buffer size: " + buffer.size() + "/" + threshold + " - " + item);

      if (buffer.size() >= threshold) {
        System.out.println(Thread.currentThread().getName() + " - %cThreshold reached! Preparing to emit " + buffer.size() + " items." + "color: green");
        batchToEmit = new ArrayList<>(buffer); // Create a copy to emit
        buffer.clear(); // Clear the internal buffer
        System.out.println(Thread.currentThread().getName() + " - Buffer cleared.");
      }
    }

    if (batchToEmit != null) {
      outputSubject.onNext(Collections.unmodifiableList(batchToEmit)); // Emit an unmodifiable copy
    }
  }

  /**
   * Observable stream that emits a list of items when the buffer threshold is reached.
   *
   * @return Observable<List<T>>
   */
  public Observable<List<T>> getBufferedStream() {
    return outputSubject.hide(); // Hide subject type to prevent casting and misuse
  }

  /**
   * Manually flushes the current buffer, regardless of the threshold.
   * Emits the current buffer contents if not empty. This method is thread-safe.
   */
  public void flush() {
    List<T> batchToEmit = null;

    synchronized (lock) {
      if (outputSubject.hasComplete() || outputSubject.hasThrowable()) {
        System.out.println(Thread.currentThread().getName() + " - Cache is completed or has error, cannot flush.");
        return;
      }

      if (!buffer.isEmpty()) {
        System.out.println(Thread.currentThread().getName() + " - %cManual flush! Preparing to emit " + buffer.size() + " items." + "color: orange");
        batchToEmit = new ArrayList<>(buffer);
        buffer.clear();
        System.out.println(Thread.currentThread().getName() + " - Buffer cleared after flush.");
      } else {
        System.out.println(Thread.currentThread().getName() + " - Manual flush: Buffer is empty, nothing to emit.");
      }
    }

    if (batchToEmit != null) {
      outputSubject.onNext(Collections.unmodifiableList(batchToEmit));
    }
  }

  /**
   * Gets a copy of the current items in the buffer without emitting or clearing.
   * Useful for inspection. This method is thread-safe.
   *
   * @return A new list containing current buffer items.
   */
  public List<T> getCurrentBuffer() {
    synchronized (lock) {
      return new ArrayList<>(buffer); // Return a copy
    }
  }

  /**
   * Gets the current size of the buffer. This method is thread-safe.
   *
   * @return int current buffer size.
   */
  public int getCurrentBufferSize() {
    synchronized (lock) {
      return buffer.size();
    }
  }

  /**
   * Completes the output stream. Any remaining items in the buffer are flushed first.
   * No more items will be emitted after this. This method is thread-safe.
   */
  public void complete() {
    synchronized (lock) { // Ensure complete happens after any pending add/flush
      if (outputSubject.hasComplete() || outputSubject.hasThrowable()) {
        return; // Already completed or errored
      }
      System.out.println(Thread.currentThread().getName() + " - Completing ThresholdBufferCache output stream...");
    }
    flush(); // Emit any remaining items
    outputSubject.onComplete();
    System.out.println(Thread.currentThread().getName() + " - ThresholdBufferCache output stream completed.");
  }

  /**
   * Signals an error on the output stream. The buffer is cleared.
   * This method is thread-safe.
   *
   * @param error The error to signal.
   */
  public void error(Throwable error) {
    synchronized (lock) { // Ensure error happens after any pending add/flush
      if (outputSubject.hasComplete() || outputSubject.hasThrowable()) {
        return; // Already completed or errored
      }
      System.err.println(Thread.currentThread().getName() + " - ThresholdBufferCache output stream errored.");
      buffer.clear(); // Clear buffer on error
    }
    outputSubject.onError(error);
  }

  /**
   * Checks if the cache output stream has been completed.
   * @return true if completed, false otherwise.
   */
  public boolean isComplete() {
    return outputSubject.hasComplete();
  }

  /**
   * Checks if the cache output stream has an error.
   * @return true if has an error, false otherwise.
   */
  public boolean hasError() {
    return outputSubject.hasThrowable();
  }
}


public class RxJavaThresholdCacheDemo {
  public static void main(String[] args) throws InterruptedException {
    System.out.println("--- RxJava Threshold Buffer Cache Demo ---");
    final int CACHE_THRESHOLD = 3;
    ThresholdBufferCache<MyDataItem> myCache = new ThresholdBufferCache<>(CACHE_THRESHOLD);

    // Subscribe to the buffered stream on a separate thread for observation
    Disposable subscription = myCache.getBufferedStream()
      .observeOn(Schedulers.computation()) // Observe on a different thread
      .subscribe(
        itemsBatch -> System.out.println(Thread.currentThread().getName() +
          " - %cReceived Batch (" + itemsBatch.size() + " items): " + itemsBatch),
        error -> System.err.println(Thread.currentThread().getName() + " - Error in subscription: " + error.getMessage()),
        () -> System.out.println(Thread.currentThread().getName() + " - Subscription completed.")
      );

    System.out.println(Thread.currentThread().getName() + " - Adding items...");
    myCache.add(new MyDataItem(1, "Alpha"));
    System.out.println(Thread.currentThread().getName() + " - Current buffer size: " + myCache.getCurrentBufferSize());
    System.out.println(Thread.currentThread().getName() + " - Current buffer content: " + myCache.getCurrentBuffer());

    myCache.add(new MyDataItem(2, "Bravo"));
    System.out.println(Thread.currentThread().getName() + " - Current buffer size: " + myCache.getCurrentBufferSize());

    // At this point, no batch should have been emitted yet
    System.out.println(Thread.currentThread().getName() + "\n--- Adding one more to reach threshold ---");
    myCache.add(new MyDataItem(3, "Charlie")); // Reaches threshold, should emit [Alpha, Bravo, Charlie]

    Thread.sleep(100); // Give some time for async emission and processing

    System.out.println(Thread.currentThread().getName() + "\nAdding more items...");
    myCache.add(new MyDataItem(4, "Delta"));
    myCache.add(new MyDataItem(5, "Echo"));

    Thread.sleep(100);

    System.out.println(Thread.currentThread().getName() + "\n--- Manually flushing remaining items ---");
    System.out.println(Thread.currentThread().getName() + " - Buffer before flush: " + myCache.getCurrentBuffer());
    myCache.flush(); // Manually emit [Delta, Echo]
    System.out.println(Thread.currentThread().getName() + " - Buffer after flush: " + myCache.getCurrentBuffer());

    Thread.sleep(100);

    System.out.println(Thread.currentThread().getName() + "\nAdding one more item after flush...");
    myCache.add(new MyDataItem(6, "Foxtrot"));
    System.out.println(Thread.currentThread().getName() + " - Current buffer: " + myCache.getCurrentBuffer());

    Thread.sleep(100);

    System.out.println(Thread.currentThread().getName() + "\n--- Completing the cache ---");
    // myCache.add(new MyDataItem(7, "Golf")); // Add one more before completing
    myCache.complete(); // Will flush [Foxtrot] (or [Foxtrot, Golf] if uncommented) then complete stream

    Thread.sleep(100);

    // Try adding after completion
    System.out.println(Thread.currentThread().getName() + "\n--- Attempting to add after completion ---");
    myCache.add(new MyDataItem(8, "Hotel")); // Should not be added or emitted

    System.out.println(Thread.currentThread().getName() + " - Is cache complete? " + myCache.isComplete());

    // Simulate multi-threaded additions
    System.out.println(Thread.currentThread().getName() + "\n--- Multi-threaded additions demo ---");
    ThresholdBufferCache<Integer> concurrentCache = new ThresholdBufferCache<>(5);
    Disposable concurrentSub = concurrentCache.getBufferedStream()
      .observeOn(Schedulers.newThread())
      .subscribe(batch -> System.out.println(Thread.currentThread().getName() + " - Concurrent Batch: " + batch));

    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < 12; i++) {
      final int itemToAdd = i;
      Thread t = new Thread(() -> {
        try {
          Thread.sleep((long)(Math.random() * 50)); // Random delay
        } catch (InterruptedException e) {}
        concurrentCache.add(itemToAdd);
      }, "AdderThread-" + i);
      threads.add(t);
      t.start();
    }

    for (Thread t : threads) {
      t.join(); // Wait for all adder threads to finish
    }
    concurrentCache.complete(); // Flush any remaining and complete

    Thread.sleep(500); // Wait for async operations to finish before exiting main

    subscription.dispose();
    concurrentSub.dispose();
    System.out.println(Thread.currentThread().getName() + " - Demo finished.");
  }
}
