package com.example.netclient;

import io.reactivex.Observable;
import io.reactivex.Scheduler;
import io.reactivex.Single;
import io.reactivex.disposables.Disposable;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.subjects.BehaviorSubject;
import io.reactivex.subjects.ReplaySubject;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

class DataModel {
  String data;
  long timestamp;

  public DataModel(String data) {
    this.data = data;
    this.timestamp = System.currentTimeMillis();
  }

  @Override
  public String toString() {
    return "DataModel{" +
      "data='" + data + '\'' +
      ", fetchedAt=" + timestamp +
      '}';
  }
}

class DataService {

  public static final long CACHE_TTL_MS = 5000; // 5 seconds Time-To-Live

  // --- BehaviorSubject Cache ---
  // Using Optional to explicitly handle the "no value yet" or "cleared" state.
  private final BehaviorSubject<Optional<DataModel>> behaviorCache = BehaviorSubject.createDefault(Optional.empty());
  private final AtomicBoolean behaviorIsLoading = new AtomicBoolean(false);
  private final AtomicLong behaviorLastFetchTime = new AtomicLong(0);

  // --- ReplaySubject Cache (caches last 1 item) ---
  private ReplaySubject<DataModel> replayCache = ReplaySubject.createWithSize(1); // Store last 1 item
  private final AtomicBoolean replayIsLoading = new AtomicBoolean(false);
  private final AtomicLong replayLastFetchTime = new AtomicLong(0);
  private final AtomicBoolean replayHasValue = new AtomicBoolean(false); // ReplaySubject doesn't have a simple "getValue"

  // Simulate API call
  private Single<DataModel> fetchDataFromApi(String source) {
    return Single.fromCallable(() -> {
        System.out.println(Thread.currentThread().getName() + " - %cFetching data from API for " + source + "...");
        Thread.sleep(1000); // Simulate network delay
        return new DataModel("Data for " + source + " fetched at " + System.currentTimeMillis());
      })
      .subscribeOn(Schedulers.io()) // Perform network call on IO thread
      .doOnSuccess(data -> System.out.println(Thread.currentThread().getName() + " - API (" + source + ") response received: " + data))
      .doOnError(err -> System.err.println(Thread.currentThread().getName() + " - API (" + source + ") Error: " + err.getMessage()));
  }


  public Observable<DataModel> getDataWithBehaviorSubject() {
    return Observable.defer(() -> { // Defer ensures this logic runs for each subscriber
      Optional<DataModel> cachedValue = behaviorCache.getValue(); // Can be null if not using createDefault or if terminated
      long now = System.currentTimeMillis();

      if (cachedValue != null && cachedValue.isPresent() && (now - behaviorLastFetchTime.get() < CACHE_TTL_MS)) {
        System.out.println(Thread.currentThread().getName() + " - BehaviorSubject: Returning data from cache");
        return Observable.just(cachedValue.get());
      }

      // compareAndSet to ensure only one thread initiates loading
      if (behaviorIsLoading.compareAndSet(false, true)) {
        System.out.println(Thread.currentThread().getName() + " - BehaviorSubject: Cache invalid or empty, fetching new data...");
        // 获取数据 dataModel
        return fetchDataFromApi("BehaviorSubject")
          .toObservable() // Convert Single to Observable
          .doOnNext(data -> {
            behaviorLastFetchTime.set(System.currentTimeMillis());
            behaviorCache.onNext(Optional.of(data)); // Update cache
          })
          .doOnError(error -> {
            // Optionally update cache with an error state or clear it
            // behaviorCache.onNext(Optional.empty()); // Clear on error
            System.err.println(Thread.currentThread().getName() + " - BehaviorSubject: Error fetching data, cache not updated with new value.");
          })
          .doFinally(() -> behaviorIsLoading.set(false)); // Release lock
      } else {
        System.out.println(Thread.currentThread().getName() + " - BehaviorSubject: Request already in progress, subscribing to cache updates.");
        // Wait for the loading to complete by filtering out empty/stale values
        return behaviorCache.hide() // Hide subject type
          .filter(Optional::isPresent)
          .map(Optional::get)
          .filter(data -> data.timestamp >= behaviorLastFetchTime.get()) // Ensure it's fresh if a fetch just happened
          .firstElement() // Take the first valid emission (which will be the new data)
          .toObservable();
      }
    });
  }

  public void clearBehaviorCache() {
    System.out.println(Thread.currentThread().getName() + " - Clearing BehaviorSubject cache");
    behaviorCache.onNext(Optional.empty());
    behaviorLastFetchTime.set(0); // Invalidate TTL
  }


  public Observable<DataModel> getDataWithReplaySubject() {
    return Observable.defer(() -> {
      long now = System.currentTimeMillis();

      // ReplaySubject replays, so if it has a value and it's fresh, subsequent subscribers get it.
      // The main check is for TTL.
      if (replayHasValue.get() && (now - replayLastFetchTime.get() < CACHE_TTL_MS)) {
        System.out.println(Thread.currentThread().getName() + " - ReplaySubject: Returning data from cache via replay");
        return replayCache.hide(); // Just return the subject, it will replay
      }

      if (replayIsLoading.compareAndSet(false, true)) {
        System.out.println(Thread.currentThread().getName() + " - ReplaySubject: Cache invalid, empty or stale, fetching new data...");
        // If cache is stale, we might want to ensure old values are not replayed
        // by creating a new ReplaySubject or by ensuring subscribers only take 1 value
        // For this example, new onNext will be replayed.
        return fetchDataFromApi("ReplaySubject")
          .toObservable()
          .doOnNext(data -> {
            replayLastFetchTime.set(System.currentTimeMillis());
            replayHasValue.set(true);
            replayCache.onNext(data); // Update cache, will be replayed to current/future subscribers of THIS chain
          })
          .doOnError(error -> System.err.println(Thread.currentThread().getName() + " - ReplaySubject: Error fetching data."))
          .doFinally(() -> replayIsLoading.set(false));
      } else {
        System.out.println(Thread.currentThread().getName() + " - ReplaySubject: Request already in progress, subscribing to cache updates.");
        // Wait for the loading to complete.
        // Since ReplaySubject replays, if a fetch is in progress, new subscribers
        // will get the value once the fetch completes and onNext is called.
        return replayCache.hide()
          .filter(data -> data.timestamp >= replayLastFetchTime.get()) // Ensure value is fresh if new fetch just completed
          .firstElement() // Take first valid emission
          .toObservable();
      }
    });
  }

  public void clearReplayCache() {
    System.out.println(Thread.currentThread().getName() + " - Clearing ReplaySubject cache");
    // For ReplaySubject, to truly clear so it doesn't replay old values,
    // re-initialize it. Or, if it's bounded, it will eventually push out old values.
    // If you just want to mark it as "stale" for the TTL logic:
    replayLastFetchTime.set(0);
    replayHasValue.set(false);
    // To prevent any replay of old data from this instance:
    this.replayCache = ReplaySubject.createWithSize(1); // New instance
  }
}

public class RxJavaCacheDemo {
  public static void main(String[] args) throws InterruptedException {
    DataService service = new DataService();
    Scheduler mainThread = Schedulers.single(); // Simulate a main/UI thread for observing

    System.out.println("--- Testing BehaviorSubject Cache ---");
    // 发射默认值
    Disposable subB1 = service.getDataWithBehaviorSubject()
      .observeOn(mainThread)
      .subscribe(
        data -> System.out.println("SubB1 (Behavior): " + data),
        Throwable::printStackTrace,
        () -> System.out.println("SubB1 (Behavior) Complete")
      );

    Thread.sleep(500); // Give time for first request to start

    Disposable subB2 = service.getDataWithBehaviorSubject() // Should use cache or wait for ongoing
      .observeOn(mainThread)
      .subscribe(
        data -> System.out.println("SubB2 (Behavior) after 0.5s: " + data),
        Throwable::printStackTrace
      );

    Thread.sleep(2000); // First request should be complete
    System.out.println("\n--- BehaviorSubject: After 2s (should use cache) ---");
    Disposable subB3 = service.getDataWithBehaviorSubject()
      .observeOn(mainThread)
      .subscribe(
        data -> System.out.println("SubB3 (Behavior): " + data),
        Throwable::printStackTrace
      );

    Thread.sleep(DataService.CACHE_TTL_MS + 1000); // Wait for cache to expire
    System.out.println("\n--- BehaviorSubject: After cache TTL (should refetch) ---");
    Disposable subB4 = service.getDataWithBehaviorSubject()
      .observeOn(mainThread)
      .subscribe(
        data -> System.out.println("SubB4 (Behavior): " + data),
        Throwable::printStackTrace
      );
    Thread.sleep(1500);
    service.clearBehaviorCache();
    System.out.println("\n--- BehaviorSubject: After manual clear (should refetch) ---");
    Disposable subB5 = service.getDataWithBehaviorSubject()
      .observeOn(mainThread)
      .subscribe(
        data -> System.out.println("SubB5 (Behavior): " + data),
        Throwable::printStackTrace
      );

    Thread.sleep(2000);
    subB1.dispose();
    subB2.dispose();
    subB3.dispose();
    subB4.dispose();
    subB5.dispose();

    System.out.println("\n\n--- Testing ReplaySubject Cache ---");
    Disposable subR1 = service.getDataWithReplaySubject()
      .observeOn(mainThread)
      .subscribe(
        data -> System.out.println("SubR1 (Replay): " + data),
        Throwable::printStackTrace,
        () -> System.out.println("SubR1 (Replay) Complete")
      );

    Thread.sleep(500);

    Disposable subR2 = service.getDataWithReplaySubject() // Should use replayed value or wait
      .observeOn(mainThread)
      .subscribe(
        data -> System.out.println("SubR2 (Replay) after 0.5s: " + data),
        Throwable::printStackTrace
      );

    Thread.sleep(2000);
    System.out.println("\n--- ReplaySubject: After 2s (should use replayed cache) ---");
    Disposable subR3 = service.getDataWithReplaySubject()
      .observeOn(mainThread)
      .subscribe(
        data -> System.out.println("SubR3 (Replay): " + data),
        Throwable::printStackTrace
      );

    Thread.sleep(DataService.CACHE_TTL_MS + 1000);
    System.out.println("\n--- ReplaySubject: After cache TTL (should refetch) ---");
    Disposable subR4 = service.getDataWithReplaySubject()
      .observeOn(mainThread)
      .subscribe(
        data -> System.out.println("SubR4 (Replay): " + data),
        Throwable::printStackTrace
      );
    Thread.sleep(1500);
    service.clearReplayCache();
    System.out.println("\n--- ReplaySubject: After manual clear (should refetch) ---");
    Disposable subR5 = service.getDataWithReplaySubject()
      .observeOn(mainThread)
      .subscribe(
        data -> System.out.println("SubR5 (Replay): " + data),
        Throwable::printStackTrace
      );

    Thread.sleep(5000); // Let all operations finish
    subR1.dispose();
    subR2.dispose();
    subR3.dispose();
    subR4.dispose();
    subR5.dispose();

    System.out.println("Demo finished.");
  }
}
