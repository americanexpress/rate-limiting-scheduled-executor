/*
 * Copyright 2020 American Express Travel Related Services Company, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.americanexpress.ratelimitedscheduler.performance;

import com.americanexpress.ratelimitedscheduler.RateLimitedScheduledExecutor;
import com.americanexpress.ratelimitedscheduler.RateLimitedScheduledExecutorManager;
import com.americanexpress.ratelimitedscheduler.exceptions.RateLimitedScheduledExecutorManagerHasAlreadyStartedException;

import org.junit.Ignore;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.assertTrue;

public class SchedulerPerformanceTest {
  // this should be 20_000_000 to start to demonstrate performance advantage. it's set low to avoid
  // heap space issues on jenkins
  private static final int NUMBER_OF_TASKS = 20_000_000;
  private static final int MAX_FUTURE_MILLIS = 5_000;
  private static final int NUMBER_OF_THREADS = 16;
  private final Random random = new Random();
  private CountDownLatch countDownLatch;

  @SuppressWarnings({"squid:S1607", "squid:S2925"})
  @Test
  @Ignore
  public void testPerformanceOnNewSchedulerDefaultSettings() throws InterruptedException {
    countDownLatch = new CountDownLatch(NUMBER_OF_TASKS);
    RateLimitedScheduledExecutorManager manager = new RateLimitedScheduledExecutorManager();
    RateLimitedScheduledExecutor executorA = manager.getRateLimitedScheduledExecutor("serviceA");
    // allow time for startup
    Thread.sleep(5000);
    CountDownLatch threadCountDown = new CountDownLatch(NUMBER_OF_THREADS);
    int tasksPerThread = NUMBER_OF_TASKS / NUMBER_OF_THREADS;
    Instant startTime = Instant.now();
    for (int t = 0; t < NUMBER_OF_THREADS; t++) {
      new Thread(
              () -> {
                for (long i = 0; i < tasksPerThread; i++) {
                  executorA.schedule(getRunnable(), getRandomTime(), TimeUnit.MILLISECONDS);
                }
                threadCountDown.countDown();
              })
              .start();
    }
    threadCountDown.await();
    Instant scheduleTime = Instant.now();

    countDownLatch.await(60, TimeUnit.SECONDS);
    Instant executeTime = Instant.now();
    long executionDelay = Duration.between(scheduleTime, executeTime).toMillis();
    long totalDelay = Duration.between(startTime, executeTime).toMillis();
    long scheduleDelay = Duration.between(startTime, scheduleTime).toMillis();

    System.out.println(
            "new scheduling with defaults for "
                    + NUMBER_OF_TASKS
                    + " was done in "
                    + scheduleDelay
                    + "ms ("
                    + (NUMBER_OF_TASKS * 1000L / scheduleDelay)
                    + "TPS) execution was done in "
                    + executionDelay
                    + "ms ("
                    + (NUMBER_OF_TASKS * 1000L / executionDelay)
                    + "TPS)  total is "
                    + totalDelay
                    + "ms ("
                    + (NUMBER_OF_TASKS * 1000L / totalDelay)
                    + "TPS)  with "
                    + countDownLatch.getCount()
                    + " tasks remaining");
    assertTrue(true);
  }

  @Ignore
  @Test
  public void testPerformanceOnNewSchedulerNoBuffer()
          throws InterruptedException, RateLimitedScheduledExecutorManagerHasAlreadyStartedException {
    countDownLatch = new CountDownLatch(NUMBER_OF_TASKS);
    RateLimitedScheduledExecutorManager manager = new RateLimitedScheduledExecutorManager();
    manager.setBufferSize(0);
    RateLimitedScheduledExecutor executorA = manager.getRateLimitedScheduledExecutor("serviceA");
    // allow time for startup
    Thread.sleep(5000);
    CountDownLatch threadCountDown = new CountDownLatch(NUMBER_OF_THREADS);
    int tasksPerThread = NUMBER_OF_TASKS / NUMBER_OF_THREADS;
    Instant startTime = Instant.now();
    for (int t = 0; t < NUMBER_OF_THREADS; t++) {
      new Thread(
              () -> {
                for (long i = 0; i < tasksPerThread; i++) {
                  executorA.schedule(getRunnable(), getRandomTime(), TimeUnit.MILLISECONDS);
                }
                threadCountDown.countDown();
              })
              .start();
    }
    threadCountDown.await();
    Instant scheduleTime = Instant.now();

    countDownLatch.await(60, TimeUnit.SECONDS);
    Instant executeTime = Instant.now();
    long executionDelay = Duration.between(scheduleTime, executeTime).toMillis();
    long totalDelay = Duration.between(startTime, executeTime).toMillis();
    long scheduleDelay = Duration.between(startTime, scheduleTime).toMillis();

    System.out.println(
            "new scheduling with no buffer for "
                    + NUMBER_OF_TASKS
                    + " was done in "
                    + scheduleDelay
                    + "ms ("
                    + (NUMBER_OF_TASKS * 1000L / scheduleDelay)
                    + "TPS) execution was done in "
                    + executionDelay
                    + "ms ("
                    + (NUMBER_OF_TASKS * 1000L / executionDelay)
                    + "TPS)  total is "
                    + totalDelay
                    + "ms ("
                    + (NUMBER_OF_TASKS * 1000L / totalDelay)
                    + "TPS)  with "
                    + countDownLatch.getCount()
                    + " tasks remaining");
    assertTrue(true);
  }

  @Ignore
  @Test
  public void testPerformanceOnNewSchedulerLargeBuffer()
          throws InterruptedException, RateLimitedScheduledExecutorManagerHasAlreadyStartedException {
    countDownLatch = new CountDownLatch(NUMBER_OF_TASKS);
    RateLimitedScheduledExecutorManager manager = new RateLimitedScheduledExecutorManager();
    manager.setBufferSize(10);
    RateLimitedScheduledExecutor executorA = manager.getRateLimitedScheduledExecutor("serviceA");
    // allow time for startup
    Thread.sleep(5000);
    CountDownLatch threadCountDown = new CountDownLatch(NUMBER_OF_THREADS);
    int tasksPerThread = NUMBER_OF_TASKS / NUMBER_OF_THREADS;
    Instant startTime = Instant.now();
    for (int t = 0; t < NUMBER_OF_THREADS; t++) {
      new Thread(
              () -> {
                for (long i = 0; i < tasksPerThread; i++) {
                  executorA.schedule(getRunnable(), getRandomTime(), TimeUnit.MILLISECONDS);
                }
                threadCountDown.countDown();
              })
              .start();
    }
    threadCountDown.await();
    Instant scheduleTime = Instant.now();

    countDownLatch.await(60, TimeUnit.SECONDS);
    Instant executeTime = Instant.now();
    long executionDelay = Duration.between(scheduleTime, executeTime).toMillis();
    long totalDelay = Duration.between(startTime, executeTime).toMillis();
    long scheduleDelay = Duration.between(startTime, scheduleTime).toMillis();

    System.out.println(
            "new scheduling with large buffer for "
                    + NUMBER_OF_TASKS
                    + " was done in "
                    + scheduleDelay
                    + "ms ("
                    + (NUMBER_OF_TASKS * 1000L / scheduleDelay)
                    + "TPS) execution was done in "
                    + executionDelay
                    + "ms ("
                    + (NUMBER_OF_TASKS * 1000L / executionDelay)
                    + "TPS)  total is "
                    + totalDelay
                    + "ms ("
                    + (NUMBER_OF_TASKS * 1000L / totalDelay)
                    + "TPS)  with "
                    + countDownLatch.getCount()
                    + " tasks remaining");
    assertTrue(true);
  }

  @Ignore
  @Test
  public void testPerformanceOnNewSchedulerSmallInterval()
          throws InterruptedException, RateLimitedScheduledExecutorManagerHasAlreadyStartedException {
    countDownLatch = new CountDownLatch(NUMBER_OF_TASKS);
    RateLimitedScheduledExecutorManager manager = new RateLimitedScheduledExecutorManager();
    manager.setMillisPerInterval(100);
    RateLimitedScheduledExecutor executorA = manager.getRateLimitedScheduledExecutor("serviceA");
    // allow time for startup
    Thread.sleep(5000);
    CountDownLatch threadCountDown = new CountDownLatch(NUMBER_OF_THREADS);
    int tasksPerThread = NUMBER_OF_TASKS / NUMBER_OF_THREADS;
    Instant startTime = Instant.now();
    for (int t = 0; t < NUMBER_OF_THREADS; t++) {
      new Thread(
              () -> {
                for (long i = 0; i < tasksPerThread; i++) {
                  executorA.schedule(getRunnable(), getRandomTime(), TimeUnit.MILLISECONDS);
                }
                threadCountDown.countDown();
              })
              .start();
    }
    threadCountDown.await();
    Instant scheduleTime = Instant.now();

    countDownLatch.await(60, TimeUnit.SECONDS);
    Instant executeTime = Instant.now();
    long executionDelay = Duration.between(scheduleTime, executeTime).toMillis();
    long totalDelay = Duration.between(startTime, executeTime).toMillis();
    long scheduleDelay = Duration.between(startTime, scheduleTime).toMillis();

    System.out.println(
            "new scheduling with small interval for "
                    + NUMBER_OF_TASKS
                    + " was done in "
                    + scheduleDelay
                    + "ms ("
                    + (NUMBER_OF_TASKS * 1000L / scheduleDelay)
                    + "TPS) execution was done in "
                    + executionDelay
                    + "ms ("
                    + (NUMBER_OF_TASKS * 1000L / executionDelay)
                    + "TPS)  total is "
                    + totalDelay
                    + "ms ("
                    + (NUMBER_OF_TASKS * 1000L / totalDelay)
                    + "TPS)  with "
                    + countDownLatch.getCount()
                    + " tasks remaining");
    assertTrue(true);
  }

  @Ignore
  @Test
  public void testPerformanceOnNewSchedulerLargeInterval()
          throws InterruptedException, RateLimitedScheduledExecutorManagerHasAlreadyStartedException {
    countDownLatch = new CountDownLatch(NUMBER_OF_TASKS);
    RateLimitedScheduledExecutorManager manager = new RateLimitedScheduledExecutorManager();
    manager.setMillisPerInterval(5000);
    RateLimitedScheduledExecutor executorA = manager.getRateLimitedScheduledExecutor("serviceA");
    // allow time for startup
    Thread.sleep(5000);
    CountDownLatch threadCountDown = new CountDownLatch(NUMBER_OF_THREADS);
    int tasksPerThread = NUMBER_OF_TASKS / NUMBER_OF_THREADS;
    Instant startTime = Instant.now();
    for (int t = 0; t < NUMBER_OF_THREADS; t++) {
      new Thread(
              () -> {
                for (long i = 0; i < tasksPerThread; i++) {
                  executorA.schedule(getRunnable(), getRandomTime(), TimeUnit.MILLISECONDS);
                }
                threadCountDown.countDown();
              })
              .start();
    }
    threadCountDown.await();
    Instant scheduleTime = Instant.now();

    countDownLatch.await(60, TimeUnit.SECONDS);
    Instant executeTime = Instant.now();
    long executionDelay = Duration.between(scheduleTime, executeTime).toMillis();
    long totalDelay = Duration.between(startTime, executeTime).toMillis();
    long scheduleDelay = Duration.between(startTime, scheduleTime).toMillis();

    System.out.println(
            "new scheduling with large interval for "
                    + NUMBER_OF_TASKS
                    + " was done in "
                    + scheduleDelay
                    + "ms ("
                    + (NUMBER_OF_TASKS * 1000L / scheduleDelay)
                    + "TPS) execution was done in "
                    + executionDelay
                    + "ms ("
                    + (NUMBER_OF_TASKS * 1000L / executionDelay)
                    + "TPS)  total is "
                    + totalDelay
                    + "ms ("
                    + (NUMBER_OF_TASKS * 1000L / totalDelay)
                    + "TPS)  with "
                    + countDownLatch.getCount()
                    + " tasks remaining");
    assertTrue(true);
  }

  @Ignore
  @Test
  public void testPerformanceOnJVMScheduler() throws InterruptedException {
    countDownLatch = new CountDownLatch(NUMBER_OF_TASKS);
    ScheduledThreadPoolExecutor scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(8);
    // allow time for startup
    Thread.sleep(5000);

    CountDownLatch threadCountDown = new CountDownLatch(NUMBER_OF_THREADS);
    int tasksPerThread = NUMBER_OF_TASKS / NUMBER_OF_THREADS;
    Instant startTime = Instant.now();
    for (int t = 0; t < NUMBER_OF_THREADS; t++) {
      new Thread(
              () -> {
                for (long i = 0; i < tasksPerThread; i++) {
                  scheduledThreadPoolExecutor.schedule(
                          getRunnable(), getRandomTime(), TimeUnit.MILLISECONDS);
                }
                threadCountDown.countDown();
              })
              .start();
    }
    threadCountDown.await();
    Instant scheduleTime = Instant.now();
    countDownLatch.await();
    Instant executeTime = Instant.now();
    long executionDelay = Duration.between(scheduleTime, executeTime).toMillis();
    long totalDelay = Duration.between(startTime, executeTime).toMillis();
    long scheduleDelay = Duration.between(startTime, scheduleTime).toMillis();

    System.out.println(
            "JVM scheduling for "
                    + NUMBER_OF_TASKS
                    + " was done in "
                    + scheduleDelay
                    + "ms ("
                    + (NUMBER_OF_TASKS * 1000L / scheduleDelay)
                    + "TPS) execution was done in "
                    + executionDelay
                    + "ms ("
                    + (NUMBER_OF_TASKS * 1000L / executionDelay)
                    + "TPS)  total is "
                    + totalDelay
                    + "ms ("
                    + (NUMBER_OF_TASKS * 1000L / totalDelay)
                    + "TPS)  with "
                    + countDownLatch.getCount()
                    + " tasks remaining");
    assertTrue(true);
  }

  private int getRandomTime() {
    return random.nextInt(MAX_FUTURE_MILLIS);
  }

  private Runnable getRunnable() {
    return () -> countDownLatch.countDown();
  }
}
