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

package com.americanexpress.ratelimitedscheduler.integration;

import com.americanexpress.ratelimitedscheduler.RateLimitedScheduledExecutor;
import com.americanexpress.ratelimitedscheduler.RateLimitedScheduledExecutorManager;
import com.americanexpress.ratelimitedscheduler.exceptions.RateLimitedScheduledExecutorManagerHasAlreadyStartedException;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.logging.Level;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertTrue;

@SuppressWarnings({"squid:S1607", "squid:S2925"})
public class MemoryLeakTest {
  private static final Duration testDuration = Duration.ofMinutes(60);
  private final Clock clock = Clock.systemUTC();
  private Map<Integer, Long> memoryTracker;
  private Instant startTime;
  private ScheduledFuture<?> checkMemoryFuture;
  private RunnableTaskGenerator runnableTaskGeneratorA;
  private CallableTaskGenerator callableTaskGenerator;
  private CompletableFuture<Void> completableFuture;

  @Before
  public void setUp() {
    memoryTracker = new TreeMap<>();
    startTime = Instant.now();
    completableFuture = new CompletableFuture<>();
  }

  @Test
  @Ignore
  public void testForMemoryLeak()
          throws ExecutionException, InterruptedException,
          RateLimitedScheduledExecutorManagerHasAlreadyStartedException {
    LogLevelSetter.setLevel(Level.WARNING);
    RateLimitedScheduledExecutorManager manager = new RateLimitedScheduledExecutorManager();
    manager.setMillisPerInterval(100);
    RateLimitedScheduledExecutor executorA = manager.getRateLimitedScheduledExecutor("executorA");
    RateLimitedScheduledExecutor executorB = manager.getRateLimitedScheduledExecutor("executorB");
    RateLimitedScheduledExecutor executorC = manager.getRateLimitedScheduledExecutor("executorC");
    // set up for 100 tps
    runnableTaskGeneratorA = new RunnableTaskGenerator(executorA, 3000, 0, 60000, clock);
    callableTaskGenerator = new CallableTaskGenerator(executorB, 3000, 0, 60000, clock);
    // schedule some repeating tasks too
    executorC.scheduleAtFixedRate(new RepeatingTask(110), 1, 1, SECONDS);
    executorC.scheduleWithFixedDelay(new RepeatingTask(110), 1, 1, SECONDS);
    executorC.scheduleAtFixedRate(new RepeatingTask(110), 1, 1, SECONDS);
    executorC.scheduleWithFixedDelay(new RepeatingTask(110), 1, 1, SECONDS);
    executorC.scheduleAtFixedRate(new RepeatingTask(110), 1, 1, SECONDS);
    executorC.scheduleWithFixedDelay(new RepeatingTask(110), 1, 1, SECONDS);
    executorC.scheduleAtFixedRate(new RepeatingTask(110), 1, 1, SECONDS);
    executorC.scheduleWithFixedDelay(new RepeatingTask(110), 1, 1, SECONDS);
    executorC.scheduleAtFixedRate(new RepeatingTask(110), 1, 1, SECONDS);
    executorC.scheduleWithFixedDelay(new RepeatingTask(110), 1, 1, SECONDS);

    ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(2);
    checkMemoryFuture = executorService.scheduleAtFixedRate(checkMemoryRunnable(), 60, 60, SECONDS);
    runnableTaskGeneratorA.generateTasks();
    callableTaskGenerator.generateTasks();
    completableFuture.get();
    System.out.println("memory tracker has the below values");
    System.out.println(memoryTracker);
    List<Long> memoryUsage = new ArrayList<>(memoryTracker.values());
    if (memoryUsage.size() > 5) {
      long startMemoryUsageAverage = 0;
      long endMemoryUsageAverage = 0;
      for (int i = 0; i < 5; i++) {
        startMemoryUsageAverage += memoryUsage.get(i);
        endMemoryUsageAverage += memoryUsage.get(memoryUsage.size() - (i + 1));
      }
      startMemoryUsageAverage = startMemoryUsageAverage / 5;
      endMemoryUsageAverage = endMemoryUsageAverage / 5;
      System.out.println("average usage of first 5 mins is " + startMemoryUsageAverage);
      System.out.println("average usage of last 5 mins is " + endMemoryUsageAverage);
      assertTrue(startMemoryUsageAverage * 1.5 > endMemoryUsageAverage);
    }
  }

  private Runnable checkMemoryRunnable() {
    return () -> {
      System.gc();
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        e.printStackTrace();
        Thread.currentThread().interrupt();
      }
      int minuteOfRun = (int) Duration.between(startTime, Instant.now()).toSeconds() / 60;
      memoryTracker.put(minuteOfRun, getMemoryUsed());
      if (testDuration.minus(Duration.ofMinutes(minuteOfRun)).isNegative()) {
        checkMemoryFuture.cancel(false);
        System.out.println("wrapping up the work");
        completableFuture.complete(null);
      }
      runnableTaskGeneratorA.generateTasks();
      callableTaskGenerator.generateTasks();
    };
  }

  private long getMemoryUsed() {
    MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    long memoryUsed = memoryMXBean.getHeapMemoryUsage().getUsed() / 1_000;
    System.out.println(
            Instant.now()
                    + " - memory used is "
                    + memoryUsed
                    + "kb out of max"
                    + (memoryMXBean.getHeapMemoryUsage().getMax() / 1_000)
                    + "kb. We have "
                    + Thread.activeCount()
                    + " threads");
    return memoryUsed;
  }
}
