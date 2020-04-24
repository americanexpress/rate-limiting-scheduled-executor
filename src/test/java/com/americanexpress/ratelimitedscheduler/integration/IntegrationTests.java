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

import com.americanexpress.ratelimitedscheduler.ConfiguredComponentSupplier;
import com.americanexpress.ratelimitedscheduler.RateLimitedScheduledExecutor;
import com.americanexpress.ratelimitedscheduler.RateLimitedScheduledExecutorManager;
import com.americanexpress.ratelimitedscheduler.exceptions.ConfiguredComponentSupplierHasBeenAccessedException;
import com.americanexpress.ratelimitedscheduler.exceptions.RateLimitedScheduledExecutorHasTasksScheduledException;
import com.americanexpress.ratelimitedscheduler.exceptions.RateLimitedScheduledExecutorManagerHasAlreadyStartedException;
import com.americanexpress.ratelimitedscheduler.peers.PeerFinderStaticImpl;
import com.americanexpress.ratelimitedscheduler.task.ScheduledRunnableTask;
import com.americanexpress.ratelimitedscheduler.task.ScheduledTask;

import org.junit.Before;
import org.junit.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Comparator;
import java.util.SortedMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.logging.Level;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static junit.framework.TestCase.assertTrue;

public class IntegrationTests {

  private final Clock clock = Clock.systemUTC();
  private RateLimitedScheduledExecutor executorA;
  private TaskTracker taskTrackerA;
  private PeerFinderStaticImpl peerFinder;

  @Before
  public void setUp()
          throws RateLimitedScheduledExecutorManagerHasAlreadyStartedException,
          ConfiguredComponentSupplierHasBeenAccessedException {
    LogLevelSetter.setLevel(Level.INFO);
    peerFinder = new PeerFinderStaticImpl(1);
    ConfiguredComponentSupplier configuredComponentSupplier = new ConfiguredComponentSupplier();
    configuredComponentSupplier.setPeerFinder(peerFinder);
    RateLimitedScheduledExecutorManager manager =
            new RateLimitedScheduledExecutorManager(configuredComponentSupplier);
    manager.setMillisPerInterval(100);
    executorA = manager.getRateLimitedScheduledExecutor("executorA");
    taskTrackerA = new TaskTracker();
  }
  // note, this test may fail if the execution thread is very slow and so a task jumps over to the
  // next second
  @Test
  public void testMaxTPS() throws InterruptedException {
    executorA.setMaxTPS(105);
    ScheduledFuture<?> fixedRateFuture =
            executorA.scheduleAtFixedRate(new RepeatingTask(taskTrackerA, 100), 0, 100, MILLISECONDS);
    ScheduledFuture<?> fixedDelayFuture =
            executorA.scheduleWithFixedDelay(
                    new RepeatingTask(taskTrackerA, 100), 0, 1000, MILLISECONDS);
    RunnableTaskGenerator runnableTaskGenerator =
            new RunnableTaskGenerator(taskTrackerA, executorA, 500, 3000, 5000, Clock.systemUTC());
    CallableTaskGenerator callableTaskGenerator =
            new CallableTaskGenerator(taskTrackerA, executorA, 500, 3000, 5000, Clock.systemUTC());
    CountDownLatch callableLatch = callableTaskGenerator.generateTasks();
    runnableTaskGenerator.generateTasks().await();
    callableLatch.await();
    fixedDelayFuture.cancel(false);
    fixedRateFuture.cancel(false);
    // a little leeway for GC
    assertTrue(taskTrackerA.getMaxTPS() < 107);
  }


  @Test
  public void testMaxTPSWith2Peers() throws InterruptedException {
    peerFinder.setTotalNumberOfPeers(3);
    executorA.setMaxTPS(105);
    //wait for the total number of peers to be picked up
    Thread.sleep(5000);
    ScheduledFuture<?> fixedRateFuture =
            executorA.scheduleAtFixedRate(new RepeatingTask(taskTrackerA, 100), 0, 100, MILLISECONDS);
    ScheduledFuture<?> fixedDelayFuture =
            executorA.scheduleWithFixedDelay(
                    new RepeatingTask(taskTrackerA, 100), 0, 1000, MILLISECONDS);
    RunnableTaskGenerator runnableTaskGenerator =
            new RunnableTaskGenerator(taskTrackerA, executorA, 200, 3000, 5000, Clock.systemUTC());
    CallableTaskGenerator callableTaskGenerator =
            new CallableTaskGenerator(taskTrackerA, executorA, 200, 3000, 5000, Clock.systemUTC());
    CountDownLatch callableLatch = callableTaskGenerator.generateTasks();
    runnableTaskGenerator.generateTasks().await();
    callableLatch.await();
    fixedDelayFuture.cancel(false);
    fixedRateFuture.cancel(false);
    // a little leeway for GC
    assertTrue(taskTrackerA.getMaxTPS() < 37);
  }

  @Test
  public void testRepeatingRateTaskGaps() throws InterruptedException {
    CountDownLatch countDownLatch = new CountDownLatch(100);
    ScheduledFuture<?> fixedRateFuture =
            executorA.scheduleAtFixedRate(
                    new RepeatingTask(taskTrackerA, countDownLatch, 1000), 0, 500, MILLISECONDS);
    countDownLatch.await();
    fixedRateFuture.cancel(false);
    // make sure there's at least 500ms - 100ms between each task run (gives a little leeway for
    // sleepy
    // threads)
    long averageDifferenceBetweenTaskTriggersMs =
            taskTrackerA.getAverageDifferenceBetweenTaskTriggersMs();
    System.out.println(
            "averageDifferenceBetweenTaskTriggersMs = " + averageDifferenceBetweenTaskTriggersMs);

    assertTrue(averageDifferenceBetweenTaskTriggersMs >= 490);
  }

  @Test
  public void testRepeatingIntervalTaskGaps() throws InterruptedException {
    CountDownLatch countDownLatch = new CountDownLatch(30);
    ScheduledFuture<?> fixedRateFuture =
            executorA.scheduleWithFixedDelay(
                    new RepeatingTask(taskTrackerA, countDownLatch, 1000), 0, 500, MILLISECONDS);
    countDownLatch.await();
    fixedRateFuture.cancel(false);
    // make sure there's at least 1.5s - 100ms (the window size) between each task run (gives a
    // little leeway for sleepy
    // threads)
    long averageDifferenceBetweenTaskTriggersMs =
            taskTrackerA.getAverageDifferenceBetweenTaskTriggersMs();
    long minimumDifferenceBetweenTaskEndAndTriggersMs =
            taskTrackerA.getMinimumDifferenceBetweenTaskEndAndTriggersMs();
    System.out.println(
            "minimumDifferenceBetweenTaskTriggersMs = "
                    + averageDifferenceBetweenTaskTriggersMs
                    + " minimumDifferenceBetweenTaskEndAndTriggersMs = "
                    + minimumDifferenceBetweenTaskEndAndTriggersMs);

    assertTrue(averageDifferenceBetweenTaskTriggersMs >= 1400);
    assertTrue(minimumDifferenceBetweenTaskEndAndTriggersMs >= 400);
  }

  @Test
  public void testNoTaskExecutedWholeIntervalEarly() throws InterruptedException {
    RunnableTaskGenerator runnableTaskGenerator =
            new RunnableTaskGenerator(taskTrackerA, executorA, 10000, 3000, 5000, Clock.systemUTC());
    runnableTaskGenerator.generateTasks().await();
    assertTrue(taskTrackerA.getMinimumDifferenceBetweenScheduledTimeAndExecuteTimeInSeconds() >= 0);
  }

  @Test
  public void testNoTaskExecutedEarlyWithFlagSet()
          throws InterruptedException, RateLimitedScheduledExecutorHasTasksScheduledException {
    executorA.setEnsureNoTaskDispatchedEarly(true);
    RunnableTaskGenerator runnableTaskGenerator =
            new RunnableTaskGenerator(taskTrackerA, executorA, 10000, 3000, 5000, Clock.systemUTC());
    runnableTaskGenerator.generateTasks().await();
    // a tiny bit of slack to avoid test failing due to rounding issues
    assertTrue(
            taskTrackerA.getMinimumDifferenceBetweenScheduledTimeAndExecuteTimeInMilliSeconds() >= -1);
  }

  // note, this test may fail if the execution thread is very slow and so a task jumps over to the
  // next interval
  @Test
  public void testNoTaskExecutedVeryLate() throws InterruptedException {
    executorA.setTaskTimeout(Duration.ofSeconds(5));
    executorA.setMaxTPS(100);
    RunnableTaskGenerator runnableTaskGenerator =
            new RunnableTaskGenerator(taskTrackerA, executorA, 1000, 300, 500, clock);
    runnableTaskGenerator.generateTasks().await();
    taskTrackerA.printAllTasks();
    assertTrue(taskTrackerA.getMaximumDifferenceBetweenScheduledTimeAndExecuteTime() <= 6);
  }

  @Test
  public void testSortOrderWorks() throws InterruptedException {
    executorA.setMaxTPS(100);
    // sort by reverse ID
    Comparator<ScheduledTask> sortMethod =
            (delayed1, delayed2) -> {
              if (delayed1 instanceof ScheduledRunnableTask
                      && delayed2 instanceof ScheduledRunnableTask) {
                Runnable runnable1 = ((ScheduledRunnableTask) delayed1).getRunnable();
                Runnable runnable2 = ((ScheduledRunnableTask) delayed2).getRunnable();
                if (runnable1 instanceof RunnableTask && runnable2 instanceof RunnableTask) {
                  return ((Task) runnable2).getId() - ((Task) runnable1).getId();
                }
              }
              return Long.compare(delayed2.getDelay(MILLISECONDS), delayed1.getDelay(MILLISECONDS));
            };

    executorA.setSortMethod(sortMethod);
    RunnableTaskGenerator runnableTaskGenerator =
            new RunnableTaskGenerator(
                    taskTrackerA,
                    executorA,
                    1000,
                    10300,
                    10500,
                    Clock.fixed(Instant.now(), ZoneOffset.UTC));
    CountDownLatch countDownLatch = runnableTaskGenerator.generateTasks();
    countDownLatch.await();

    // check the average ID for each second is less than the previous one :)
    SortedMap<Long, Float> averageIDPerSecond = taskTrackerA.getAverageIDPerSecond();

    float lastID = Float.MAX_VALUE;
    for (Float averageID : averageIDPerSecond.values()) {
      assertTrue(averageID < lastID);
      lastID = averageID;
    }
  }
}
