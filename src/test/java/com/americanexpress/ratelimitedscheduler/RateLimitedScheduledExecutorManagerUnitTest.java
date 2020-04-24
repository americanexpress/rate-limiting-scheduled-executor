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

package com.americanexpress.ratelimitedscheduler;

import com.americanexpress.ratelimitedscheduler.exceptions.RateLimitedScheduledExecutorManagerHasAlreadyStartedException;
import com.americanexpress.ratelimitedscheduler.peers.PeerFinder;
import com.americanexpress.ratelimitedscheduler.task.ScheduledRunnableTask;
import com.americanexpress.ratelimitedscheduler.task.ScheduledTask;
import com.americanexpress.ratelimitedscheduler.task.ScheduledTaskFactory;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static java.util.logging.Level.WARNING;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "squid:S1192"})
@RunWith(MockitoJUnitRunner.class)
public class RateLimitedScheduledExecutorManagerUnitTest {
  @Mock
  private ScheduledExecutorService scheduledExecutorService;
  @Mock
  private ConfiguredComponentSupplier configuredComponentSupplier;
  @Mock
  private IntervalRunnerFactory intervalRunnerFactory;
  @Mock
  private ClockSupplier clockSupplier;
  @Mock
  private Clock clock;
  @Mock
  private RateLimitedScheduledExecutorFactory rateLimitedScheduledExecutorFactory;
  @Mock
  private Logger logger;
  @Mock
  private RateLimitedScheduledExecutor rateLimitedScheduledExecutorA;
  @Mock
  private RateLimitedScheduledExecutor rateLimitedScheduledExecutorB;
  @Mock
  private ScheduledRunnableTask scheduledTaskA;
  @Mock
  private ScheduledRunnableTask scheduledTaskA2;
  @Mock
  private ScheduledRunnableTask scheduledTaskB;
  @Mock
  private ScheduledRunnableTask scheduledTaskB2;
  @Mock
  private IntervalRunner intervalRunner;
  @Mock
  private ScheduledTaskFactory scheduledTaskFactory;
  @Mock
  private PeerFinder peerFinder;
  private RateLimitedScheduledExecutorManager rateLimitedScheduledExecutorManager;

  @Before
  public void setUp() {
    when(clockSupplier.getClock()).thenReturn(clock);
    when(configuredComponentSupplier.getScheduledExecutorService())
            .thenReturn(scheduledExecutorService);
    when(configuredComponentSupplier.getPeerFinder()).thenReturn(peerFinder);
    when(peerFinder.getTotalNumberOfPeers()).thenReturn(1);

    when(clock.instant()).thenReturn(Instant.ofEpochSecond(100));
    when(intervalRunnerFactory.getIntervalRunner(anyLong(), anyList())).thenReturn(intervalRunner);
    when(intervalRunner.submitTasksToScheduler())
            .thenReturn(CompletableFuture.completedFuture(null));
    when(rateLimitedScheduledExecutorA.getTasksForIntervalNumber(anyLong()))
            .thenReturn(List.of(scheduledTaskA, scheduledTaskA2));
    when(rateLimitedScheduledExecutorB.getTasksForIntervalNumber(anyLong()))
            .thenReturn(List.of(scheduledTaskB, scheduledTaskB2));
    rateLimitedScheduledExecutorManager =
            new RateLimitedScheduledExecutorManager(
                    configuredComponentSupplier,
                    intervalRunnerFactory,
                    scheduledTaskFactory,
                    rateLimitedScheduledExecutorFactory,
                    clockSupplier,
                    logger);

    when(rateLimitedScheduledExecutorFactory.getRateLimitedScheduledExecutorService(
            "serviceA", 0, false, rateLimitedScheduledExecutorManager))
            .thenReturn(rateLimitedScheduledExecutorA);
    when(rateLimitedScheduledExecutorFactory.getRateLimitedScheduledExecutorService(
            "serviceA", 0, true, rateLimitedScheduledExecutorManager))
            .thenReturn(rateLimitedScheduledExecutorA);
    when(rateLimitedScheduledExecutorFactory.getRateLimitedScheduledExecutorService(
            "serviceB", 0, false, rateLimitedScheduledExecutorManager))
            .thenReturn(rateLimitedScheduledExecutorB);
    when(rateLimitedScheduledExecutorFactory.getRateLimitedScheduledExecutorService(
            "serviceA", 1, false, rateLimitedScheduledExecutorManager))
            .thenReturn(rateLimitedScheduledExecutorA);
  }

  @Test
  public void testAlternateConstructor() {
    rateLimitedScheduledExecutorManager = new RateLimitedScheduledExecutorManager();
    rateLimitedScheduledExecutorManager.getRateLimitedScheduledExecutor(
            "serviceA", 30, Duration.ofSeconds(20), true);
    assertEquals(1000, rateLimitedScheduledExecutorManager.getMillisPerInterval());
    assertEquals(2, rateLimitedScheduledExecutorManager.getBufferSize());
    assertEquals(1, rateLimitedScheduledExecutorManager.getServiceList().size());
  }

  @Test
  public void testAlternateExecutorParameters() {

    rateLimitedScheduledExecutorManager.getRateLimitedScheduledExecutor(
            "serviceA", 30, Duration.ofSeconds(20), true);
    verify(rateLimitedScheduledExecutorFactory)
            .getRateLimitedScheduledExecutorService(
                    "serviceA", 0, true, rateLimitedScheduledExecutorManager);

    verify(rateLimitedScheduledExecutorA).setMaxTPS(30);
    verify(rateLimitedScheduledExecutorA).setTaskTimeout(Duration.ofSeconds(20));
  }

  @Test
  public void testGetAllTasksForInterval() {
    rateLimitedScheduledExecutorManager.getRateLimitedScheduledExecutor("serviceA");
    rateLimitedScheduledExecutorManager.getRateLimitedScheduledExecutor("serviceB");
    List<ScheduledTask> allTasksForInterval =
            rateLimitedScheduledExecutorManager.getAllTasksForInterval(100);
    assertEquals(4, allTasksForInterval.size());
    assertTrue(allTasksForInterval.contains(scheduledTaskA));
    assertTrue(allTasksForInterval.contains(scheduledTaskA2));
    assertTrue(allTasksForInterval.contains(scheduledTaskB));
    assertTrue(allTasksForInterval.contains(scheduledTaskB2));
    verify(rateLimitedScheduledExecutorA).getTasksForIntervalNumber(100);
    verify(rateLimitedScheduledExecutorB).getTasksForIntervalNumber(100);
  }

  @Test
  public void testInitialisation() {
    rateLimitedScheduledExecutorManager.initializeIfNeeded();
    assertTrue(rateLimitedScheduledExecutorManager.getServiceList().isEmpty());
    assertEquals(102, rateLimitedScheduledExecutorManager.getIntervalCounter());
    verify(scheduledExecutorService)
            .scheduleAtFixedRate(any(), eq(0L), eq(1000L), eq(TimeUnit.MILLISECONDS));
    ArgumentCaptor<Runnable> runnableArgumentCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(scheduledExecutorService)
            .scheduleWithFixedDelay(
                    runnableArgumentCaptor.capture(), eq(0L), eq(5000L), eq(TimeUnit.MILLISECONDS));
    runnableArgumentCaptor.getValue().run();
    verify(peerFinder).updateNetworkWithThisInstance();
    verify(peerFinder).getTotalNumberOfPeers();
    clearInvocations(peerFinder);
    rateLimitedScheduledExecutorManager.getRateLimitedScheduledExecutor("serviceA");
    when(peerFinder.getTotalNumberOfPeers()).thenReturn(2);
    runnableArgumentCaptor.getValue().run();
    verify(peerFinder).updateNetworkWithThisInstance();
    verify(peerFinder).getTotalNumberOfPeers();
    verify(rateLimitedScheduledExecutorA).setNumberOfPeers(2);
  }

  @Test
  public void testRegularTask() {
    rateLimitedScheduledExecutorManager.getRateLimitedScheduledExecutor("serviceA");
    rateLimitedScheduledExecutorManager.getRateLimitedScheduledExecutor("serviceB");
    ArgumentCaptor<Runnable> runnableArgumentCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(scheduledExecutorService)
            .scheduleAtFixedRate(
                    runnableArgumentCaptor.capture(), eq(0L), eq(1000L), eq(TimeUnit.MILLISECONDS));
    Runnable task = runnableArgumentCaptor.getValue();
    task.run();
    verify(rateLimitedScheduledExecutorA).getTasksForIntervalNumber(102);
    verify(rateLimitedScheduledExecutorB).getTasksForIntervalNumber(102);
    verify(logger).finer("processing tasks for 102, got 4 items in total");
    ArgumentCaptor<List> listArgumentCaptor = ArgumentCaptor.forClass(List.class);
    verify(intervalRunnerFactory).getIntervalRunner(eq(102L), listArgumentCaptor.capture());
    List resultList = listArgumentCaptor.getValue();
    assertTrue(resultList.contains(scheduledTaskA));
    assertTrue(resultList.contains(scheduledTaskB));
    assertTrue(resultList.contains(scheduledTaskA2));
    assertTrue(resultList.contains(scheduledTaskB2));
    assertEquals(4, resultList.size());
    verify(intervalRunner).submitTasksToScheduler();
    verify(rateLimitedScheduledExecutorA).removeCountsForInterval(102);
    verify(rateLimitedScheduledExecutorB).removeCountsForInterval(102);
  }

  @Test
  public void testExceptionLogging() {
    rateLimitedScheduledExecutorManager.getRateLimitedScheduledExecutor("serviceA");
    ArgumentCaptor<Runnable> runnableArgumentCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(scheduledExecutorService)
            .scheduleAtFixedRate(
                    runnableArgumentCaptor.capture(), eq(0L), eq(1000L), eq(TimeUnit.MILLISECONDS));
    Runnable task = runnableArgumentCaptor.getValue();
    rateLimitedScheduledExecutorManager.getRateLimitedScheduledExecutor("serviceA");
    when(rateLimitedScheduledExecutorA.getTasksForIntervalNumber(102))
            .thenThrow(new RuntimeException("oh noes"));
    task.run();
    verify(logger).log(eq(WARNING), eq("got a throwable of "), any(RuntimeException.class));
  }

  @Test
  public void testLoggerInitialisedWhenNoLogger() {
    rateLimitedScheduledExecutorManager =
            new RateLimitedScheduledExecutorManager(
                    configuredComponentSupplier,
                    intervalRunnerFactory,
                    scheduledTaskFactory,
                    rateLimitedScheduledExecutorFactory,
                    clockSupplier,
                    null);
    assertNotNull(rateLimitedScheduledExecutorManager.getLogger());
  }

  @Test
  public void testAddRecordsForMissingInterval() {
    assertFalse(rateLimitedScheduledExecutorManager.addTaskForInterval(scheduledTaskA, 50));
  }

  @Test
  public void testAddRecordsWithRunnerException() {
    rateLimitedScheduledExecutorManager.getRateLimitedScheduledExecutor("serviceA");
    rateLimitedScheduledExecutorManager.getRateLimitedScheduledExecutor("serviceB");

    ArgumentCaptor<Runnable> runnableArgumentCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(scheduledExecutorService)
            .scheduleAtFixedRate(
                    runnableArgumentCaptor.capture(), eq(0L), eq(1000L), eq(TimeUnit.MILLISECONDS));
    Runnable task = runnableArgumentCaptor.getValue();
    task.run();
    verify(rateLimitedScheduledExecutorA).getTasksForIntervalNumber(102);
    verify(rateLimitedScheduledExecutorB).getTasksForIntervalNumber(102);
    verify(logger).finer("processing tasks for 102, got 4 items in total");
    doThrow(new RejectedExecutionException("whatever")).when(intervalRunner).addTask(any());
    assertFalse(rateLimitedScheduledExecutorManager.addTaskForInterval(scheduledTaskA, 102));
    verify(intervalRunner).addTask(scheduledTaskA);
    verify(logger)
            .finer("tried to add task to list for interval 102 but it was already being executed");
  }

  @Test
  public void testAddRecordsHappyPath() {
    rateLimitedScheduledExecutorManager.getRateLimitedScheduledExecutor("serviceA");
    rateLimitedScheduledExecutorManager.getRateLimitedScheduledExecutor("serviceB");

    ArgumentCaptor<Runnable> runnableArgumentCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(scheduledExecutorService)
            .scheduleAtFixedRate(
                    runnableArgumentCaptor.capture(), eq(0L), eq(1000L), eq(TimeUnit.MILLISECONDS));
    Runnable task = runnableArgumentCaptor.getValue();
    task.run();
    verify(rateLimitedScheduledExecutorA).getTasksForIntervalNumber(102);
    verify(rateLimitedScheduledExecutorB).getTasksForIntervalNumber(102);
    verify(logger).finer("processing tasks for 102, got 4 items in total");
    assertTrue(rateLimitedScheduledExecutorManager.addTaskForInterval(scheduledTaskA, 102));
    verify(intervalRunner).addTask(scheduledTaskA);
  }

  @Test
  public void testSetMillisPerIntervalHappyPath()
          throws RateLimitedScheduledExecutorManagerHasAlreadyStartedException {
    rateLimitedScheduledExecutorManager.setMillisPerInterval(500);
    verify(rateLimitedScheduledExecutorFactory).setMillisPerInterval(500);
    verify(intervalRunnerFactory).setMillisPerInterval(500);
    verify(scheduledTaskFactory).setMillisPerInterval(500);
    assertEquals(500, rateLimitedScheduledExecutorManager.getMillisPerInterval());
  }

  @Test
  public void testSetMillisPerIntervalAfterStartThrowsException() {
    rateLimitedScheduledExecutorManager.getRateLimitedScheduledExecutor("serviceA");
    try {
      rateLimitedScheduledExecutorManager.setMillisPerInterval(500);
      fail();
    } catch (Exception e) {
      assertEquals(
              RateLimitedScheduledExecutorManagerHasAlreadyStartedException.class, e.getClass());
    }
    verify(rateLimitedScheduledExecutorFactory, times(0)).setMillisPerInterval(500);
    verify(intervalRunnerFactory, times(0)).setMillisPerInterval(500);
    verify(scheduledTaskFactory, times(0)).setMillisPerInterval(500);
    assertEquals(1000, rateLimitedScheduledExecutorManager.getMillisPerInterval());
  }

  @Test
  public void testSetPeerFinderIntervalMsAfterStartThrowsException() {
    rateLimitedScheduledExecutorManager.getRateLimitedScheduledExecutor("serviceA");
    try {
      rateLimitedScheduledExecutorManager.setPeerFinderIntervalMs(500);
      fail();
    } catch (Exception e) {
      assertEquals(
              RateLimitedScheduledExecutorManagerHasAlreadyStartedException.class, e.getClass());
    }
    assertEquals(5000, rateLimitedScheduledExecutorManager.getPeerFinderIntervalMs());
  }

  @Test
  public void testSetPeerFinderMsHappyPath()
          throws RateLimitedScheduledExecutorManagerHasAlreadyStartedException {
    rateLimitedScheduledExecutorManager.setPeerFinderIntervalMs(2000);
    rateLimitedScheduledExecutorManager.initializeIfNeeded();
    verify(scheduledExecutorService)
            .scheduleWithFixedDelay(any(Runnable.class), eq(0L), eq(2000L), eq(TimeUnit.MILLISECONDS));
    assertEquals(2000, rateLimitedScheduledExecutorManager.getPeerFinderIntervalMs());
  }

  @Test
  public void testSetMillisPerIntervalWithValueTooSmall()
          throws RateLimitedScheduledExecutorManagerHasAlreadyStartedException {
    try {
      rateLimitedScheduledExecutorManager.setMillisPerInterval(5);
      fail();
    } catch (RuntimeException e) {
      assertEquals(IllegalArgumentException.class, e.getClass());
      assertEquals("millisPerInterval must be at least 10", e.getMessage());
    }
    verify(rateLimitedScheduledExecutorFactory, times(0)).setMillisPerInterval(500);
    verify(intervalRunnerFactory, times(0)).setMillisPerInterval(500);
    verify(scheduledTaskFactory, times(0)).setMillisPerInterval(500);
    assertEquals(1000, rateLimitedScheduledExecutorManager.getMillisPerInterval());
  }

  @Test
  public void testSetBufferSizeHappyPath()
          throws RateLimitedScheduledExecutorManagerHasAlreadyStartedException {
    rateLimitedScheduledExecutorManager.setBufferSize(50);
    assertEquals(50, rateLimitedScheduledExecutorManager.getBufferSize());
    rateLimitedScheduledExecutorManager.getRateLimitedScheduledExecutor("serviceA");
    assertEquals(150, rateLimitedScheduledExecutorManager.getIntervalCounter());
  }

  @Test
  public void testSetBufferAfterStartThrowsException() {
    rateLimitedScheduledExecutorManager.getRateLimitedScheduledExecutor("serviceA");
    try {
      rateLimitedScheduledExecutorManager.setBufferSize(50);
      fail();
    } catch (Exception e) {
      assertEquals(
              RateLimitedScheduledExecutorManagerHasAlreadyStartedException.class, e.getClass());
    }
    assertEquals(2, rateLimitedScheduledExecutorManager.getBufferSize());
  }

  @Test
  public void testSeBufferWithNegativeThrowsException()
          throws RateLimitedScheduledExecutorManagerHasAlreadyStartedException {
    try {
      rateLimitedScheduledExecutorManager.setBufferSize(-1);
      fail();
    } catch (RuntimeException e) {
      assertEquals(IllegalArgumentException.class, e.getClass());
      assertEquals("bufferSize must be greater than 0", e.getMessage());
    }

    assertEquals(2, rateLimitedScheduledExecutorManager.getBufferSize());
  }

  @Test
  public void testShutdown() {
    rateLimitedScheduledExecutorManager.shutdown();
    verify(configuredComponentSupplier, times(1)).shutdown();
  }
}
