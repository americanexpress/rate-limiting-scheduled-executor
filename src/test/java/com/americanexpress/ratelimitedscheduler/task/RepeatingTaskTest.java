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

package com.americanexpress.ratelimitedscheduler.task;

import com.americanexpress.ratelimitedscheduler.RateLimitedScheduledExecutor;

import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.logging.Logger;

import static com.americanexpress.ratelimitedscheduler.task.RepeatingTask.RepeatingType.DELAY;
import static com.americanexpress.ratelimitedscheduler.task.RepeatingTask.RepeatingType.RATE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RepeatingTaskTest {
  private final int millisPerInterval = 1000;
  private RepeatingTask repeatingTask;
  @Mock
  private ScheduledTaskFactory scheduledTaskFactory;
  @Mock
  private RateLimitedScheduledExecutor rateLimitedScheduledExecutor;
  @Mock
  private Clock clock;
  @Mock
  private Logger logger;
  private Runnable runnable;
  @Mock
  private ScheduledRunnableTask<Void> scheduledRunnableTask;
  private ArgumentCaptor<BiConsumer<? super Void, ? super Throwable>> consumerCaptor;

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() {
    runnable = TestCase::fail;
    consumerCaptor = ArgumentCaptor.forClass(BiConsumer.class);
    when(clock.instant()).thenReturn(Instant.ofEpochSecond(100));

    repeatingTask =
            new RepeatingTask(
                    runnable,
                    Duration.ofMinutes(1),
                    Duration.ofSeconds(10),
                    false,
                    RATE,
                    millisPerInterval,
                    scheduledTaskFactory,
                    rateLimitedScheduledExecutor,
                    clock,
                    logger);
    when(scheduledTaskFactory.getRepeatingScheduledTask(eq(runnable), any(Duration.class)))
            .thenReturn(scheduledRunnableTask);
  }

  @Test
  public void ensureRateTaskRejectedWhenUnderInterval() {
    try {
      new RepeatingTask(
              runnable,
              Duration.ofMinutes(1),
              Duration.ofMillis(500),
              false,
              RATE,
              millisPerInterval,
              scheduledTaskFactory,
              rateLimitedScheduledExecutor,
              clock,
              logger);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals(
              "we can not schedule repeating tasks more than once per interval", e.getMessage());
    }
  }

  @Test
  public void ensureIntervalTaskRejectedWhenUnder5Intervals() {
    try {
      new RepeatingTask(
              runnable,
              Duration.ofSeconds(60),
              Duration.ofSeconds(4),
              false,
              DELAY,
              millisPerInterval,
              scheduledTaskFactory,
              rateLimitedScheduledExecutor,
              clock,
              logger);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals(
              "we can not schedule repeat with fixed delay tasks more than once per 5 intervals",
              e.getMessage());
    }
  }

  @Test
  public void getTaskForIntervalBeforeTaskAvailable() {
    assertFalse(repeatingTask.getTaskForIntervalNumber(105).isPresent());
  }

  @Test
  public void testLoggingOnMissedSecondForRateBasedRepeat() {
    Optional<ScheduledRunnableTask<Void>> taskForInterval =
            repeatingTask.getTaskForIntervalNumber(165);
    assertTrue(taskForInterval.isPresent());
    assertEquals(scheduledRunnableTask, taskForInterval.get());
    verify(logger)
            .fine(
                    "we seemed to have missed the correct interval for this task - was it triggered in the past?");
  }

  @Test
  public void testWarningOnMissedSecondForDelayBasedRepeat() {
    repeatingTask =
            new RepeatingTask(
                    runnable,
                    Duration.ofMinutes(1),
                    Duration.ofSeconds(10),
                    false,
                    DELAY,
                    millisPerInterval,
                    scheduledTaskFactory,
                    rateLimitedScheduledExecutor,
                    clock,
                    logger);
    Optional<ScheduledRunnableTask<Void>> taskForInterval =
            repeatingTask.getTaskForIntervalNumber(165);
    assertTrue(taskForInterval.isPresent());
    assertEquals(scheduledRunnableTask, taskForInterval.get());
    verify(logger)
            .fine(
                    "we seemed to have missed the correct interval for this task - was it triggered in the past?");
  }

  @Test
  public void getTaskForIntervalAtInterval() {
    when(scheduledTaskFactory.getRepeatingScheduledTask(eq(runnable), any(Duration.class)))
            .thenReturn(scheduledRunnableTask);
    // after a minute
    Optional<ScheduledRunnableTask<Void>> taskForInterval =
            repeatingTask.getTaskForIntervalNumber(160);
    assertTrue(taskForInterval.isPresent());
    assertEquals(scheduledRunnableTask, taskForInterval.get());
    // after 5 more seconds should be false
    assertFalse(repeatingTask.getTaskForIntervalNumber(165).isPresent());
    // after 5 more seconds should exist
    taskForInterval = repeatingTask.getTaskForIntervalNumber(170);
    assertTrue(taskForInterval.isPresent());
    assertEquals(scheduledRunnableTask, taskForInterval.get());
    taskForInterval = repeatingTask.getTaskForIntervalNumber(180);
    assertTrue(taskForInterval.isPresent());
    assertEquals(scheduledRunnableTask, taskForInterval.get());
    // repeat call should fail
    assertFalse(repeatingTask.getTaskForIntervalNumber(180).isPresent());
  }

  @Test
  public void getTaskForIntervalAtIntervalWithNoTaskEarly() {
    repeatingTask =
            new RepeatingTask(
                    runnable,
                    Duration.ofMinutes(1),
                    Duration.ofSeconds(10),
                    true,
                    RATE,
                    millisPerInterval,
                    scheduledTaskFactory,
                    rateLimitedScheduledExecutor,
                    clock,
                    logger);
    when(scheduledTaskFactory.getRepeatingScheduledTask(eq(runnable), any(Duration.class)))
            .thenReturn(scheduledRunnableTask);
    // after a minute
    Optional<ScheduledRunnableTask<Void>> taskForInterval =
            repeatingTask.getTaskForIntervalNumber(160);
    assertFalse(taskForInterval.isPresent());
    taskForInterval = repeatingTask.getTaskForIntervalNumber(161);
    assertTrue(taskForInterval.isPresent());
    assertEquals(scheduledRunnableTask, taskForInterval.get());
    // after 5 more seconds should be false
    assertFalse(repeatingTask.getTaskForIntervalNumber(166).isPresent());
    // after 5 more seconds should exist
    taskForInterval = repeatingTask.getTaskForIntervalNumber(171);
    assertTrue(taskForInterval.isPresent());
    assertEquals(scheduledRunnableTask, taskForInterval.get());
    taskForInterval = repeatingTask.getTaskForIntervalNumber(181);
    assertTrue(taskForInterval.isPresent());
    assertEquals(scheduledRunnableTask, taskForInterval.get());
    // repeat call should fail
    assertFalse(repeatingTask.getTaskForIntervalNumber(181).isPresent());
  }

  @Test
  public void getTaskForIntervalAtDelay() {
    repeatingTask =
            new RepeatingTask(
                    runnable,
                    Duration.ofMinutes(1),
                    Duration.ofSeconds(10),
                    false,
                    DELAY,
                    millisPerInterval,
                    scheduledTaskFactory,
                    rateLimitedScheduledExecutor,
                    clock,
                    logger);
    // after a minute
    Optional<ScheduledRunnableTask<Void>> taskForInterval =
            repeatingTask.getTaskForIntervalNumber(160);
    assertTrue(taskForInterval.isPresent());
    assertEquals(scheduledRunnableTask, taskForInterval.get());
    verify(scheduledRunnableTask).whenComplete(consumerCaptor.capture());
    // take 2 seconds to run
    when(clock.instant()).thenReturn(Instant.ofEpochSecond(162));
    // both values are ignored anyway
    consumerCaptor.getValue().accept(null, null);
    // after 5 more seconds should be false
    assertFalse(repeatingTask.getTaskForIntervalNumber(165).isPresent());
    // after 5 more seconds (+10 seconds total) should not exist
    assertFalse(repeatingTask.getTaskForIntervalNumber(170).isPresent());
    // should exist after 12 seconds (+10, +2)
    taskForInterval = repeatingTask.getTaskForIntervalNumber(172);
    assertTrue(taskForInterval.isPresent());
    assertEquals(scheduledRunnableTask, taskForInterval.get());

    verify(scheduledRunnableTask, times(2)).whenComplete(consumerCaptor.capture());
    // take 7 seconds to run
    when(clock.instant()).thenReturn(Instant.ofEpochSecond(179));
    // both values are ignored anyway
    consumerCaptor.getValue().accept(null, null);
    taskForInterval = repeatingTask.getTaskForIntervalNumber(189);
    assertTrue(taskForInterval.isPresent());
    assertEquals(scheduledRunnableTask, taskForInterval.get());
    verify(scheduledRunnableTask, times(3)).whenComplete(consumerCaptor.capture());
    // 0 seconds
    when(clock.instant()).thenReturn(Instant.ofEpochSecond(189));
    consumerCaptor.getValue().accept(null, null);
    // repeat call to the captor should log
    consumerCaptor.getValue().accept(null, null);
    verify(logger).warning("told we are no longer running when we are already no longer running");
    // repeat call should fail
    assertFalse(repeatingTask.getTaskForIntervalNumber(189).isPresent());
  }

  @Test
  public void getTaskForIntervalAtDelayWithNoTaskEarly() {
    repeatingTask =
            new RepeatingTask(
                    runnable,
                    Duration.ofMinutes(1),
                    Duration.ofSeconds(10),
                    true,
                    DELAY,
                    millisPerInterval,
                    scheduledTaskFactory,
                    rateLimitedScheduledExecutor,
                    clock,
                    logger);
    // after a minute
    Optional<ScheduledRunnableTask<Void>> taskForInterval =
            repeatingTask.getTaskForIntervalNumber(160);
    assertFalse(taskForInterval.isPresent());
    taskForInterval = repeatingTask.getTaskForIntervalNumber(161);
    assertTrue(taskForInterval.isPresent());
    assertEquals(scheduledRunnableTask, taskForInterval.get());
    verify(scheduledRunnableTask).whenComplete(consumerCaptor.capture());
    // take 2 seconds to run
    when(clock.instant()).thenReturn(Instant.ofEpochSecond(162));
    // both values are ignored anyway
    consumerCaptor.getValue().accept(null, null);
    // after 5 more seconds should be false
    assertFalse(repeatingTask.getTaskForIntervalNumber(165).isPresent());
    // after 5 more seconds (+10 seconds total) should not exist
    assertFalse(repeatingTask.getTaskForIntervalNumber(170).isPresent());
    // should exist after 12 seconds (+10, +2)
    taskForInterval = repeatingTask.getTaskForIntervalNumber(172);
    assertFalse(taskForInterval.isPresent());
    taskForInterval = repeatingTask.getTaskForIntervalNumber(173);
    assertTrue(taskForInterval.isPresent());
    assertEquals(scheduledRunnableTask, taskForInterval.get());

    verify(scheduledRunnableTask, times(2)).whenComplete(consumerCaptor.capture());
    // take 7 seconds to run
    when(clock.instant()).thenReturn(Instant.ofEpochSecond(179));
    // both values are ignored anyway
    consumerCaptor.getValue().accept(null, null);
    taskForInterval = repeatingTask.getTaskForIntervalNumber(189);
    assertFalse(taskForInterval.isPresent());
    taskForInterval = repeatingTask.getTaskForIntervalNumber(190);
    assertTrue(taskForInterval.isPresent());
    assertEquals(scheduledRunnableTask, taskForInterval.get());
    verify(scheduledRunnableTask, times(3)).whenComplete(consumerCaptor.capture());
    // 0 seconds
    when(clock.instant()).thenReturn(Instant.ofEpochSecond(190));
    consumerCaptor.getValue().accept(null, null);
    // repeat call to the captor should log
    consumerCaptor.getValue().accept(null, null);
    verify(logger).warning("told we are no longer running when we are already no longer running");
    // repeat call should fail
    assertFalse(repeatingTask.getTaskForIntervalNumber(190).isPresent());
  }

  @Test
  public void getDelayForIntervalTask() {
    assertEquals(60, repeatingTask.getDelay(SECONDS));
    when(clock.instant()).thenReturn(Instant.ofEpochSecond(159));
    assertEquals(1, repeatingTask.getDelay(SECONDS));
    when(clock.instant()).thenReturn(Instant.ofEpochSecond(160));
    Optional<ScheduledRunnableTask<Void>> taskForInterval =
            repeatingTask.getTaskForIntervalNumber(160);
    assertTrue(taskForInterval.isPresent());
    assertEquals(scheduledRunnableTask, taskForInterval.get());
    assertEquals(10, repeatingTask.getDelay(SECONDS));
  }

  @Test
  public void getDelayForDelayTask() {
    repeatingTask =
            new RepeatingTask(
                    runnable,
                    Duration.ofMinutes(1),
                    Duration.ofSeconds(10),
                    false,
                    DELAY,
                    millisPerInterval,
                    scheduledTaskFactory,
                    rateLimitedScheduledExecutor,
                    clock,
                    logger);

    assertEquals(60, repeatingTask.getDelay(SECONDS));
    when(clock.instant()).thenReturn(Instant.ofEpochSecond(159));
    assertEquals(1, repeatingTask.getDelay(SECONDS));
    Optional<ScheduledRunnableTask<Void>> taskForInterval =
            repeatingTask.getTaskForIntervalNumber(160);
    // a second later, the job has not completed
    assertTrue(taskForInterval.isPresent());
    assertEquals(scheduledRunnableTask, taskForInterval.get());
    when(clock.instant()).thenReturn(Instant.ofEpochSecond(161));
    assertEquals(-1, repeatingTask.getDelay(SECONDS));
    verify(scheduledRunnableTask).whenComplete(consumerCaptor.capture());
    // take 2 seconds to run
    when(clock.instant()).thenReturn(Instant.ofEpochSecond(162));
    // both values are ignored anyway
    consumerCaptor.getValue().accept(null, null);
    assertEquals(10, repeatingTask.getDelay(SECONDS));
  }

  @Test
  public void testCancellation() throws InterruptedException, ExecutionException {
    assertFalse(repeatingTask.isCancelled());
    assertFalse(repeatingTask.isDone());
    repeatingTask.cancel(true);
    assertTrue(repeatingTask.isCancelled());
    assertTrue(repeatingTask.isDone());
    assertFalse(repeatingTask.getTaskForIntervalNumber(160).isPresent());
    try {
      repeatingTask.get();
      fail();
    } catch (RuntimeException e) {
      assertEquals(CancellationException.class, e.getClass());
    }
  }

  @Test
  public void testGetWithTimeout() {
    try {
      repeatingTask.get(1, MILLISECONDS);
    } catch (Exception e) {
      assertEquals(TimeoutException.class, e.getClass());
    }
  }

  @Test
  public void testHashCodeAndEquals() {
    RepeatingTask repeatingTaskB =
            new RepeatingTask(
                    runnable,
                    Duration.ofMinutes(1),
                    Duration.ofSeconds(10),
                    false,
                    RATE,
                    millisPerInterval,
                    scheduledTaskFactory,
                    rateLimitedScheduledExecutor,
                    clock,
                    logger);
    assertEquals(repeatingTask.hashCode(), repeatingTaskB.hashCode());
    assertEquals(repeatingTask, repeatingTaskB);
    assertEquals(repeatingTask, repeatingTask);
    assertNotEquals(repeatingTask, null);
  }

  @Test
  public void testToString() {
    String toString = repeatingTask.toString();
    assertTrue(toString.contains("runnable=" + runnable));
    assertTrue(toString.contains("repeatingType=RATE"));
    assertTrue(toString.contains("isRunning=false"));
    assertTrue(toString.contains("nextAttemptMillis=160000"));
  }

  @Test
  public void testCompare() {
    RepeatingTask repeatingTaskIn10Seconds =
            new RepeatingTask(
                    runnable,
                    Duration.ofSeconds(10),
                    Duration.ofSeconds(10),
                    false,
                    DELAY,
                    millisPerInterval,
                    scheduledTaskFactory,
                    rateLimitedScheduledExecutor,
                    clock,
                    logger);
    RepeatingTask repeatingTaskIn15Seconds =
            new RepeatingTask(
                    runnable,
                    Duration.ofSeconds(15),
                    Duration.ofSeconds(10),
                    false,
                    DELAY,
                    millisPerInterval,
                    scheduledTaskFactory,
                    rateLimitedScheduledExecutor,
                    clock,
                    logger);
    RepeatingTask repeatingTaskIn20Seconds =
            new RepeatingTask(
                    runnable,
                    Duration.ofSeconds(20),
                    Duration.ofSeconds(10),
                    false,
                    DELAY,
                    millisPerInterval,
                    scheduledTaskFactory,
                    rateLimitedScheduledExecutor,
                    clock,
                    logger);

    List<RepeatingTask> tasks =
            new ArrayList<>(
                    List.of(repeatingTaskIn15Seconds, repeatingTaskIn20Seconds, repeatingTaskIn10Seconds));
    tasks.sort(RepeatingTask::compareTo);
    assertEquals(repeatingTaskIn10Seconds, tasks.get(0));
    assertEquals(repeatingTaskIn15Seconds, tasks.get(1));
    assertEquals(repeatingTaskIn20Seconds, tasks.get(2));
  }
}
