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

import com.americanexpress.ratelimitedscheduler.exceptions.CollectionHasBeenEmptiedException;

import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ScheduledRunnableTaskUnitTest {
  @Mock
  private Clock clock;
  private boolean hasRun = false;
  private boolean hasRun2 = false;
  private Runnable runnable;

  @Before
  public void setUp() {
    // should never run so
    runnable = TestCase::fail;
    when(clock.instant()).thenReturn(Instant.ofEpochSecond(100));
  }

  @Test
  public void testDelayIsCalculatedCorrectly() {
    ScheduledRunnableTask scheduledTask =
            new ScheduledRunnableTask(runnable, Duration.ofHours(1), false, clock);
    assertEquals(3700000L, scheduledTask.getScheduledTimeMillis());
    assertFalse(scheduledTask.isRepeating());
  }

  @Test
  public void testTaskIsStoredCorrectly() {
    ScheduledRunnableTask scheduledTask =
            new ScheduledRunnableTask(runnable, Duration.ofHours(1), true, clock);
    assertEquals(runnable, scheduledTask.getRunnable());
    assertTrue(scheduledTask.isRepeating());
  }

  @Test
  public void testCancelCompletesFuture() {
    ScheduledRunnableTask scheduledTask =
            new ScheduledRunnableTask(runnable, Duration.ofHours(1), false, clock);
    assertFalse(scheduledTask.isDone());
    scheduledTask.cancel(true);
    assertTrue(scheduledTask.isDone());
  }

  @Test
  public void testHashCodeAndEquals() {
    ScheduledRunnableTask scheduledTaskA =
            new ScheduledRunnableTask(runnable, Duration.ofHours(1), false, clock);
    ScheduledRunnableTask scheduledTaskB =
            new ScheduledRunnableTask(runnable, Duration.ofHours(1), false, clock);
    assertEquals(scheduledTaskA.hashCode(), scheduledTaskB.hashCode());
    assertEquals(scheduledTaskA, scheduledTaskB);
    assertEquals(scheduledTaskA, scheduledTaskA);
    assertNotEquals(scheduledTaskA, null);
  }

  @Test
  public void testRunAsyncWithNoReturnValueRunsProperly()
          throws ExecutionException, InterruptedException {
    ScheduledRunnableTask<Void> scheduledTask =
            new ScheduledRunnableTask<>(() -> hasRun = true, Duration.ofMillis(1), false, clock);
    scheduledTask.runAsync(ForkJoinPool.commonPool());
    assertNull(scheduledTask.get());
    assertTrue(hasRun);
  }

  @Test
  public void testRunAsyncWithReturnValueRunsProperly()
          throws ExecutionException, InterruptedException, TimeoutException {
    ScheduledRunnableTask<String> scheduledTask =
            new ScheduledRunnableTask<>(
                    () -> hasRun = true, "hello", Duration.ofMillis(1), false, clock);
    scheduledTask.runAsync(ForkJoinPool.commonPool());
    assertEquals("hello", scheduledTask.get(1, TimeUnit.SECONDS));
    assertTrue(hasRun);
  }

  @Test
  public void testRunAsyncWithExceptionIsHandled() throws InterruptedException {
    ScheduledRunnableTask<Void> scheduledTask =
            new ScheduledRunnableTask<>(
                    () -> {
                      throw new CollectionHasBeenEmptiedException();
                    },
                    Duration.ofMillis(1),
                    false,
                    clock);
    scheduledTask.runAsync(ForkJoinPool.commonPool());
    try {
      scheduledTask.get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(CollectionHasBeenEmptiedException.class, e.getCause().getClass());
      assertTrue(scheduledTask.isDone());
      assertTrue(scheduledTask.isCompletedExceptionally());
    }
  }

  @Test
  public void testToString() {
    ScheduledRunnableTask<String> scheduledTask =
            new ScheduledRunnableTask<>(runnable, "hello", Duration.ofMillis(1), false, clock);
    assertTrue(scheduledTask.toString().contains("runnable=" + runnable));
    assertTrue(scheduledTask.toString().contains("returnObject=hello"));
    assertTrue(scheduledTask.toString().contains("wasRequestedImmediately=false"));
    assertTrue(scheduledTask.toString().contains("isRepeating=false"));
  }

  @Test
  public void testNegativeScheduleTimeIsNormalised() {
    ScheduledRunnableTask<String> scheduledTask =
            new ScheduledRunnableTask<>(runnable, "hello", Duration.ofSeconds(-10), false, clock);
    assertEquals(100001L, scheduledTask.getScheduledTimeMillis());
    assertFalse(scheduledTask.wasRequestedImmediately());
  }

  @Test
  public void testZeroScheduleTimeIsDetectedAsImmediate() {
    ScheduledRunnableTask<String> scheduledTask =
            new ScheduledRunnableTask<>(runnable, "hello", Duration.ofSeconds(0), false, clock);
    assertEquals(100000L, scheduledTask.getScheduledTimeMillis());
    assertTrue(scheduledTask.wasRequestedImmediately());
  }

  @Test
  public void testTimeOut() throws InterruptedException {
    ScheduledRunnableTask<String> scheduledTask =
            new ScheduledRunnableTask<>(runnable, "hello", Duration.ofMillis(1), false, clock);
    scheduledTask.timeOut();
    try {
      scheduledTask.get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(TimeoutException.class, e.getCause().getClass());
      assertEquals(
              "java.util.concurrent.TimeoutException: this tasks was not executed prior to being timed out",
              e.getMessage());
      assertTrue(scheduledTask.isCompletedExceptionally());
      assertTrue(scheduledTask.isDone());
    }
  }

  @Test
  public void testWhenComplete() throws ExecutionException, InterruptedException {
    ScheduledRunnableTask<Void> scheduledTask =
            new ScheduledRunnableTask<>(() -> hasRun = true, Duration.ofMillis(1), false, clock);
    CompletableFuture<Void> doneFuture =
            scheduledTask.whenComplete((s, throwable) -> hasRun2 = true);
    scheduledTask.runAsync(ForkJoinPool.commonPool());
    doneFuture.get();
    assertTrue(hasRun2);
    assertTrue(hasRun);
  }

  @Test
  public void testDelay() {
    ScheduledRunnableTask<String> scheduledTask =
            new ScheduledRunnableTask<>(runnable, "hello", Duration.ofSeconds(10), false, clock);
    assertEquals(10, scheduledTask.getDelay(TimeUnit.SECONDS));
    when(clock.instant()).thenReturn(Instant.ofEpochSecond(105));
    assertEquals(5, scheduledTask.getDelay(TimeUnit.SECONDS));
  }

  @Test
  public void testCancelBeforeRun() throws ExecutionException, InterruptedException {
    ScheduledRunnableTask<Void> scheduledTask =
            new ScheduledRunnableTask<>(() -> hasRun = true, Duration.ofSeconds(10), false, clock);
    scheduledTask.cancel(true);
    assertTrue(scheduledTask.isCancelled());
    assertTrue(scheduledTask.isDone());
    assertTrue(scheduledTask.isCompletedExceptionally());
    try {
      scheduledTask.get();
      fail();
    } catch (RuntimeException e) {
      assertEquals(CancellationException.class, e.getClass());
      assertFalse(hasRun);
    }
  }

  @Test
  public void testSortOrder() {
    when(clock.instant()).thenReturn(Instant.ofEpochSecond(100));
    ScheduledRunnableTask<Void> scheduledTask10SecondsTime =
            new ScheduledRunnableTask<>(runnable, Duration.ofSeconds(10), false, clock);
    ScheduledRunnableTask<Void> scheduledTask5SecondsTime =
            new ScheduledRunnableTask<>(runnable, Duration.ofSeconds(5), false, clock);
    ScheduledRunnableTask<Void> scheduledTask11SecondsTime =
            new ScheduledRunnableTask<>(runnable, Duration.ofSeconds(11), false, clock);

    List<ScheduledRunnableTask<Void>> taskList =
            List.of(scheduledTask10SecondsTime, scheduledTask5SecondsTime, scheduledTask11SecondsTime);

    List<ScheduledRunnableTask<Void>> taskListReversed = new ArrayList<>(taskList);
    Collections.reverse(taskListReversed);

    List<ScheduledRunnableTask<Void>> sortedByLatest = new ArrayList<>(taskList);

    sortedByLatest.sort(ScheduledRunnableTask::compareTo);
    assertEquals(scheduledTask5SecondsTime, sortedByLatest.get(0));
    assertEquals(scheduledTask10SecondsTime, sortedByLatest.get(1));
    assertEquals(scheduledTask11SecondsTime, sortedByLatest.get(2));

    sortedByLatest.sort(ScheduledRunnableTask::compareTo);
    assertEquals(scheduledTask5SecondsTime, sortedByLatest.get(0));
    assertEquals(scheduledTask10SecondsTime, sortedByLatest.get(1));
    assertEquals(scheduledTask11SecondsTime, sortedByLatest.get(2));
  }
}
