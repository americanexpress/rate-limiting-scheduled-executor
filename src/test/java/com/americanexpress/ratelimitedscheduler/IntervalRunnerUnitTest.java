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

import com.americanexpress.ratelimitedscheduler.task.ScheduledCallableTask;
import com.americanexpress.ratelimitedscheduler.task.ScheduledRunnableTask;
import com.americanexpress.ratelimitedscheduler.task.ScheduledTask;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class IntervalRunnerUnitTest {
  @Mock
  private ScheduledExecutorService scheduledExecutorService;
  @Mock
  private Clock clock;
  @Mock
  private Logger logger;
  @Mock
  private ScheduledRunnableTask taskA;
  @Mock
  private ScheduledRunnableTask taskB;
  @Mock
  private ScheduledCallableTask taskC;
  @Mock
  private ScheduledCallableTask taskD;

  private AtomicInteger runCounter;
  private IntervalRunner intervalRunner;
  @Mock
  private ConfiguredComponentSupplier configuredComponentSupplier;
  @Mock
  private ScheduledFuture scheduledFuture;
  @Mock
  private Executor executor;

  @Before
  public void setUp() {
    runCounter = new AtomicInteger();
    when(configuredComponentSupplier.getScheduledExecutorService()).thenReturn(scheduledExecutorService);
    when(configuredComponentSupplier.getExecutor()).thenReturn(executor);
    when(clock.instant()).thenReturn(Instant.ofEpochSecond(100));
    when(scheduledExecutorService.scheduleAtFixedRate(any(), anyLong(), anyLong(), any()))
            .thenReturn(scheduledFuture);
  }

  @Test
  public void testSubmitRecords() {
    List<ScheduledTask> tasks = List.of(taskA, taskB, taskC, taskD);
    when(taskA.isDone()).thenReturn(false);
    when(taskB.isDone()).thenReturn(true);
    when(taskC.isDone()).thenReturn(false);
    when(taskD.isDone()).thenReturn(true);
    intervalRunner = new IntervalRunner(101L, tasks, 1000, configuredComponentSupplier, clock, logger);
    CompletableFuture<Void> tasksCompletableFuture = intervalRunner.submitTasksToScheduler();
    ArgumentCaptor<Runnable> initialRunCaptor = ArgumentCaptor.forClass(Runnable.class);
    ArgumentCaptor<Runnable> repeatRunCaptor = ArgumentCaptor.forClass(Runnable.class);

    verify(scheduledExecutorService)
            .schedule(initialRunCaptor.capture(), eq(900_000_000L), eq(TimeUnit.NANOSECONDS));
    assertFalse(tasksCompletableFuture.isDone());
    Runnable initialTaskRunnable = initialRunCaptor.getValue();
    // as if we have gone forwards 900ms
    when(clock.instant()).thenReturn(Instant.ofEpochSecond(100).plusMillis(900));
    initialTaskRunnable.run();
    verify(scheduledExecutorService)
            .scheduleAtFixedRate(
                    repeatRunCaptor.capture(),
                    eq(100_000_000L),
                    eq(250_000_000L),
                    eq(TimeUnit.NANOSECONDS));
    assertFalse(tasksCompletableFuture.isDone());
    Runnable repeatTaskRunnable = repeatRunCaptor.getValue();

    assertEquals(0, runCounter.get());
    // run all the tasks
    repeatTaskRunnable.run();
    repeatTaskRunnable.run();
    repeatTaskRunnable.run();
    repeatTaskRunnable.run();
    verify(taskA).runAsync(executor);
    verify(taskB, times(0)).runAsync(executor);
    verify(taskC).runAsync(executor);
    verify(taskD, times(0)).runAsync(executor);

    verify(logger, times(0)).finest("completed all tasks so cancelling future");
    verify(logger).finest(String.format("task %s was cancelled so not running it", taskB));
    verify(logger).finest(String.format("task %s was cancelled so not running it", taskD));
    // once more should identify that everything is done
    repeatTaskRunnable.run();
    assertTrue(tasksCompletableFuture.isDone());
    verify(logger)
            .finest(
                    "completed all tasks so cancelling runNextTask future and completing our individual future");
    verify(scheduledFuture).cancel(false);
  }

  @Test
  public void testCompletedSubmitRecordsWithNoRecords() {
    List<ScheduledTask> tasks = List.of();
    intervalRunner = new IntervalRunner(101L, tasks, 1000, configuredComponentSupplier, clock, logger);
    CompletableFuture<Void> voidCompletableFuture = intervalRunner.submitTasksToScheduler();
    ArgumentCaptor<Runnable> initialRunCaptor = ArgumentCaptor.forClass(Runnable.class);

    verify(scheduledExecutorService)
            .schedule(initialRunCaptor.capture(), eq(900_000_000L), eq(TimeUnit.NANOSECONDS));
    initialRunCaptor.getValue().run();

    verify(scheduledExecutorService, times(0))
            .scheduleAtFixedRate(any(), anyLong(), anyLong(), any());
    assertTrue(voidCompletableFuture.isDone());
  }

  @Test
  public void testAddTaskHappyPath() {
    List<ScheduledTask> tasks = List.of(taskA);
    intervalRunner = new IntervalRunner(101L, tasks, 1000, configuredComponentSupplier, clock, logger);
    intervalRunner.addTask(taskB);
    assertEquals(2, intervalRunner.getTasks().size());
  }

  @Test
  public void testAddTaskAfterRun() {
    List<ScheduledTask> tasks = List.of();
    intervalRunner = new IntervalRunner(101L, tasks, 1000, configuredComponentSupplier, clock, logger);
    intervalRunner.submitTasksToScheduler();
    ArgumentCaptor<Runnable> initialRunCaptor = ArgumentCaptor.forClass(Runnable.class);
    ArgumentCaptor<Runnable> repeatRunCaptor = ArgumentCaptor.forClass(Runnable.class);

    verify(scheduledExecutorService)
            .schedule(initialRunCaptor.capture(), eq(900_000_000L), eq(TimeUnit.NANOSECONDS));

    Runnable initialTaskRunnable = initialRunCaptor.getValue();
    // as if we have gone forwards 900ms
    when(clock.instant()).thenReturn(Instant.ofEpochSecond(100).plusMillis(900));
    intervalRunner.addTask(taskB);
    initialTaskRunnable.run();
    // if we have 1_000_000_000 we only have one task
    verify(scheduledExecutorService)
            .scheduleAtFixedRate(
                    repeatRunCaptor.capture(),
                    eq(100_000_000L),
                    eq(1_000_000_000L),
                    eq(TimeUnit.NANOSECONDS));

    try {
      intervalRunner.addTask(taskB);
      fail();
    } catch (RejectedExecutionException e) {
      assertEquals("this interval runner is already running", e.getMessage());
    }
  }
}
