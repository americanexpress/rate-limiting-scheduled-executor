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

import com.americanexpress.ratelimitedscheduler.exceptions.AllTasksFailedToExecuteException;
import com.americanexpress.ratelimitedscheduler.exceptions.RateLimitedScheduledExecutorHasTasksScheduledException;
import com.americanexpress.ratelimitedscheduler.task.RepeatingTask;
import com.americanexpress.ratelimitedscheduler.task.ScheduledCallableTask;
import com.americanexpress.ratelimitedscheduler.task.ScheduledRunnableTask;
import com.americanexpress.ratelimitedscheduler.task.ScheduledTask;
import com.americanexpress.ratelimitedscheduler.task.ScheduledTaskFactory;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.americanexpress.ratelimitedscheduler.task.TaskSorters.SORTED_BY_SCHEDULED_TIME_EARLIEST_FIRST_WITH_REPEATING_FIRST_THEN_IMMEDIATE_THEN_DELAYED;
import static com.americanexpress.ratelimitedscheduler.task.TaskSorters.SORTED_BY_SCHEDULED_TIME_LATEST_FIRST;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RateLimitedScheduledExecutorUnitTest {
  @Mock
  ScheduledTaskFactory scheduledTaskFactory;
  private RateLimitedScheduledExecutor rateLimitedScheduledExecutor;
  @Mock
  private Clock clock;
  @Mock
  private Logger logger = Logger.getLogger("testLogger");
  @Mock
  private ScheduledRunnableTask<Void> taskA;
  @Mock
  private ScheduledRunnableTask<Void> taskB;
  @Mock
  private ScheduledRunnableTask<Void> taskC;
  @Mock
  private ScheduledRunnableTask<Void> taskD;
  @Mock
  private ScheduledRunnableTask<Void> submittedTask;
  @Mock
  private ScheduledRunnableTask<Void> repeatedTaskInstance;
  @Mock
  private RepeatingTask repeatingTask;
  @Mock
  private ScheduledRunnableTask<String> stringTask;
  @Mock
  private ScheduledCallableTask<String> callableTask;
  @Mock
  private ScheduledCallableTask<String> callableSubmitTaskA;
  @Mock
  private ScheduledCallableTask<String> callableSubmitTaskB;
  @Mock
  private Runnable submittedRunnable;
  @Mock
  private Runnable runnableA;
  @Mock
  private Runnable runnableB;
  @Mock
  private Runnable runnableC;
  @Mock
  private Runnable runnableD;
  @Mock
  private Runnable stringRunnable;
  @Mock
  private Runnable repeatRunnable;
  @Mock
  private Callable<String> callableA;
  @Mock
  private Callable<String> callableB;
  @Mock
  private LockableListFactory lockableListFactory;
  private boolean factoryReturnsEmptiedSets = false;
  private boolean factoryFiresException = false;
  @Mock
  private RateLimitedScheduledExecutorManager rateLimitedScheduledExecutorManager;

  @Before
  public void setUp() {
    when(lockableListFactory.getLockableList())
            .thenAnswer(
                    (Answer<LockableList>)
                            invocationOnMock -> {
                              if (factoryFiresException) {
                                throw new RuntimeException("something broke");
                              }
                              LockableList lockableList = new LockableList();
                              if (factoryReturnsEmptiedSets) {
                                lockableList.empty();
                              }
                              return lockableList;
                            });

    when(taskA.getScheduledTimeMillis()).thenReturn(105000L);
    when(callableTask.getScheduledTimeMillis()).thenReturn(105000L);
    when(taskB.getScheduledTimeMillis()).thenReturn(110000L);
    when(stringTask.getScheduledTimeMillis()).thenReturn(100000L);
    when(callableSubmitTaskA.getScheduledTimeMillis()).thenReturn(100000L);
    when(clock.instant()).thenReturn(Instant.ofEpochSecond(100));
    rateLimitedScheduledExecutor =
            new RateLimitedScheduledExecutor(
                    "tag",
                    false,
                    1,
                    1000,
                    scheduledTaskFactory,
                    lockableListFactory,
                    rateLimitedScheduledExecutorManager,
                    logger,
                    clock);
    when(scheduledTaskFactory.getScheduledTask(eq(submittedRunnable), eq(Duration.ZERO)))
            .thenReturn(submittedTask);
    when(scheduledTaskFactory.getScheduledTask(eq(runnableA), any(Duration.class)))
            .thenReturn(taskA);
    when(scheduledTaskFactory.getScheduledTask(eq(runnableB), any(Duration.class)))
            .thenReturn(taskB);
    when(scheduledTaskFactory.getScheduledTask(eq(runnableC), any(Duration.class)))
            .thenReturn(taskC);
    when(scheduledTaskFactory.getScheduledTask(eq(runnableD), any(Duration.class)))
            .thenReturn(taskD);
    when(scheduledTaskFactory.getRepeatingTask(
            eq(repeatRunnable),
            any(Duration.class),
            any(Duration.class),
            anyBoolean(),
            any(RepeatingTask.RepeatingType.class),
            eq(rateLimitedScheduledExecutor)))
            .thenReturn(repeatingTask);

    when(taskA.getRunnable()).thenReturn(runnableA);
    when(scheduledTaskFactory.getScheduledTask(eq(callableA), eq(Duration.ofSeconds(5))))
            .thenReturn(callableTask);
    when(scheduledTaskFactory.getScheduledTask(eq(callableA), eq(Duration.ZERO)))
            .thenReturn(callableSubmitTaskA);
    when(scheduledTaskFactory.getScheduledTask(eq(callableB), eq(Duration.ZERO)))
            .thenReturn(callableSubmitTaskB);
    when(scheduledTaskFactory.getScheduledTask(
            eq(stringRunnable), eq("hello"), any(Duration.class)))
            .thenReturn(stringTask);
  }

  @Test
  public void testInitialisedProperly() {
    assertEquals("tag", rateLimitedScheduledExecutor.getServiceTag());
    assertEquals(scheduledTaskFactory, rateLimitedScheduledExecutor.getScheduledTaskFactory());
    assertEquals(logger, rateLimitedScheduledExecutor.getLogger());
    assertEquals(95, rateLimitedScheduledExecutor.getLastProcessedInterval());
    assertEquals(Double.POSITIVE_INFINITY, rateLimitedScheduledExecutor.getMaxTPS(), 0);
    assertEquals(Duration.ZERO, rateLimitedScheduledExecutor.getTaskTimeout());
    assertTrue(rateLimitedScheduledExecutor.getScheduledTasks().isEmpty());
    assertTrue(rateLimitedScheduledExecutor.getOverflowBucket().isEmpty());
    assertTrue(rateLimitedScheduledExecutor.getRepeatingTasks().isEmpty());
    assertEquals(
            SORTED_BY_SCHEDULED_TIME_EARLIEST_FIRST_WITH_REPEATING_FIRST_THEN_IMMEDIATE_THEN_DELAYED,
            rateLimitedScheduledExecutor.getSortMethod());
    assertEquals(1000, rateLimitedScheduledExecutor.getMillisPerInterval());
    assertEquals(0, rateLimitedScheduledExecutor.getNumberOfTasksAwaitingExecution());
    assertFalse(rateLimitedScheduledExecutor.isEnsureNoTaskDispatchedEarly());
  }

  @Test
  public void testScheduleTaskHappyPath() {
    assertEquals(0, rateLimitedScheduledExecutor.getScheduledTasks().size());
    assertEquals(taskA, rateLimitedScheduledExecutor.schedule(runnableA, 5, SECONDS));
    verify(scheduledTaskFactory).getScheduledTask(runnableA, Duration.ofSeconds(5));
    assertEquals(1, rateLimitedScheduledExecutor.getScheduledTasks().size());
    assertEquals(0, rateLimitedScheduledExecutor.getBacklogSize(), 0);
    assertEquals(1, rateLimitedScheduledExecutor.getScheduledTasks().get(105L).size());
    assertTrue(rateLimitedScheduledExecutor.getScheduledTasks().get(105L).contains(taskA));
    assertEquals(1, rateLimitedScheduledExecutor.getNumberOfTasksAwaitingExecution());
    assertTrue(rateLimitedScheduledExecutor.getTasksForIntervalNumber(105).contains(taskA));
  }

  @Test
  public void testScheduleTaskWithNoEarlyTasks()
          throws RateLimitedScheduledExecutorHasTasksScheduledException {
    rateLimitedScheduledExecutor.setEnsureNoTaskDispatchedEarly(true);
    assertEquals(0, rateLimitedScheduledExecutor.getScheduledTasks().size());
    assertEquals(taskA, rateLimitedScheduledExecutor.schedule(runnableA, 5, SECONDS));
    verify(scheduledTaskFactory).getScheduledTask(runnableA, Duration.ofSeconds(5));
    assertEquals(1, rateLimitedScheduledExecutor.getScheduledTasks().size());
    assertEquals(0, rateLimitedScheduledExecutor.getBacklogSize(), 0);
    assertEquals(1, rateLimitedScheduledExecutor.getScheduledTasks().get(106L).size());
    assertTrue(rateLimitedScheduledExecutor.getScheduledTasks().get(106L).contains(taskA));
    assertEquals(1, rateLimitedScheduledExecutor.getNumberOfTasksAwaitingExecution());
    assertFalse(rateLimitedScheduledExecutor.getTasksForIntervalNumber(105).contains(taskA));
    assertEquals(1, rateLimitedScheduledExecutor.getNumberOfTasksAwaitingExecution());
    assertTrue(rateLimitedScheduledExecutor.getTasksForIntervalNumber(106).contains(taskA));
    assertEquals(0, rateLimitedScheduledExecutor.getNumberOfTasksAwaitingExecution());
    assertTrue(rateLimitedScheduledExecutor.isEnsureNoTaskDispatchedEarly());
  }

  @Test
  public void testSubmitTaskHappyPath() {
    rateLimitedScheduledExecutor.getTasksForIntervalNumber(100);
    assertEquals(0, rateLimitedScheduledExecutor.getScheduledTasks().size());
    assertEquals(submittedTask, rateLimitedScheduledExecutor.submit(submittedRunnable));
    verify(scheduledTaskFactory).getScheduledTask(submittedRunnable, Duration.ZERO);
    // shoudl go straight to the overflow
    assertEquals(0, rateLimitedScheduledExecutor.getBacklogSize(), 0);
    assertEquals(1, rateLimitedScheduledExecutor.getOverflowBucket().size());
    assertTrue(rateLimitedScheduledExecutor.getOverflowBucket().contains(submittedTask));
    assertEquals(1, rateLimitedScheduledExecutor.getNumberOfTasksAwaitingExecution());
    assertTrue(rateLimitedScheduledExecutor.getTasksForIntervalNumber(101).contains(submittedTask));
    assertEquals(0, rateLimitedScheduledExecutor.getNumberOfTasksAwaitingExecution());
  }

  @Test
  public void testExecuteTaskHappyPath() {
    rateLimitedScheduledExecutor.getTasksForIntervalNumber(100);
    assertEquals(0, rateLimitedScheduledExecutor.getScheduledTasks().size());
    rateLimitedScheduledExecutor.execute(submittedRunnable);
    verify(scheduledTaskFactory).getScheduledTask(submittedRunnable, Duration.ZERO);
    // shoudl go straight to the overflow
    assertEquals(1, rateLimitedScheduledExecutor.getOverflowBucket().size());
    assertTrue(rateLimitedScheduledExecutor.getOverflowBucket().contains(submittedTask));
    assertEquals(1, rateLimitedScheduledExecutor.getNumberOfTasksAwaitingExecution());
    assertTrue(rateLimitedScheduledExecutor.getTasksForIntervalNumber(101).contains(submittedTask));
    assertEquals(0, rateLimitedScheduledExecutor.getNumberOfTasksAwaitingExecution());
  }

  @Test
  public void testSubmitTaskWithReturnObjectHappyPath() {
    rateLimitedScheduledExecutor.getTasksForIntervalNumber(100);
    assertEquals(0, rateLimitedScheduledExecutor.getScheduledTasks().size());
    assertEquals(stringTask, rateLimitedScheduledExecutor.submit(stringRunnable, "hello"));
    verify(scheduledTaskFactory).getScheduledTask(stringRunnable, "hello", Duration.ZERO);
    // shoudl go straight to the overflow
    assertEquals(1, rateLimitedScheduledExecutor.getOverflowBucket().size());
    assertTrue(rateLimitedScheduledExecutor.getOverflowBucket().contains(stringTask));
    assertEquals(1, rateLimitedScheduledExecutor.getNumberOfTasksAwaitingExecution());
    assertTrue(rateLimitedScheduledExecutor.getTasksForIntervalNumber(101).contains(stringTask));
    assertEquals(0, rateLimitedScheduledExecutor.getNumberOfTasksAwaitingExecution());
  }

  @Test
  public void testScheduleCallableHappyPath() {
    assertEquals(0, rateLimitedScheduledExecutor.getScheduledTasks().size());
    assertEquals(callableTask, rateLimitedScheduledExecutor.schedule(callableA, 5, SECONDS));
    verify(scheduledTaskFactory).getScheduledTask(callableA, Duration.ofSeconds(5));
    assertEquals(1, rateLimitedScheduledExecutor.getScheduledTasks().size());
    assertEquals(1, rateLimitedScheduledExecutor.getNumberOfTasksAwaitingExecution());
    assertEquals(1, rateLimitedScheduledExecutor.getScheduledTasks().get(105L).size());
    assertTrue(rateLimitedScheduledExecutor.getScheduledTasks().get(105L).contains(callableTask));
  }

  @Test
  public void testSubmitCallableHappyPath() {
    rateLimitedScheduledExecutor.getTasksForIntervalNumber(100);
    assertEquals(0, rateLimitedScheduledExecutor.getScheduledTasks().size());
    assertEquals(callableSubmitTaskA, rateLimitedScheduledExecutor.submit(callableA));
    verify(scheduledTaskFactory).getScheduledTask(callableA, Duration.ZERO);
    // shoudl go straight to the overflow
    assertEquals(1, rateLimitedScheduledExecutor.getOverflowBucket().size());
    assertEquals(1, rateLimitedScheduledExecutor.getNumberOfTasksAwaitingExecution());
    assertTrue(
            rateLimitedScheduledExecutor.getTasksForIntervalNumber(101).contains(callableSubmitTaskA));
    assertEquals(0, rateLimitedScheduledExecutor.getNumberOfTasksAwaitingExecution());
  }

  @Test
  public void testInvokeAllCallableHappyPath() {
    rateLimitedScheduledExecutor.getTasksForIntervalNumber(100);
    assertEquals(0, rateLimitedScheduledExecutor.getScheduledTasks().size());
    List<Future<String>> futures =
            rateLimitedScheduledExecutor.invokeAll(List.of(callableA, callableB));
    assertTrue(futures.contains(callableSubmitTaskA));
    assertTrue(futures.contains(callableSubmitTaskB));
    assertEquals(2, futures.size());
    verify(scheduledTaskFactory).getScheduledTask(callableA, Duration.ZERO);
    verify(scheduledTaskFactory).getScheduledTask(callableB, Duration.ZERO);
    // shoudl go straight to the overflow
    assertEquals(2, rateLimitedScheduledExecutor.getOverflowBucket().size());
    assertEquals(2, rateLimitedScheduledExecutor.getNumberOfTasksAwaitingExecution());
    List<ScheduledTask> tasksForInterval101 =
            rateLimitedScheduledExecutor.getTasksForIntervalNumber(101);
    assertTrue(tasksForInterval101.contains(callableSubmitTaskA));
    assertTrue(tasksForInterval101.contains(callableSubmitTaskB));
    assertEquals(0, rateLimitedScheduledExecutor.getNumberOfTasksAwaitingExecution());
  }

  @Test
  public void testInvokeAllWithTimeoutCallableHappyPath()
          throws ExecutionException, InterruptedException {
    rateLimitedScheduledExecutor.getTasksForIntervalNumber(100);
    assertEquals(0, rateLimitedScheduledExecutor.getScheduledTasks().size());
    CompletableFuture<List<Future<String>>> completableFuture =
            CompletableFuture.supplyAsync(
                    () -> {
                      try {
                        return rateLimitedScheduledExecutor.invokeAll(
                                List.of(callableA, callableB), 1, SECONDS);
                      } catch (InterruptedException e) {
                        fail();
                        return Collections.emptyList();
                      }
                    });
    // let the thread start running
    Thread.sleep(1000);
    verify(scheduledTaskFactory).getScheduledTask(callableA, Duration.ZERO);
    verify(scheduledTaskFactory).getScheduledTask(callableB, Duration.ZERO);
    assertEquals(2, rateLimitedScheduledExecutor.getOverflowBucket().size());
    List<ScheduledTask> tasksForInterval101 =
            rateLimitedScheduledExecutor.getTasksForIntervalNumber(101);
    assertTrue(tasksForInterval101.contains(callableSubmitTaskA));
    assertTrue(tasksForInterval101.contains(callableSubmitTaskB));
    when(callableSubmitTaskA.isDone()).thenReturn(true);
    when(callableSubmitTaskB.isDone()).thenReturn(true);
    List<Future<String>> futures = completableFuture.get();
    assertTrue(futures.contains(callableSubmitTaskA));
    assertTrue(futures.contains(callableSubmitTaskB));
    assertEquals(2, futures.size());
  }

  @Test
  public void testInvokeAllWithTimeoutCallableRanOutOfTime()
          throws ExecutionException, InterruptedException {
    rateLimitedScheduledExecutor.getTasksForIntervalNumber(100);
    assertEquals(0, rateLimitedScheduledExecutor.getScheduledTasks().size());
    CompletableFuture<List<Future<String>>> completableFuture =
            CompletableFuture.supplyAsync(
                    () -> {
                      try {
                        return rateLimitedScheduledExecutor.invokeAll(
                                List.of(callableA, callableB), 1, SECONDS);
                      } catch (InterruptedException e) {
                        fail();
                        return Collections.emptyList();
                      }
                    });
    // let the thread start running
    Thread.sleep(1000);
    verify(scheduledTaskFactory).getScheduledTask(callableA, Duration.ZERO);
    verify(scheduledTaskFactory).getScheduledTask(callableB, Duration.ZERO);
    assertEquals(2, rateLimitedScheduledExecutor.getOverflowBucket().size());
    List<ScheduledTask> tasksForInterval101 =
            rateLimitedScheduledExecutor.getTasksForIntervalNumber(101);
    assertTrue(tasksForInterval101.contains(callableSubmitTaskA));
    assertTrue(tasksForInterval101.contains(callableSubmitTaskB));
    // only one of them completes
    lenient().when(callableSubmitTaskA.isDone()).thenReturn(true);
    // bump the clock forwards
    lenient().when(clock.instant()).thenReturn(Instant.ofEpochSecond(102));
    List<Future<String>> futures = completableFuture.get();
    assertTrue(futures.contains(callableSubmitTaskA));
    assertTrue(futures.contains(callableSubmitTaskB));
    assertEquals(2, futures.size());
  }

  @Test
  public void testInvokeAnyWithTimeoutCallableRanOutOfTime()
          throws ExecutionException, InterruptedException {
    rateLimitedScheduledExecutor.getTasksForIntervalNumber(100);
    assertEquals(0, rateLimitedScheduledExecutor.getScheduledTasks().size());
    CompletableFuture<String> completableFuture =
            CompletableFuture.supplyAsync(
                    () -> {
                      try {
                        return rateLimitedScheduledExecutor.invokeAny(
                                List.of(callableA, callableB), 1, SECONDS);
                      } catch (TimeoutException e) {
                        return "timeout";
                      } catch (InterruptedException | ExecutionException e) {
                        fail();
                        return "fail";
                      }
                    });
    // let the thread start running
    Thread.sleep(1000);
    verify(scheduledTaskFactory).getScheduledTask(callableA, Duration.ZERO);
    verify(scheduledTaskFactory).getScheduledTask(callableB, Duration.ZERO);
    assertEquals(2, rateLimitedScheduledExecutor.getOverflowBucket().size());
    List<ScheduledTask> tasksForInterval101 =
            rateLimitedScheduledExecutor.getTasksForIntervalNumber(101);
    assertTrue(tasksForInterval101.contains(callableSubmitTaskA));
    assertTrue(tasksForInterval101.contains(callableSubmitTaskB));
    // bump the clock forwards
    lenient().when(clock.instant()).thenReturn(Instant.ofEpochSecond(102));
    // check we were timed out
    assertEquals("timeout", completableFuture.get());
  }

  @Test
  public void testInvokeAnyWithTimeoutCallableHappyPath()
          throws ExecutionException, InterruptedException {
    rateLimitedScheduledExecutor.getTasksForIntervalNumber(100);
    assertEquals(0, rateLimitedScheduledExecutor.getScheduledTasks().size());
    CompletableFuture<String> completableFuture =
            CompletableFuture.supplyAsync(
                    () -> {
                      try {
                        return rateLimitedScheduledExecutor.invokeAny(
                                List.of(callableA, callableB), 1, SECONDS);
                      } catch (TimeoutException e) {
                        return "timeout";
                      } catch (InterruptedException | ExecutionException e) {
                        fail();
                        return "fail";
                      }
                    });
    // let the thread start running
    Thread.sleep(1000);

    verify(scheduledTaskFactory).getScheduledTask(callableA, Duration.ZERO);
    verify(scheduledTaskFactory).getScheduledTask(callableB, Duration.ZERO);
    assertEquals(2, rateLimitedScheduledExecutor.getOverflowBucket().size());
    List<ScheduledTask> tasksForInterval101 =
            rateLimitedScheduledExecutor.getTasksForIntervalNumber(101);
    assertTrue(tasksForInterval101.contains(callableSubmitTaskA));
    assertTrue(tasksForInterval101.contains(callableSubmitTaskB));
    // bump the clock forwards
    when(callableSubmitTaskA.isDone()).thenReturn(true);
    when(callableSubmitTaskA.get()).thenReturn("Afinished");
    // check we were timed out
    assertEquals("Afinished", completableFuture.get());
  }

  @Test
  public void testInvokeAnyWithoutTimeoutCallableHappyPath()
          throws ExecutionException, InterruptedException {
    rateLimitedScheduledExecutor.getTasksForIntervalNumber(100);
    assertEquals(0, rateLimitedScheduledExecutor.getScheduledTasks().size());
    CompletableFuture<String> completableFuture =
            CompletableFuture.supplyAsync(
                    () -> {
                      try {
                        return rateLimitedScheduledExecutor.invokeAny(List.of(callableA, callableB));
                      } catch (InterruptedException | ExecutionException e) {
                        fail();
                        return "fail";
                      }
                    });
    // let the thread start running
    Thread.sleep(1000);

    verify(scheduledTaskFactory).getScheduledTask(callableA, Duration.ZERO);
    verify(scheduledTaskFactory).getScheduledTask(callableB, Duration.ZERO);
    assertEquals(2, rateLimitedScheduledExecutor.getOverflowBucket().size());
    List<ScheduledTask> tasksForInterval101 =
            rateLimitedScheduledExecutor.getTasksForIntervalNumber(101);
    assertTrue(tasksForInterval101.contains(callableSubmitTaskA));
    assertTrue(tasksForInterval101.contains(callableSubmitTaskB));

    when(callableSubmitTaskB.isDone()).thenReturn(true);
    when(callableSubmitTaskB.get()).thenReturn("Bfinished");
    // check we were timed out
    assertEquals("Bfinished", completableFuture.get());
  }

  @Test
  public void testInvokeAnyWithoutTimeoutCallableAllTasksFail()
          throws ExecutionException, InterruptedException {
    rateLimitedScheduledExecutor.getTasksForIntervalNumber(100);
    assertEquals(0, rateLimitedScheduledExecutor.getScheduledTasks().size());
    CompletableFuture<String> completableFuture =
            CompletableFuture.supplyAsync(
                    () -> {
                      try {
                        return rateLimitedScheduledExecutor.invokeAny(List.of(callableA, callableB));
                      } catch (InterruptedException e) {
                        fail();
                        return "fail";
                      } catch (ExecutionException e) {
                        assertEquals(AllTasksFailedToExecuteException.class, e.getCause().getClass());
                        return "executionException";
                      }
                    });
    // let the thread start running
    Thread.sleep(2000);

    verify(scheduledTaskFactory).getScheduledTask(callableA, Duration.ZERO);
    verify(scheduledTaskFactory).getScheduledTask(callableB, Duration.ZERO);
    assertEquals(2, rateLimitedScheduledExecutor.getOverflowBucket().size());
    List<ScheduledTask> tasksForInterval101 =
            rateLimitedScheduledExecutor.getTasksForIntervalNumber(101);
    assertTrue(tasksForInterval101.contains(callableSubmitTaskA));
    assertTrue(tasksForInterval101.contains(callableSubmitTaskB));
    when(callableSubmitTaskB.get()).thenThrow(new RuntimeException("bad file"));
    when(callableSubmitTaskA.get()).thenThrow(new RuntimeException("bad file"));
    when(callableSubmitTaskA.isDone()).thenReturn(true);
    when(callableSubmitTaskB.isDone()).thenReturn(true);


    // check we were timed out
    assertEquals("executionException", completableFuture.get());

    verify(logger, times(2))
            .log(eq(Level.INFO), eq("got an exception during invokeAll"), any(Exception.class));
  }

  @Test
  public void testInvokeAnyWithoutTimeoutTakesOver100Years()
          throws ExecutionException, InterruptedException {
    rateLimitedScheduledExecutor.getTasksForIntervalNumber(100);
    assertEquals(0, rateLimitedScheduledExecutor.getScheduledTasks().size());
    CompletableFuture<String> completableFuture =
            CompletableFuture.supplyAsync(
                    () -> {
                      try {
                        return rateLimitedScheduledExecutor.invokeAny(List.of(callableA, callableB));
                      } catch (InterruptedException e) {
                        fail();
                        return "fail";
                      } catch (ExecutionException e) {
                        assertEquals(TimeoutException.class, e.getCause().getClass());
                        return "timeoutException";
                      }
                    });
    // let the thread start running
    Thread.sleep(1000);

    verify(scheduledTaskFactory).getScheduledTask(callableA, Duration.ZERO);
    verify(scheduledTaskFactory).getScheduledTask(callableB, Duration.ZERO);
    assertEquals(2, rateLimitedScheduledExecutor.getOverflowBucket().size());
    List<ScheduledTask> tasksForInterval101 =
            rateLimitedScheduledExecutor.getTasksForIntervalNumber(101);
    assertTrue(tasksForInterval101.contains(callableSubmitTaskA));
    assertTrue(tasksForInterval101.contains(callableSubmitTaskB));
    // bump the clock 100 years
    when(clock.instant()).thenReturn(Instant.MAX);
    // check we were timed out
    assertEquals("timeoutException", completableFuture.get());
  }

  @Test
  public void testAddTaskWhenShutDown() {
    rateLimitedScheduledExecutor.shutdown();
    assertEquals(0, rateLimitedScheduledExecutor.getScheduledTasks().size());
    try {
      rateLimitedScheduledExecutor.schedule(runnableA, 5, SECONDS);
      fail();
    } catch (RejectedExecutionException e) {
      assertEquals(0, rateLimitedScheduledExecutor.getScheduledTasks().size());
    }
  }

  @Test
  public void testAddCallableWhenShutDown() {
    rateLimitedScheduledExecutor.shutdown();
    assertEquals(0, rateLimitedScheduledExecutor.getScheduledTasks().size());
    try {
      rateLimitedScheduledExecutor.schedule(callableA, 5, SECONDS);
      fail();
    } catch (RejectedExecutionException e) {
      assertEquals(0, rateLimitedScheduledExecutor.getScheduledTasks().size());
    }
  }

  @Test
  public void testScheduleRateAfterShutdown() {
    rateLimitedScheduledExecutor.shutdown();
    assertEquals(0, rateLimitedScheduledExecutor.getRepeatingTasks().size());
    try {
      rateLimitedScheduledExecutor.scheduleAtFixedRate(repeatRunnable, 5, 5, SECONDS);
      fail();
    } catch (RejectedExecutionException e) {
      assertEquals(0, rateLimitedScheduledExecutor.getRepeatingTasks().size());
    }
  }

  @Test
  public void testScheduleDelayAfterShutdown() {
    rateLimitedScheduledExecutor.shutdown();
    assertEquals(0, rateLimitedScheduledExecutor.getRepeatingTasks().size());
    try {
      rateLimitedScheduledExecutor.scheduleWithFixedDelay(repeatRunnable, 5, 5, SECONDS);
      fail();
    } catch (RejectedExecutionException e) {
      assertEquals(0, rateLimitedScheduledExecutor.getRepeatingTasks().size());
    }
  }

  @Test
  public void testAddAndRemoveRepeatingRateTaskHappyPath() {
    when(repeatingTask.getTaskForIntervalNumber(105)).thenReturn(Optional.of(repeatedTaskInstance));
    when(repeatingTask.getTaskForIntervalNumber(106)).thenReturn(Optional.empty());
    assertEquals(0, rateLimitedScheduledExecutor.getRepeatingTasks().size());
    assertEquals(
            repeatingTask,
            rateLimitedScheduledExecutor.scheduleAtFixedRate(repeatRunnable, 0, 5, SECONDS));
    assertEquals(1, rateLimitedScheduledExecutor.getRepeatingTasks().size());
    assertTrue(rateLimitedScheduledExecutor.getRepeatingTasks().contains(repeatingTask));
    List<ScheduledTask> tasksForInterval105 =
            rateLimitedScheduledExecutor.getTasksForIntervalNumber(105);
    assertEquals(1, tasksForInterval105.size());
    assertTrue(tasksForInterval105.contains(repeatedTaskInstance));
    assertTrue(rateLimitedScheduledExecutor.getTasksForIntervalNumber(106).isEmpty());
    rateLimitedScheduledExecutor.removeRepeatingTask(repeatingTask);
    assertTrue(rateLimitedScheduledExecutor.getRepeatingTasks().isEmpty());
    verify(scheduledTaskFactory)
            .getRepeatingTask(
                    repeatRunnable,
                    Duration.ZERO,
                    Duration.ofSeconds(5),
                    false,
                    RepeatingTask.RepeatingType.RATE,
                    rateLimitedScheduledExecutor);
  }

  @Test
  public void testAddAndRemoveRepeatingRateWithNoTasksEarly()
          throws RateLimitedScheduledExecutorHasTasksScheduledException {
    rateLimitedScheduledExecutor.setEnsureNoTaskDispatchedEarly(true);
    when(repeatingTask.getTaskForIntervalNumber(105)).thenReturn(Optional.of(repeatedTaskInstance));
    when(repeatingTask.getTaskForIntervalNumber(106)).thenReturn(Optional.empty());
    assertEquals(0, rateLimitedScheduledExecutor.getRepeatingTasks().size());
    assertEquals(
            repeatingTask,
            rateLimitedScheduledExecutor.scheduleAtFixedRate(repeatRunnable, 0, 5, SECONDS));
    assertEquals(1, rateLimitedScheduledExecutor.getRepeatingTasks().size());
    assertTrue(rateLimitedScheduledExecutor.getRepeatingTasks().contains(repeatingTask));
    List<ScheduledTask> tasksForInterval105 =
            rateLimitedScheduledExecutor.getTasksForIntervalNumber(105);
    assertEquals(1, tasksForInterval105.size());
    assertTrue(tasksForInterval105.contains(repeatedTaskInstance));
    assertTrue(rateLimitedScheduledExecutor.getTasksForIntervalNumber(106).isEmpty());
    rateLimitedScheduledExecutor.removeRepeatingTask(repeatingTask);
    assertTrue(rateLimitedScheduledExecutor.getRepeatingTasks().isEmpty());
    verify(scheduledTaskFactory)
            .getRepeatingTask(
                    repeatRunnable,
                    Duration.ZERO,
                    Duration.ofSeconds(5),
                    true,
                    RepeatingTask.RepeatingType.RATE,
                    rateLimitedScheduledExecutor);
  }

  @Test
  public void testAddRepeatingDelayTaskHappyPath() {
    when(repeatingTask.getTaskForIntervalNumber(105)).thenReturn(Optional.of(repeatedTaskInstance));
    when(repeatingTask.getTaskForIntervalNumber(106)).thenReturn(Optional.empty());
    assertEquals(0, rateLimitedScheduledExecutor.getRepeatingTasks().size());
    assertEquals(
            repeatingTask,
            rateLimitedScheduledExecutor.scheduleWithFixedDelay(repeatRunnable, 0, 5, SECONDS));
    assertEquals(1, rateLimitedScheduledExecutor.getRepeatingTasks().size());
    List<ScheduledTask> tasksForInterval105 =
            rateLimitedScheduledExecutor.getTasksForIntervalNumber(105);
    assertEquals(1, tasksForInterval105.size());
    assertTrue(tasksForInterval105.contains(repeatedTaskInstance));
    assertTrue(rateLimitedScheduledExecutor.getTasksForIntervalNumber(106).isEmpty());
    verify(scheduledTaskFactory)
            .getRepeatingTask(
                    repeatRunnable,
                    Duration.ZERO,
                    Duration.ofSeconds(5),
                    false,
                    RepeatingTask.RepeatingType.DELAY,
                    rateLimitedScheduledExecutor);
  }

  @Test
  public void testAddRepeatingDelayWithNoTasksEarly()
          throws RateLimitedScheduledExecutorHasTasksScheduledException {
    rateLimitedScheduledExecutor.setEnsureNoTaskDispatchedEarly(true);
    when(repeatingTask.getTaskForIntervalNumber(105)).thenReturn(Optional.of(repeatedTaskInstance));
    when(repeatingTask.getTaskForIntervalNumber(106)).thenReturn(Optional.empty());
    assertEquals(0, rateLimitedScheduledExecutor.getRepeatingTasks().size());
    assertEquals(
            repeatingTask,
            rateLimitedScheduledExecutor.scheduleWithFixedDelay(repeatRunnable, 0, 5, SECONDS));
    assertEquals(1, rateLimitedScheduledExecutor.getRepeatingTasks().size());
    List<ScheduledTask> tasksForInterval105 =
            rateLimitedScheduledExecutor.getTasksForIntervalNumber(105);
    assertEquals(1, tasksForInterval105.size());
    assertTrue(tasksForInterval105.contains(repeatedTaskInstance));
    assertTrue(rateLimitedScheduledExecutor.getTasksForIntervalNumber(106).isEmpty());
    verify(scheduledTaskFactory)
            .getRepeatingTask(
                    repeatRunnable,
                    Duration.ZERO,
                    Duration.ofSeconds(5),
                    true,
                    RepeatingTask.RepeatingType.DELAY,
                    rateLimitedScheduledExecutor);
  }

  @Test
  public void testShutDown() {
    when(taskB.isDone()).thenReturn(true);
    assertEquals(0, rateLimitedScheduledExecutor.getRepeatingTasks().size());
    assertEquals(
            repeatingTask,
            rateLimitedScheduledExecutor.scheduleAtFixedRate(repeatRunnable, 0, 5, SECONDS));
    assertEquals(taskA, rateLimitedScheduledExecutor.schedule(runnableA, 5, SECONDS));
    assertEquals(taskB, rateLimitedScheduledExecutor.schedule(runnableB, 10, SECONDS));
    assertEquals(1, rateLimitedScheduledExecutor.getRepeatingTasks().size());
    assertFalse(rateLimitedScheduledExecutor.isTerminated());
    rateLimitedScheduledExecutor.shutdown();
    assertTrue(rateLimitedScheduledExecutor.isShutdown());
    verify(repeatingTask).cancel(false);
    assertTrue(rateLimitedScheduledExecutor.getRepeatingTasks().isEmpty());
    assertFalse(rateLimitedScheduledExecutor.isTerminated());

    assertEquals(1, rateLimitedScheduledExecutor.getTasksForIntervalNumber(105).size());
    assertTrue(rateLimitedScheduledExecutor.isTerminated());
    verify(repeatingTask, times(0)).getTaskForIntervalNumber(anyLong());
  }

  @Test
  public void testAwaitTerminationSuccessful() throws InterruptedException, ExecutionException {
    assertEquals(taskA, rateLimitedScheduledExecutor.schedule(runnableA, 5, SECONDS));

    rateLimitedScheduledExecutor.shutdown();
    assertTrue(rateLimitedScheduledExecutor.isShutdown());
    // run await termination on a different thread
    CompletableFuture<Boolean> completableFuture =
            CompletableFuture.supplyAsync(
                    () -> {
                      try {
                        return rateLimitedScheduledExecutor.awaitTermination(1, SECONDS);
                      } catch (InterruptedException e) {
                        fail();
                        return false;
                      }
                    });
    // wait for a bit to let it cycle
    Thread.sleep(1000);
    // clear the backlog
    assertEquals(1, rateLimitedScheduledExecutor.getTasksForIntervalNumber(105).size());
    // wait for a bit more to ensure it cycles
    Thread.sleep(1000);
    // bump the clock forwards
    lenient().when(clock.instant()).thenReturn(Instant.ofEpochSecond(102));
    assertTrue(completableFuture.get());
  }

  @Test
  public void testBacklogSizeWithRunningPaused() {
    rateLimitedScheduledExecutor.setMaxTPS(0);
    assertEquals(Double.POSITIVE_INFINITY, rateLimitedScheduledExecutor.getBacklogSize(), 0);
  }

  @Test
  public void testAwaitTerminationTimesOut() throws InterruptedException, ExecutionException {
    assertEquals(taskA, rateLimitedScheduledExecutor.schedule(runnableA, 5, SECONDS));

    rateLimitedScheduledExecutor.shutdown();
    assertTrue(rateLimitedScheduledExecutor.isShutdown());
    // run await termination on a different thread
    CompletableFuture<Boolean> completableFuture =
            CompletableFuture.supplyAsync(
                    () -> {
                      try {
                        return rateLimitedScheduledExecutor.awaitTermination(1, SECONDS);
                      } catch (InterruptedException e) {
                        fail();
                        return false;
                      }
                    });
    // wait for a bit more to ensure it cycles
    Thread.sleep(1000);
    // bump the clock forwards
    when(clock.instant()).thenReturn(Instant.ofEpochSecond(102));
    assertFalse(completableFuture.get());
  }

  @Test
  public void testShutDownNow() {
    assertEquals(0, rateLimitedScheduledExecutor.getRepeatingTasks().size());
    assertEquals(
            repeatingTask,
            rateLimitedScheduledExecutor.scheduleAtFixedRate(repeatRunnable, 0, 5, SECONDS));
    assertEquals(taskA, rateLimitedScheduledExecutor.schedule(runnableA, 5, SECONDS));
    assertEquals(callableTask, rateLimitedScheduledExecutor.schedule(callableA, 5, SECONDS));
    assertEquals(1, rateLimitedScheduledExecutor.getRepeatingTasks().size());
    List<Runnable> shutdownList = rateLimitedScheduledExecutor.shutdownNow();
    assertEquals(1, shutdownList.size());
    assertTrue(shutdownList.contains(runnableA));
    assertTrue(rateLimitedScheduledExecutor.isShutdown());
    assertTrue(rateLimitedScheduledExecutor.isTerminated());
    verify(repeatingTask, times(0)).getTaskForIntervalNumber(anyLong());
  }

  @Test
  public void testAddTaskWhenSecondAlreadyProcessedGoesToOverflowBucketWhenNoListsAvailable() {
    rateLimitedScheduledExecutor.getTasksForIntervalNumber(105);

    assertTrue(rateLimitedScheduledExecutor.getOverflowBucket().isEmpty());
    assertEquals(taskA, rateLimitedScheduledExecutor.schedule(runnableA, 5, SECONDS));
    verify(logger)
            .finer(
                    String.format(
                            "%s attempting to place task %s which is meant for interval %s directly in a IntervalRunner for as the delay of %s is too short to schedule properly",
                            "tag", taskA, 105, 0));
    verify(rateLimitedScheduledExecutorManager).addTaskForInterval(taskA, 105);
    verify(logger)
            .finer(
                    String.format(
                            "%s this failed so placing the task %s directly in the overflow bucket",
                            "tag", taskA));
    assertEquals(1, rateLimitedScheduledExecutor.getOverflowBucket().size());
    assertTrue(rateLimitedScheduledExecutor.getOverflowBucket().contains(taskA));
  }

  @Test
  public void testAddTaskWhenSecondAlreadyProcessedGoesToIntervalRunner() {
    rateLimitedScheduledExecutor.getTasksForIntervalNumber(105);
    when(rateLimitedScheduledExecutorManager.addTaskForInterval(taskA, 105)).thenReturn(true);
    assertTrue(rateLimitedScheduledExecutor.getOverflowBucket().isEmpty());
    assertEquals(taskA, rateLimitedScheduledExecutor.schedule(runnableA, 5, SECONDS));
    verify(logger)
            .finer(
                    String.format(
                            "%s attempting to place task %s which is meant for interval %s directly in a IntervalRunner for as the delay of %s is too short to schedule properly",
                            "tag", taskA, 105, 0));
    verify(rateLimitedScheduledExecutorManager).addTaskForInterval(taskA, 105);
    verify(logger).finer("tag placed it directly into the IntervalRunner for interval 105");
    assertEquals(0, rateLimitedScheduledExecutor.getOverflowBucket().size());
  }

  @Test
  public void testAddTaskWhenBucketIsLocked() {
    factoryReturnsEmptiedSets = true;
    assertTrue(rateLimitedScheduledExecutor.getOverflowBucket().isEmpty());
    assertEquals(taskA, rateLimitedScheduledExecutor.schedule(runnableA, 5, SECONDS));
    verify(logger)
            .finer(
                    String.format(
                            "%s placing task %s directly in the overflow bucket the correct bucket was already emptied",
                            "tag", taskA));

    assertEquals(1, rateLimitedScheduledExecutor.getOverflowBucket().size());
    assertTrue(rateLimitedScheduledExecutor.getOverflowBucket().contains(taskA));
  }

  @Test
  public void testFilterOldTasksWithNoTimeout() {
    List<ScheduledTask> taskList = List.of(taskA, taskB);
    assertEquals(taskList, rateLimitedScheduledExecutor.removeOldTasksFromList(taskList, 0));
  }

  @Test
  public void testFilterOldTasksWithTimeout() {
    rateLimitedScheduledExecutor.setTaskTimeout(Duration.ofSeconds(7));
    List<ScheduledTask> taskList = List.of(taskA, taskB);
    List<ScheduledTask> resultList =
            rateLimitedScheduledExecutor.removeOldTasksFromList(taskList, 115);
    // check taskb is there on its own
    assertEquals(1, resultList.size());
    assertTrue(resultList.contains(taskB));
    // check task a was correctly dealt with
    verify(taskA).timeOut();
    verify(logger).finer(String.format("%s timed out task %s", "tag", taskA));
  }

  @Test
  public void testFilterCompletetasks() {
    List<ScheduledTask> taskList = List.of(taskA, taskB);
    when(taskA.isDone()).thenReturn(true);
    when(taskB.isDone()).thenReturn(false);
    List<ScheduledTask> resultList =
            rateLimitedScheduledExecutor.removeCompletedOrCancelledTasks(taskList);
    // check taskb is there on its own
    assertEquals(1, resultList.size());
    assertTrue(resultList.contains(taskB));
  }

  @Test
  public void testCatchUpWhenSecondsJump() {
    when(taskA.isDone()).thenReturn(false);
    when(taskC.isDone()).thenReturn(false);
    when(taskD.isDone()).thenReturn(false);
    when(taskC.getScheduledTimeMillis()).thenReturn(106000L);
    when(taskD.getScheduledTimeMillis()).thenReturn(107000L);
    rateLimitedScheduledExecutor.schedule(runnableA, 5, SECONDS);
    rateLimitedScheduledExecutor.schedule(runnableB, 10, SECONDS);
    rateLimitedScheduledExecutor.schedule(runnableC, 6, SECONDS);
    rateLimitedScheduledExecutor.schedule(runnableD, 7, SECONDS);
    assertTrue(rateLimitedScheduledExecutor.getOverflowBucket().isEmpty());
    List<ScheduledTask> tasksForInterval =
            rateLimitedScheduledExecutor.getTasksForIntervalNumber(107);
    verify(logger)
            .fine(
                    "tag - jumped forwards in time - we were expecting 96 but we got 107 so sweeping up all other tasks");
    verify(logger).finest("tag - returning 3 tasks");
    // check acd are included but not b
    assertEquals(3, tasksForInterval.size());
    assertTrue(tasksForInterval.contains(taskA));
    assertTrue(tasksForInterval.contains(taskC));
    assertTrue(tasksForInterval.contains(taskD));
    assertTrue(rateLimitedScheduledExecutor.getOverflowBucket().isEmpty());
  }

  @Test
  public void testOutOfOrderRequests() {
    when(taskA.isDone()).thenReturn(false);
    when(taskC.getScheduledTimeMillis()).thenReturn(106000L);
    when(taskD.getScheduledTimeMillis()).thenReturn(107000L);
    rateLimitedScheduledExecutor.schedule(runnableA, 5, SECONDS);
    rateLimitedScheduledExecutor.schedule(runnableB, 10, SECONDS);
    rateLimitedScheduledExecutor.schedule(runnableC, 6, SECONDS);
    rateLimitedScheduledExecutor.schedule(runnableD, 7, SECONDS);
    assertTrue(rateLimitedScheduledExecutor.getOverflowBucket().isEmpty());
    rateLimitedScheduledExecutor.setLastProcessedInterval(105);
    try {
      rateLimitedScheduledExecutor.getTasksForIntervalNumber(105);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals(
              "we have already processed tasks for 105 requesting tasks for 105 is not valid",
              e.getMessage());
      assertEquals(1, rateLimitedScheduledExecutor.getOverflowBucket().size());
      assertTrue(rateLimitedScheduledExecutor.getOverflowBucket().contains(taskA));
    }
  }

  @Test
  public void testRateLimitedRequests() {
    when(taskA.isDone()).thenReturn(false);
    when(taskB.isDone()).thenReturn(false);
    when(taskC.isDone()).thenReturn(false);
    when(taskD.isDone()).thenReturn(false);

    when(taskA.getScheduledTimeMillis()).thenReturn(105005L);
    when(taskB.getScheduledTimeMillis()).thenReturn(105002L);
    when(taskC.getScheduledTimeMillis()).thenReturn(105003L);
    when(taskD.getScheduledTimeMillis()).thenReturn(105006L);
    rateLimitedScheduledExecutor.schedule(runnableA, 5005, MILLISECONDS);
    rateLimitedScheduledExecutor.schedule(runnableB, 5002, MILLISECONDS);
    rateLimitedScheduledExecutor.schedule(runnableC, 5003, MILLISECONDS);
    rateLimitedScheduledExecutor.schedule(runnableD, 5006, MILLISECONDS);
    rateLimitedScheduledExecutor.setMaxTPS(2);
    rateLimitedScheduledExecutor.setLastProcessedInterval(104);
    assertEquals(2, rateLimitedScheduledExecutor.getBacklogSize(), 0);
    List<ScheduledTask> tasksForInterval =
            rateLimitedScheduledExecutor.getTasksForIntervalNumber(105);

    assertEquals(2, rateLimitedScheduledExecutor.getMaxTPS(), 0);
    verify(logger).finest("tag returning 2 tasks, overflowing 2");

    // check we got the earliest scheduled items back
    assertEquals(2, tasksForInterval.size());
    assertTrue(tasksForInterval.contains(taskB));
    assertTrue(tasksForInterval.contains(taskC));
    assertEquals(1, rateLimitedScheduledExecutor.getBacklogSize(), 0);
    // check the bucket contains the latest scheduled items
    assertEquals(2, rateLimitedScheduledExecutor.getOverflowBucket().size());
    assertTrue(rateLimitedScheduledExecutor.getOverflowBucket().contains(taskA));
    assertTrue(rateLimitedScheduledExecutor.getOverflowBucket().contains(taskD));
    tasksForInterval = rateLimitedScheduledExecutor.getTasksForIntervalNumber(106);

    assertEquals(0, rateLimitedScheduledExecutor.getBacklogSize(), 0);
    assertEquals(2, tasksForInterval.size());
    assertTrue(tasksForInterval.contains(taskA));
    assertTrue(tasksForInterval.contains(taskD));
  }

  @Test
  public void testRateLimitedRequestsWithAlternateSort() {
    when(taskA.isDone()).thenReturn(false);
    when(taskB.isDone()).thenReturn(false);
    when(taskC.isDone()).thenReturn(false);
    when(taskD.isDone()).thenReturn(false);

    when(taskA.getScheduledTimeMillis()).thenReturn(105005L);
    when(taskB.getScheduledTimeMillis()).thenReturn(105002L);
    when(taskC.getScheduledTimeMillis()).thenReturn(105003L);
    when(taskD.getScheduledTimeMillis()).thenReturn(105006L);
    rateLimitedScheduledExecutor.schedule(runnableA, 5005, MILLISECONDS);
    rateLimitedScheduledExecutor.schedule(runnableB, 5002, MILLISECONDS);
    rateLimitedScheduledExecutor.schedule(runnableC, 5003, MILLISECONDS);
    rateLimitedScheduledExecutor.schedule(runnableD, 5006, MILLISECONDS);
    rateLimitedScheduledExecutor.setMaxTPS(2);

    rateLimitedScheduledExecutor.setLastProcessedInterval(104);
    assertEquals(2, rateLimitedScheduledExecutor.getBacklogSize(), 0);
    rateLimitedScheduledExecutor.setSortMethod(SORTED_BY_SCHEDULED_TIME_LATEST_FIRST);

    List<ScheduledTask> tasksForInterval =
            rateLimitedScheduledExecutor.getTasksForIntervalNumber(105);
    assertEquals(1, rateLimitedScheduledExecutor.getBacklogSize(), 0);
    assertEquals(2, rateLimitedScheduledExecutor.getMaxTPS(), 0);
    verify(logger).finest("tag returning 2 tasks, overflowing 2");

    // check we got the earliest scheduled items back
    assertEquals(2, tasksForInterval.size());
    assertTrue(tasksForInterval.contains(taskA));
    assertTrue(tasksForInterval.contains(taskD));

    // check the bucket contains the latest scheduled items
    assertEquals(2, rateLimitedScheduledExecutor.getOverflowBucket().size());
    assertTrue(rateLimitedScheduledExecutor.getOverflowBucket().contains(taskC));
    assertTrue(rateLimitedScheduledExecutor.getOverflowBucket().contains(taskB));
    assertEquals(
            SORTED_BY_SCHEDULED_TIME_LATEST_FIRST, rateLimitedScheduledExecutor.getSortMethod());
  }

  @Test
  public void testSetInvalidTPS() {
    // 10 intervals per second
    rateLimitedScheduledExecutor =
            new RateLimitedScheduledExecutor(
                    "tag",
                    false,
                    1,
                    100,
                    scheduledTaskFactory,
                    lockableListFactory,
                    rateLimitedScheduledExecutorManager,
                    logger,
                    clock);

    try {
      rateLimitedScheduledExecutor.setMaxTPS(-1);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("you can not set maxTPS -1.0 to a negative number", e.getMessage());
      assertEquals(Double.POSITIVE_INFINITY, rateLimitedScheduledExecutor.getMaxTPS(), 0);
    }
  }

  @Test
  public void testRateLimitedRequestsUnderLimitAfterFiltering() {
    when(taskB.isDone()).thenReturn(false);
    when(taskC.isDone()).thenReturn(false);
    when(taskD.isDone()).thenReturn(true);

    when(taskA.getScheduledTimeMillis()).thenReturn(100000L);
    when(taskB.getScheduledTimeMillis()).thenReturn(105002L);
    when(taskC.getScheduledTimeMillis()).thenReturn(105003L);
    when(taskD.getScheduledTimeMillis()).thenReturn(105006L);

    rateLimitedScheduledExecutor.schedule(runnableA, 0, MILLISECONDS);
    rateLimitedScheduledExecutor.schedule(runnableB, 5002, MILLISECONDS);
    rateLimitedScheduledExecutor.schedule(runnableC, 5003, MILLISECONDS);
    rateLimitedScheduledExecutor.schedule(runnableD, 5006, MILLISECONDS);
    rateLimitedScheduledExecutor.setMaxTPS(2);
    rateLimitedScheduledExecutor.setTaskTimeout(Duration.ofSeconds(2));
    List<ScheduledTask> tasksForInterval =
            rateLimitedScheduledExecutor.getTasksForIntervalNumber(105);

    assertEquals(2, rateLimitedScheduledExecutor.getMaxTPS(), 0);
    verify(logger).finest("tag - returning 2 tasks");

    // check we got the earliest scheduled items back
    assertEquals(2, tasksForInterval.size());
    assertTrue(tasksForInterval.contains(taskB));
    assertTrue(tasksForInterval.contains(taskC));

    verify(taskA).timeOut();
    verify(logger).finer(String.format("%s timed out task %s", "tag", taskA));
    // check the bucket contains the latest scheduled items
    assertEquals(0, rateLimitedScheduledExecutor.getOverflowBucket().size());
  }

  @Test
  public void testLoggingWithException() {
    factoryFiresException = true;
    assertEquals(0, rateLimitedScheduledExecutor.getTasksForIntervalNumber(105).size());
    verify(logger)
            .log(
                    eq(Level.WARNING),
                    eq("tag got an exception whilst getting tasks for a given interval of 105"),
                    any(RuntimeException.class));
  }

  @Test
  public void testSetInvalidTaskTimeout() {
    try {
      rateLimitedScheduledExecutor.setTaskTimeout(Duration.ofSeconds(-1));
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("the duration for a task timeout must be positive", e.getMessage());
      assertEquals(Duration.ZERO, rateLimitedScheduledExecutor.getTaskTimeout());
    }
  }

  @Test
  public void testRemoveCountForInterval() {
    rateLimitedScheduledExecutor.setMaxTPS(5);
    assertEquals(taskA, rateLimitedScheduledExecutor.schedule(runnableA, 5, SECONDS));
    assertTrue(rateLimitedScheduledExecutor.getNumberOfTasksRemainingPerInterval().isEmpty());

    assertEquals(1, rateLimitedScheduledExecutor.getTasksForIntervalNumber(105).size());
    assertEquals(1, rateLimitedScheduledExecutor.getNumberOfTasksRemainingPerInterval().size());
    assertEquals(
            4, rateLimitedScheduledExecutor.getNumberOfTasksRemainingPerInterval().get(105L).get());

    rateLimitedScheduledExecutor.removeCountsForInterval(105);
    assertTrue(rateLimitedScheduledExecutor.getNumberOfTasksRemainingPerInterval().isEmpty());
  }

  @Test
  public void testSethNoEarlyTasksWithTasksWaiting() {
    rateLimitedScheduledExecutor.schedule(runnableA, 5, SECONDS);
    try {
      rateLimitedScheduledExecutor.setEnsureNoTaskDispatchedEarly(true);
      fail();
    } catch (RateLimitedScheduledExecutorHasTasksScheduledException e) {
      assertFalse(rateLimitedScheduledExecutor.isEnsureNoTaskDispatchedEarly());
    }
  }

  @Test
  public void testTransactionsPerInterval() {
    rateLimitedScheduledExecutor =
            new RateLimitedScheduledExecutor(
                    "tag",
                    false,
                    1,
                    10,
                    scheduledTaskFactory,
                    lockableListFactory,
                    rateLimitedScheduledExecutorManager,
                    logger,
                    clock);
    rateLimitedScheduledExecutor.setMaxTPS(17);
    rateLimitedScheduledExecutor.setNumberOfPeers(4);
    long totalTPS = 0;
    for (int i = 10; i < 10_000_010; i++) {
      totalTPS += rateLimitedScheduledExecutor.getNumberOfTasksForInterval(i);
    }
    // check the TPS over 10,000,000 intervals, with 100 intervals per second, with 4 peers is
    // 425,000
    assertEquals(425_000, totalTPS);
  }

  @Test
  public void testOldTransactionsReassessedAfterTPSOrNumberOfPeersChange() {
    rateLimitedScheduledExecutor.setMaxTPS(10);
    rateLimitedScheduledExecutor.schedule(runnableA, 5, SECONDS);
    rateLimitedScheduledExecutor.getTasksForIntervalNumber(101);
    rateLimitedScheduledExecutor.getTasksForIntervalNumber(102);
    rateLimitedScheduledExecutor.getTasksForIntervalNumber(103);
    rateLimitedScheduledExecutor.getTasksForIntervalNumber(104);
    rateLimitedScheduledExecutor.getTasksForIntervalNumber(105);
    assertEquals(5, rateLimitedScheduledExecutor.getNumberOfTasksRemainingPerInterval().size());
    assertEquals(
            10, rateLimitedScheduledExecutor.getNumberOfTasksRemainingPerInterval().get(101L).get());
    assertEquals(
            10, rateLimitedScheduledExecutor.getNumberOfTasksRemainingPerInterval().get(102L).get());
    assertEquals(
            10, rateLimitedScheduledExecutor.getNumberOfTasksRemainingPerInterval().get(103L).get());
    assertEquals(
            10, rateLimitedScheduledExecutor.getNumberOfTasksRemainingPerInterval().get(104L).get());
    assertEquals(
            9, rateLimitedScheduledExecutor.getNumberOfTasksRemainingPerInterval().get(105L).get());
    rateLimitedScheduledExecutor.setNumberOfPeers(2);
    assertEquals(5, rateLimitedScheduledExecutor.getNumberOfTasksRemainingPerInterval().size());
    assertEquals(
            5, rateLimitedScheduledExecutor.getNumberOfTasksRemainingPerInterval().get(101L).get());
    assertEquals(
            5, rateLimitedScheduledExecutor.getNumberOfTasksRemainingPerInterval().get(102L).get());
    assertEquals(
            5, rateLimitedScheduledExecutor.getNumberOfTasksRemainingPerInterval().get(103L).get());
    assertEquals(
            5, rateLimitedScheduledExecutor.getNumberOfTasksRemainingPerInterval().get(104L).get());
    assertEquals(
            4, rateLimitedScheduledExecutor.getNumberOfTasksRemainingPerInterval().get(105L).get());
    rateLimitedScheduledExecutor.setMaxTPS(20);
    assertEquals(5, rateLimitedScheduledExecutor.getNumberOfTasksRemainingPerInterval().size());
    assertEquals(
            10, rateLimitedScheduledExecutor.getNumberOfTasksRemainingPerInterval().get(101L).get());
    assertEquals(
            10, rateLimitedScheduledExecutor.getNumberOfTasksRemainingPerInterval().get(102L).get());
    assertEquals(
            10, rateLimitedScheduledExecutor.getNumberOfTasksRemainingPerInterval().get(103L).get());
    assertEquals(
            10, rateLimitedScheduledExecutor.getNumberOfTasksRemainingPerInterval().get(104L).get());
    assertEquals(
            9, rateLimitedScheduledExecutor.getNumberOfTasksRemainingPerInterval().get(105L).get());
  }
}
