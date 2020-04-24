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
import com.americanexpress.ratelimitedscheduler.exceptions.CollectionHasBeenEmptiedException;
import com.americanexpress.ratelimitedscheduler.exceptions.RateLimitedScheduledExecutorHasTasksScheduledException;
import com.americanexpress.ratelimitedscheduler.task.RepeatingTask;
import com.americanexpress.ratelimitedscheduler.task.ScheduledCallableTask;
import com.americanexpress.ratelimitedscheduler.task.ScheduledRunnableTask;
import com.americanexpress.ratelimitedscheduler.task.ScheduledTask;
import com.americanexpress.ratelimitedscheduler.task.ScheduledTaskFactory;

import com.google.common.annotations.VisibleForTesting;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static com.americanexpress.ratelimitedscheduler.task.RepeatingTask.RepeatingType.DELAY;
import static com.americanexpress.ratelimitedscheduler.task.RepeatingTask.RepeatingType.RATE;
import static com.americanexpress.ratelimitedscheduler.task.TaskSorters.SORTED_BY_SCHEDULED_TIME_EARLIEST_FIRST_WITH_REPEATING_FIRST_THEN_IMMEDIATE_THEN_DELAYED;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * a single, rate-limited executor. This should not be created by itself as the tasks won't actually
 * run without the Manager
 */
@SuppressWarnings({"WeakerAccess", "squid:S2629", "NullableProblems"})
public class RateLimitedScheduledExecutor implements ScheduledExecutorService {
  // how long each interval is
  private final int millisPerInterval;
  // contains all the tasks that repeat
  private final LockableList<RepeatingTask> repeatingTasks;
  // creates lockable lists
  private final LockableListFactory lockableListFactory;
  // used for tagging logs
  private final String serviceTag;
  private final Logger logger;
  private final ScheduledTaskFactory scheduledTaskFactory;
  // a map containing buckets of tasks for every interval in the future (populated on demand)
  private final Map<Long, LockableList<ScheduledTask>> scheduledTasks;
  // a place to store tasks we did not attempt due to TPS throttling
  private final LockableList<ScheduledTask> overflowBucket;
  // holds how many tasks were scheduled per interval
  private final Map<Long, AtomicInteger> numberOfTasksRemainingPerInterval;
  // the manager of this executor, used as a bridge of accessing the taskRunner
  private final RateLimitedScheduledExecutorManager rateLimitedScheduledExecutorManager;
  private final Clock clock;
  // keep track of the last interval we processed, so we can dump tasks occuring in a interval we
  // have
  // already processed straight into the overflow
  private long lastProcessedInterval;
  // how many TPS this will process
  private double maxTPS;
  // the maximum duration between a schedule and trying a task to give up
  private Duration taskTimeout;
  // how to sort tasks in the event that TPS is limited
  private Comparator<ScheduledTask> sortMethod;
  // whether or not the executor is shut down
  private boolean isShutDown;
  // whether we force all tasks not to dispatch before they were meant to (dispatch in the window
  // that starts after the scheduled time, or are happy with the
  // average time (dispatch in the window in which the schedule time occurs)
  private boolean ensureNoTaskDispatchedEarly;
  // how many peers we have to share the load with
  private int numberOfPeers;
  /**
   * creates a RateLimitedScheduledExecutor
   *
   * @param serviceTag used by logger to give info as to which scheduledExecutor this is
   * @param ensureNoTaskDispatchedEarly sets a task will be attempted in the correct window (so may
   *     dispatch up to 1 interval worth of time early) or in the interval after it is scheduled (so
   *     will never dispatch early)
   * @param numberOfPeers how many peers we have at startup
   * @param millisPerInterval how many miliseconds in each interval
   * @param scheduledTaskFactory a factory for generating scheduled tasks based on parameters
   * @param lockableListFactory a factory for generating lockable listss
   * @param rateLimitedScheduledExecutorManager the manager which provides a link to intervalrunners
   * @param logger to log with
   * @param clock a time source (externalised for testing)
   */
  RateLimitedScheduledExecutor(
          String serviceTag,
          boolean ensureNoTaskDispatchedEarly,
          int numberOfPeers,
          int millisPerInterval,
          ScheduledTaskFactory scheduledTaskFactory,
          LockableListFactory lockableListFactory,
          RateLimitedScheduledExecutorManager rateLimitedScheduledExecutorManager,
          Logger logger,
          Clock clock) {
    this.numberOfPeers = numberOfPeers;
    this.millisPerInterval = millisPerInterval;
    this.serviceTag = serviceTag;
    this.scheduledTaskFactory = scheduledTaskFactory;
    this.lockableListFactory = lockableListFactory;
    this.rateLimitedScheduledExecutorManager = rateLimitedScheduledExecutorManager;
    this.logger = logger;
    this.clock = clock;
    scheduledTasks = new ConcurrentHashMap<>();
    numberOfTasksRemainingPerInterval = new ConcurrentHashMap<>();
    // default the sort method
    sortMethod =
            SORTED_BY_SCHEDULED_TIME_EARLIEST_FIRST_WITH_REPEATING_FIRST_THEN_IMMEDIATE_THEN_DELAYED;

    overflowBucket = lockableListFactory.getLockableList();
    lastProcessedInterval = Instant.now(clock).minusSeconds(5).toEpochMilli() / millisPerInterval;
    setMaxTPS(Double.POSITIVE_INFINITY);
    taskTimeout = Duration.ZERO;
    isShutDown = false;
    this.ensureNoTaskDispatchedEarly = ensureNoTaskDispatchedEarly;
    repeatingTasks = lockableListFactory.getLockableList();
  }

  @VisibleForTesting
  int getNumberOfTasksForInterval(long intervalNumber) {
    return getNumberOfTasksForInterval(intervalNumber, maxTPS, numberOfPeers);
  }

  /**
   * figure out how many transactions we can execute in a given interval
   *
   * @param intervalNumber    which interval we are talking about
   * @param thisMaxTPS        the maximum TPS that has been set
   * @param thisNumberOfPeers how many peers we are looking at
   * @return how many tasks we can schedule
   */
  private int getNumberOfTasksForInterval(
          long intervalNumber, double thisMaxTPS, int thisNumberOfPeers) {
    // if we have infinate TPS, support loads of transactions
    if (Double.isInfinite(thisMaxTPS)) {
      return Integer.MAX_VALUE;
    }
    // if there are no peers detected, or max TPS is zero, pause execution
    if (thisNumberOfPeers == 0 || thisMaxTPS == 0) {
      return 0;
    }
    // work out how many transactions we had at the start of the interval
    double transactionsAtStartOfInterval =
            (thisMaxTPS * millisPerInterval * intervalNumber) / (1000 * thisNumberOfPeers);
    // and how many at the end
    double transactionsAtEndOfInterval =
            (thisMaxTPS * millisPerInterval * (intervalNumber + 1)) / (1000 * thisNumberOfPeers);
    // then floor each number to work out how many whole transactions we can execute. This means we
    // can execute whole transactions (you can't execute less than a whole one) and still meet TPS
    // requirements
    return (int)
            (Math.floor(transactionsAtEndOfInterval) - Math.floor(transactionsAtStartOfInterval));
  }

  /**
   * get tasks in scope for a given interval.
   *
   * @param intervalNumber which interval (since the epoch) to pull tasks for
   * @return a list of tasks for this data
   * @throws IllegalArgumentException if you request tasks for a interval which is earlier than (or
   *     the same as) the most recent interval we processed.
   */
  List<ScheduledTask> getTasksForIntervalNumber(long intervalNumber) {
    try {
      overflowBucket.lock();
      repeatingTasks.lock();
      // get the tasks scheduled for in scope
      List<ScheduledTask> tasksForTheInterval =
              scheduledTasks
                      .getOrDefault(intervalNumber, lockableListFactory.getLockableList())
                      .empty();
      // for each possible repeating task
      for (RepeatingTask repeatingTask : repeatingTasks) {
        // if there is a task for this interval, add it to ths list
        repeatingTask.getTaskForIntervalNumber(intervalNumber).ifPresent(tasksForTheInterval::add);
      }
      // add all the tasks from the bucket (don't empty it as we do want to reuse it)
      tasksForTheInterval.addAll(overflowBucket);
      overflowBucket.clear();

      // clean up any missed intervals
      if (intervalNumber > lastProcessedInterval + 1) {
        logger.fine(
                String.format(
                        "%s - jumped forwards in time - we were expecting %s but we got %s so sweeping up all other tasks",
                        serviceTag, (lastProcessedInterval + 1), intervalNumber));
        for (long thisInterval = lastProcessedInterval + 1;
             thisInterval < intervalNumber;
             thisInterval++) {
          tasksForTheInterval.addAll(
                  scheduledTasks
                          .getOrDefault(thisInterval, lockableListFactory.getLockableList())
                          .empty());
          scheduledTasks.remove(thisInterval);
        }
      }

      // remove dead tasks from the list
      tasksForTheInterval =
              removeCompletedOrCancelledTasks(
                      removeOldTasksFromList(tasksForTheInterval, intervalNumber));

      // check for out-of-order requests
      if (intervalNumber <= lastProcessedInterval) {
        // dump all the records in the overflow bucket and exit
        overflowBucket.addAll(tasksForTheInterval);
        throw new IllegalArgumentException(
                "we have already processed tasks for "
                        + lastProcessedInterval
                        + " requesting tasks for "
                        + intervalNumber
                        + " is not valid");
      }
      // update the last processed interval
      lastProcessedInterval = intervalNumber;

      // we now have all the tasks we need. lets work out how many transactions we can do
      int numberOfTasksInThisInterval = getNumberOfTasksForInterval(intervalNumber);
      if (tasksForTheInterval.size() <= numberOfTasksInThisInterval) {
        // no tasks to overflow, so just return them all

        logger.finest(
                String.format("%s - returning %s tasks", serviceTag, tasksForTheInterval.size()));
        numberOfTasksRemainingPerInterval.put(
                intervalNumber,
                new AtomicInteger(numberOfTasksInThisInterval - tasksForTheInterval.size()));
        return tasksForTheInterval;

      } else {
        // we will need to overflow some tasks
        // so sort them
        tasksForTheInterval.sort(sortMethod);

        // add what we can't use to the overflow bucket
        overflowBucket.addAll(
                tasksForTheInterval.subList(numberOfTasksInThisInterval, tasksForTheInterval.size()));
        // return what is not left over
        logger.finest(
                String.format(
                        "%s returning %s tasks, overflowing %s",
                        serviceTag, numberOfTasksInThisInterval, overflowBucket.size()));
        // store how many we put in the bucket
        numberOfTasksRemainingPerInterval.put(intervalNumber, new AtomicInteger(0));
        // return the list
        return tasksForTheInterval.subList(0, numberOfTasksInThisInterval);
      }
    } catch (IllegalArgumentException e) {
      // allow the illegalargumentexception to pass through
      throw e;
    } catch (Exception e) {
      // catch all others to avoid squishing exceptions
      logger.log(
              Level.WARNING,
              String.format(
                      "%s got an exception whilst getting tasks for a given interval of %s",
                      serviceTag, intervalNumber),
              e);
      return Collections.emptyList();
    } finally {
      // clean up - remove this list from our pointers (so we don't build up memory) and unlock the
      // overflow bucket and repeating task list
      scheduledTasks.remove(intervalNumber);
      overflowBucket.unlock();
      repeatingTasks.unlock();
    }
  }

  /**
   * clean up the how many records we used for a given interval (avoids memory leaks)
   *
   * @param intervalNumber which interval to remove
   */
  void removeCountsForInterval(long intervalNumber) {
    numberOfTasksRemainingPerInterval.remove(intervalNumber);
  }

  @VisibleForTesting
  List<ScheduledTask> removeCompletedOrCancelledTasks(Collection<ScheduledTask> tasks) {
    // removes everything where the completedfuture has already been completed, to avoid it being
    // re-attempted
    return tasks.stream()
            .filter(scheduledTask -> !scheduledTask.isDone())
            .collect(Collectors.toList());
  }

  /**
   * remove anything that should have timed out
   *
   * @param tasks the list of tasks to check
   * @param intervalNumber which interval to assess them against
   * @return the list, without records that have been timed out
   */
  @VisibleForTesting
  List<ScheduledTask> removeOldTasksFromList(List<ScheduledTask> tasks, long intervalNumber) {
    // doesn't do anything if we have no timeout
    if (taskTimeout.isZero()) {
      return tasks;
    }
    // work out the end of the interval in milliseconds
    long oldestMillis = (intervalNumber + 1) * millisPerInterval - taskTimeout.toMillis();

    // remove tasks older than the date from the list, and time them out
    return tasks.stream()
            .filter(
                    scheduledTask -> {
                      if (scheduledTask.getScheduledTimeMillis() > oldestMillis) {
                        return true;
                      } else {
                        logger.finer(String.format("%s timed out task %s", serviceTag, scheduledTask));
                        scheduledTask.timeOut();
                        return false;
                      }
                    })
            .collect(Collectors.toList());
  }

  /**
   * get the Comparitor which is currently being used to sort tasks in case the TPS threshold is
   * exceeded
   *
   * @return the comparitor
   */
  public Comparator<ScheduledTask> getSortMethod() {
    return sortMethod;
  }

  /**
   * set the Comparitor which to use to sort tasks in case the TPS threshold is exceeded
   *
   * @param sortMethod the comparitor to use, typed with ScheduledTask (so you can sort by
   *     additional attributes of type T)
   */
  public void setSortMethod(Comparator<ScheduledTask> sortMethod) {
    this.sortMethod = sortMethod;
  }

  /**
   * work out how many interval's worth of records we have 'backed up'. This includes future
   * records, but does not include future repeating records.
   *
   * @return how many intervals are already backed up
   */
  public double getBacklogSize() {
    int records = overflowBucket.size();
    long interval = lastProcessedInterval + 1;
    records += scheduledTasks.getOrDefault(interval, new LockableList<>()).size();
    int transactionsPerInterval;
    while (records >= (transactionsPerInterval = getNumberOfTasksForInterval(interval))) {
      if (transactionsPerInterval == 0) {
        // we have no peers or TPS is paused, so we will never complete
        return Double.POSITIVE_INFINITY;
      }
      interval++;
      records =
              records
                      - transactionsPerInterval
                      + scheduledTasks.getOrDefault(interval, new LockableList<>()).size();
    }
    return (double) interval - (lastProcessedInterval + 1);
  }

  /**
   * get how many tasks are currently scheduled against the scheduler. reapeating tasks are counted
   * as one task
   *
   * @return how many tasks are scheduled
   */
  public int getNumberOfTasksAwaitingExecution() {
    int tasksInQueue = repeatingTasks.size();
    for (LockableList<ScheduledTask> tasksForInterval : scheduledTasks.values()) {
      tasksInQueue += tasksForInterval.size();
    }
    tasksInQueue += overflowBucket.size();
    return tasksInQueue;
  }

  /**
   * gets whether a task will be attempted in the correct window (so may dispatch up to 1 interval
   * worth of time early) or in the interval after it is scheduled (so will never dispatch early)
   *
   * @return true if a task may not be dispatched early
   */
  public boolean isEnsureNoTaskDispatchedEarly() {
    return ensureNoTaskDispatchedEarly;
  }

  /**
   * sets a task will be attempted in the correct window (so may dispatch up to 1 interval * worth
   * of time early) or in the interval after it is scheduled (so will never dispatch early)
   *
   * @param ensureNoTaskDispatchedEarly should be true if no task may be dispatched early
   * @throws RateLimitedScheduledExecutorHasTasksScheduledException if the executor currently has
   *     tasks scheduled. these must all be cancelled\completed before attempting to set this value
   */
  public void setEnsureNoTaskDispatchedEarly(boolean ensureNoTaskDispatchedEarly)
          throws RateLimitedScheduledExecutorHasTasksScheduledException {
    if (getNumberOfTasksAwaitingExecution() == 0) {
      this.ensureNoTaskDispatchedEarly = ensureNoTaskDispatchedEarly;
    } else {
      throw new RateLimitedScheduledExecutorHasTasksScheduledException();
    }
  }

  /**
   * get the maximum transactions this executor is currently triggering in a second zero indicates
   * no limit
   *
   * @return the maximum TPS
   */
  public double getMaxTPS() {
    return maxTPS;
  }

  /**
   * set the maximum transactions this executor should trigger in an interval. infinity removes the
   * limit and will trigger as many as possible, 0 pauses all triggering of tasks
   *
   * @param maxTPS the max TPS we should attempt
   */
  public void setMaxTPS(double maxTPS) {
    if (maxTPS < 0) {
      throw new IllegalArgumentException(
              "you can not set maxTPS " + maxTPS + " to a negative number");
    }
    recalculateRemainingTPS(this.maxTPS, maxTPS, numberOfPeers, numberOfPeers);
    this.maxTPS = maxTPS;
  }

  /**
   * recalcuate how many TPS remain for scheduling after the effective TPS has been changed
   *
   * @param oldMaxTPS what the old maximum TPS was
   * @param newMaxTPS what the new maxumim TPS is
   * @param oldPeers  how many peers we used to have
   * @param newPeers  how many peers we have now
   */
  private void recalculateRemainingTPS(
          double oldMaxTPS, double newMaxTPS, int oldPeers, int newPeers) {
    numberOfTasksRemainingPerInterval.forEach(
            (intervalNumber, remainingTasks) -> {
              int diff =
                      getNumberOfTasksForInterval(intervalNumber, oldMaxTPS, oldPeers)
                              - getNumberOfTasksForInterval(intervalNumber, newMaxTPS, newPeers);
              remainingTasks.getAndUpdate(operand -> operand - diff);
            });
  }
  /**
   * how long to attempt a task for before it is timed out note that it may be attempted shortly
   * after this, if the executor thread is very busy (it may be submitted, but not yet executed)
   *
   * @return how long to keep attempting a task before timing out
   */
  public Duration getTaskTimeout() {
    return taskTimeout;
  }

  /**
   * how long to attempt a task for before it is timed out * note that it may be attempted shortly
   * after this, if the executor thread is very busy (it may be submitted, but not yet executed)
   *
   * @param taskTimeout how long to attempt for - must be positive
   */
  public void setTaskTimeout(Duration taskTimeout) {
    if (taskTimeout.isNegative()) {
      throw new IllegalArgumentException("the duration for a task timeout must be positive");
    }
    this.taskTimeout = taskTimeout;
  }

  /**
   * Submits a one-shot task that becomes enabled after the given delay.
   *
   * @param command the task to execute
   * @param delay the time from now to delay execution
   * @param unit the time unit of the delay parameter
   * @return a ScheduledTask representing pending completion of the task and whose {@code get()}
   *     method will return {@code null} upon completion
   * @throws RejectedExecutionException if the task cannot be scheduled for execution
   * @throws NullPointerException if command or unit is null
   */
  @Override
  public ScheduledRunnableTask<Void> schedule(Runnable command, long delay, TimeUnit unit) {
    Duration durationDelay = Duration.ZERO.plus(delay, unit.toChronoUnit());
    // create the scheduled task
    ScheduledRunnableTask<Void> scheduledTask =
            scheduledTaskFactory.getScheduledTask(command, durationDelay);
    scheduleTask(scheduledTask);
    return scheduledTask;
  }

  /**
   * internal mechanism to schedule a task, used by lots of external-facing functions
   *
   * @param scheduledTask the task to schedule
   */
  private void scheduleTask(ScheduledTask scheduledTask) {
    exceptionIfShutDown();
    // if we need to ensure nothing is dispatched early, and it wasn't scheduled to dispatch right
    // now, then go to the next logical bucket
    long interval =
            (ensureNoTaskDispatchedEarly && !scheduledTask.wasRequestedImmediately())
                    ? (scheduledTask.getScheduledTimeMillis() / millisPerInterval) + 1
                    : scheduledTask.getScheduledTimeMillis() / millisPerInterval;
    // if we have already processed this interval, dump the task in the intervalrunner for
    // immediate attempt
    if (interval <= lastProcessedInterval) {
      logger.finer(
              String.format(
                      "%s attempting to place task %s which is meant for interval %s directly in a IntervalRunner for as the delay of %s is too short to schedule properly",
                      serviceTag, scheduledTask, interval, scheduledTask.getDelay(MILLISECONDS)));
      // try each interval bucket
      while (interval <= lastProcessedInterval) {
        AtomicInteger tasksScheduled = numberOfTasksRemainingPerInterval.get(interval);
        // if we tracked a number of tasks
        if (tasksScheduled != null
                // and we are within TPS limit
                && tasksScheduled.getAndDecrement() > 0
                // and we can add the task directly to the tasks in a interval runner
                && rateLimitedScheduledExecutorManager.addTaskForInterval(scheduledTask, interval)) {
          logger.finer(
                  String.format(
                          "%s placed it directly into the IntervalRunner for interval %s",
                          serviceTag, interval));
          // then we are done
          return;
        }
        // otherwise try the next interval
        interval++;
      }
      logger.finer(
              String.format(
                      "%s this failed so placing the task %s directly in the overflow bucket",
                      serviceTag, scheduledTask));
      // place directly in the overflowbucket
      overflowBucket.add(scheduledTask);
    } else {
      // create an entry if no tasks are yet to be scheduled for this interval
      scheduledTasks.putIfAbsent(interval, lockableListFactory.getLockableList());
      try {
        // add the task
        scheduledTasks.get(interval).add(scheduledTask);
      } catch (CollectionHasBeenEmptiedException | NullPointerException ignored) {
        logger.finer(
                String.format(
                        "%s placing task %s directly in the overflow bucket the correct bucket was already emptied",
                        serviceTag, scheduledTask));
        // catches a race condition where we emptied the list as this was going on, and dumps it in
        // the overflow
        overflowBucket.add(scheduledTask);
      }
    }
  }

  /**
   * Submits a value-returning one-shot task that becomes enabled after the given delay.
   *
   * @param callable the function to execute
   * @param delay the time from now to delay execution
   * @param unit the time unit of the delay parameter
   * @return a ScheduledCallableTask that can be used to extract result or cancel
   * @throws RejectedExecutionException if the task cannot be scheduled for execution
   * @throws NullPointerException if callable or unit is null
   */
  @Override
  public <V> ScheduledCallableTask<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {

    Duration durationDelay = Duration.ZERO.plus(delay, unit.toChronoUnit());
    // create the scheduled task
    ScheduledCallableTask<V> scheduledTask =
            scheduledTaskFactory.getScheduledTask(callable, durationDelay);
    scheduleTask(scheduledTask);
    return scheduledTask;
  }

  /**
   * Submits a periodic action that becomes enabled first after the given initial delay, and
   * subsequently with the given period; that is, executions will commence after {@code
   * initialDelay}, then {@code initialDelay + period}, then {@code initialDelay + 2 * period}, and
   * so on.
   *
   * <p>The sequence of task executions continues indefinitely until one of the following
   * exceptional completions occur:
   *
   * <ul>
   *   <li>The task is {@linkplain Future#cancel explicitly cancelled} via the returned future.
   *   <li>The executor terminates, also resulting in task cancellation.
   * </ul>
   *
   * <p>Subsequent executions are suppressed. Subsequent calls to {@link Future#isDone isDone()} on
   * the returned future will return {@code true}.
   *
   * <p>If any execution of this task takes longer than its period, then subsequent executions may
   * start late, but will not concurrently execute.
   *
   * @param command the task to execute
   * @param initialDelay the time to delay first execution
   * @param period the period between successive executions
   * @param unit the time unit of the initialDelay and period parameters
   * @return a RepeatingTask representing pending completion of the series of repeated tasks. The
   *     future's {@link Future#get() get()} method will never return normally, and will throw an
   *     exception upon task cancellation or abnormal termination of a task execution.
   * @throws NullPointerException if command or unit is null
   * @throws IllegalArgumentException if period is too small to effectively schedule
   */
  @Override
  public RepeatingTask scheduleAtFixedRate(
          Runnable command, long initialDelay, long period, TimeUnit unit) {
    exceptionIfShutDown();
    RepeatingTask repeatingTask =
            scheduledTaskFactory.getRepeatingTask(
                    command,
                    Duration.of(initialDelay, unit.toChronoUnit()),
                    Duration.of(period, unit.toChronoUnit()),
                    ensureNoTaskDispatchedEarly,
                    RATE,
                    this);
    repeatingTasks.add(repeatingTask);
    return repeatingTask;
  }

  /**
   * Submits a periodic action that becomes enabled first after the given initial delay, and
   * subsequently with the given delay between the termination of one execution and the commencement
   * of the next.
   *
   * <p>The sequence of task executions continues indefinitely until one of the following
   * exceptional completions occur:
   *
   * <ul>
   *   <li>The task is {@linkplain Future#cancel explicitly cancelled} via the returned future.
   *   <li>The executor terminates, also resulting in task cancellation.
   * </ul>
   *
   * <p>Subsequent executions are suppressed. Subsequent calls to {@link Future#isDone isDone()} on
   * the returned future will return {@code true}.
   *
   * @param command the task to execute
   * @param initialDelay the time to delay first execution
   * @param delay the delay between the termination of one execution and the commencement of the
   *     next
   * @param unit the time unit of the initialDelay and delay parameters
   * @return a RepeatingTask representing pending completion of the series of repeated tasks. The
   *     future's {@link Future#get() get()} method will never return normally, and will throw an
   *     exception upon task cancellation or abnormal termination of a task execution.
   * @throws NullPointerException if command or unit is null
   * @throws IllegalArgumentException if period is too small to effectively schedule
   */
  @Override
  public RepeatingTask scheduleWithFixedDelay(
          Runnable command, long initialDelay, long delay, TimeUnit unit) {
    exceptionIfShutDown();
    RepeatingTask repeatingTask =
            scheduledTaskFactory.getRepeatingTask(
                    command,
                    Duration.of(initialDelay, unit.toChronoUnit()),
                    Duration.of(delay, unit.toChronoUnit()),
                    ensureNoTaskDispatchedEarly,
                    DELAY,
                    this);
    repeatingTasks.add(repeatingTask);
    return repeatingTask;
  }

  /**
   * Initiates an orderly shutdown in which previously submitted tasks are executed, but no new
   * tasks will be accepted. Invocation has no additional effect if already shut down.
   *
   * <p>This method does not wait for previously submitted tasks to complete execution. Use {@link
   * #awaitTermination awaitTermination} to do that.
   */
  @Override
  public void shutdown() {
    isShutDown = true;
    for (RepeatingTask repeatingTask : new ArrayList<>(repeatingTasks)) {
      repeatingTask.cancel(false);
    }
    repeatingTasks.clear();
  }

  /**
   * Attempts to stop all actively executing tasks, halts the processing of waiting tasks, and
   * returns a list of the tasks that were awaiting execution.
   *
   * <p>This method does not wait for actively executing tasks to terminate. Use {@link
   * #awaitTermination awaitTermination} to do that.
   *
   * <p>There are no guarantees beyond best-effort attempts to stop processing actively executing
   * tasks. For example, typical implementations will cancel via {@link Thread#interrupt}, so any
   * task that fails to respond to interrupts may never terminate.
   *
   * <p>this method also does not attempt to stop tasks that are already with an intervalrunner for
   * execution - so you will still execute millisPerInterval*bufferSize future milliseconds worth of
   * tasks
   *
   * @return list of tasks that never commenced execution. Does not include callables or repeating
   *     tasks
   */
  @Override
  public List<Runnable> shutdownNow() {
    shutdown();
    long intervalNumber = Instant.now(clock).toEpochMilli() / millisPerInterval;
    overflowBucket.lock();
    List<ScheduledTask> taskList = new ArrayList<>(overflowBucket);
    overflowBucket.unlock();
    // for every future list of tasks
    for (LockableList<ScheduledTask> tasksForInterval : scheduledTasks.values()) {
      taskList.addAll(tasksForInterval.empty());
    }
    scheduledTasks.clear();
    // add every runnable
    return getRunnablesFromBucket(
            // that wasn't cancelled
            removeCompletedOrCancelledTasks(
                    // or timed out (as of right now)
                    removeOldTasksFromList(taskList, intervalNumber)));
  }

  /**
   * gets runnables from a bucket of mixed runnables and callables
   *
   * @param tasks the list of tasks to assess
   * @return the runnables from that list
   */
  private List<Runnable> getRunnablesFromBucket(List<ScheduledTask> tasks) {
    // for each task
    return tasks.stream()
            .map(
                    // get the runnable if it was a ScheduledRunnableTask
                    scheduledTask -> {
                      if (scheduledTask instanceof ScheduledRunnableTask) {
                        return ((ScheduledRunnableTask) scheduledTask).getRunnable();
                      } else {
                        return null;
                      }
                    })
            // filter out empty records (which were from callables)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
  }

  /**
   * Returns {@code true} if this executor has been shut down.
   *
   * @return {@code true} if this executor has been shut down
   */
  @Override
  public boolean isShutdown() {
    return isShutDown;
  }

  private void exceptionIfShutDown() {
    if (isShutDown) {
      throw new RejectedExecutionException("this executor has been shut down");
    }
  }

  /**
   * Returns {@code true} if all tasks have completed following shut down. Note that {@code
   * isTerminated} is never {@code true} unless either {@code shutdown} or {@code shutdownNow} was
   * called first.
   *
   * @return {@code true} if all tasks have completed following shut down
   */
  @Override
  public boolean isTerminated() {
    // if there are no tasks in the bucket and we have been flagged to shut down
    if (isShutDown && overflowBucket.isEmpty()) {
      // get the time
      long intervalNumber = Instant.now(clock).toEpochMilli() / millisPerInterval;
      // if there are no tasks in any future interval
      for (LockableList<ScheduledTask> tasksForInterval : scheduledTasks.values()) {
        tasksForInterval.lock();
        List<ScheduledTask> tasksForThisInterval =
                removeCompletedOrCancelledTasks(
                        removeOldTasksFromList(tasksForInterval, intervalNumber));
        tasksForInterval.unlock();
        if (!tasksForThisInterval.isEmpty()) {
          return false;
        }
      }
      // then return true
      return true;
    }
    return false;
  }

  /**
   * Blocks until all tasks have completed execution after a shutdown request, or the timeout
   * occurs, or the current thread is interrupted, whichever happens first.
   *
   * <p>this is blocking, and probably shouldn't be used
   *
   * @param timeout the maximum time to wait
   * @param unit the time unit of the timeout argument
   * @return {@code true} if this executor terminated and {@code false} if the timeout elapsed
   *     before termination
   * @throws InterruptedException if interrupted while waiting
   */
  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    Instant endInstant = Instant.now(clock).plus(timeout, unit.toChronoUnit());
    while (Instant.now(clock).isBefore(endInstant)) {
      if (isTerminated()) {
        return true;
      }
      Thread.sleep(100);
    }
    return false;
  }

  /**
   * Submits a value-returning task for execution and returns a Future representing the pending
   * results of the task. The Future's {@code get} method will return the task's result upon
   * successful completion.
   *
   * <p>If you would like to immediately block waiting for a task, you can use constructions of the
   * form {@code result = exec.submit(aCallable).get();}
   *
   * @param task the task to submit
   * @return a Future representing pending completion of the task
   * @throws RejectedExecutionException if the task cannot be scheduled for execution
   * @throws NullPointerException if the task is null
   */
  @Override
  public <T> Future<T> submit(Callable<T> task) {
    return schedule(task, 0, SECONDS);
  }

  /**
   * Submits a Runnable task for execution and returns a Future representing that task. The Future's
   * {@code get} method will return the given result upon successful completion.
   *
   * @param task the task to submit
   * @param result the result to return
   * @return a Future representing pending completion of the task
   * @throws RejectedExecutionException if the task cannot be scheduled for execution
   * @throws NullPointerException if the task is null
   */
  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    // create the scheduled task with the return object
    ScheduledRunnableTask<T> scheduledTask =
            scheduledTaskFactory.getScheduledTask(task, result, Duration.ZERO);
    scheduleTask(scheduledTask);
    return scheduledTask;
  }

  /**
   * Submits a Runnable task for execution and returns a Future representing that task. The Future's
   * {@code get} method will return {@code null} upon <em>successful</em> completion.
   *
   * @param task the task to submit
   * @return a Future representing pending completion of the task
   * @throws RejectedExecutionException if the task cannot be scheduled for execution
   * @throws NullPointerException if the task is null
   */
  @Override
  public Future<?> submit(Runnable task) {
    return schedule(task, 0, SECONDS);
  }

  /**
   * Executes the given tasks, returning a list of Futures holding their status and results when all
   * complete. {@link Future#isDone} is {@code true} for each element of the returned list. Note
   * that a <em>completed</em> task could have terminated either normally or by throwing an
   * exception. The results of this method are undefined if the given collection is modified while
   * this operation is in progress.
   *
   * @param tasks the collection of tasks
   * @return a list of Futures representing the tasks, in the same sequential order as produced by
   *     the iterator for the given task list, each of which has completed
   * @throws NullPointerException if tasks or any of its elements are {@code null}
   * @throws RejectedExecutionException if any task cannot be scheduled for execution
   */
  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) {
    List<Future<T>> futures = new ArrayList<>();
    for (Callable<T> callable : tasks) {
      futures.add(schedule(callable, 0, SECONDS));
    }
    return futures;
  }

  /**
   * Executes the given tasks, returning a list of Futures holding their status and results when all
   * complete or the timeout expires, whichever happens first. {@link Future#isDone} is {@code true}
   * for each element of the returned list. Upon return, tasks that have not completed are
   * cancelled. Note that a <em>completed</em> task could have terminated either normally or by
   * throwing an exception. The results of this method are undefined if the given collection is
   * modified while this operation is in progress.
   *
   * @param tasks the collection of tasks
   * @param timeout the maximum time to wait
   * @param unit the time unit of the timeout argument
   * @param <T> the type of the values returned from the tasks
   * @return a list of Futures representing the tasks, in the same sequential order as produced by
   *     the iterator for the given task list. If the operation did not time out, each task will
   *     have completed. If it did time out, some of these tasks will not have completed.
   * @throws InterruptedException if interrupted while waiting, in which case unfinished tasks are
   *     cancelled
   * @throws NullPointerException if tasks, any of its elements, or unit are {@code null}
   * @throws RejectedExecutionException if any task cannot be scheduled for execution
   */
  @Override
  public <T> List<Future<T>> invokeAll(
          Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
          throws InterruptedException {
    List<Future<T>> futures = invokeAll(tasks);
    // work out the end time
    Instant endInstant = Instant.now(clock).plus(timeout, unit.toChronoUnit());
    boolean allDone = false;
    // if we are still before the end time and we haven't completed
    while (Instant.now(clock).isBefore(endInstant) && !allDone) {
      allDone = true;
      for (Future<T> future : futures) {
        // if the future isn't done, mark all done to false
        allDone = allDone && future.isDone();
      }
      Thread.sleep(100);
    }
    return futures;
  }

  /**
   * Executes the given tasks, returning the result of one that has completed successfully (i.e.,
   * without throwing an exception), if any do. Upon normal or exceptional return, tasks that have
   * not completed are cancelled. The results of this method are undefined if the given collection
   * is modified while this operation is in progress.
   *
   * @param tasks the collection of tasks
   * @param <T> the type of the values returned from the tasks
   * @return the result returned by one of the tasks
   * @throws InterruptedException if interrupted while waiting
   * @throws NullPointerException if tasks or any element task subject to execution is {@code null}
   * @throws IllegalArgumentException if tasks is empty
   * @throws ExecutionException if no task successfully completes
   * @throws RejectedExecutionException if tasks cannot be scheduled for execution
   */
  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
          throws InterruptedException, ExecutionException {
    // wait 100 years
    try {
      return invokeAny(tasks, 36500, TimeUnit.DAYS);
    } catch (TimeoutException e) {
      // wrap a timeout in an executionexception. given the time limit is effectively infinite, this
      // should never happen
      throw new ExecutionException(e);
    }
  }

  /**
   * Executes the given tasks, returning the result of one that has completed successfully (i.e.,
   * without throwing an exception), if any do before the given timeout elapses. Upon normal or
   * exceptional return, tasks that have not completed are cancelled. The results of this method are
   * undefined if the given collection is modified while this operation is in progress.
   *
   * @param tasks the collection of tasks
   * @param timeout the maximum time to wait
   * @param unit the time unit of the timeout argument
   * @param <T> the type of the values returned from the tasks
   * @return the result returned by one of the tasks
   * @throws InterruptedException if interrupted while waiting
   * @throws NullPointerException if tasks, or unit, or any element task subject to execution is
   *     {@code null}
   * @throws TimeoutException if the given timeout elapses before any task successfully completes
   * @throws ExecutionException if no task successfully completes
   * @throws RejectedExecutionException if tasks cannot be scheduled for execution
   */
  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
          throws InterruptedException, ExecutionException, TimeoutException {
    Instant endInstant = Instant.now(clock).plus(timeout, unit.toChronoUnit());
    T result = null;
    List<Future<T>> futures = invokeAll(tasks);

    // whilst there is still time on the clock and we haven't got a result
    while (Instant.now(clock).isBefore(endInstant) && result == null) {
      // check if we have run out of futures to execute
      if (futures.isEmpty()) {
        throw new ExecutionException(new AllTasksFailedToExecuteException());
      }
      // for each item
      Iterator<Future<T>> iterator = futures.iterator();
      while (iterator.hasNext()) {
        Future<T> future = iterator.next();
        if (future.isDone()) {
          // if we are complete, try to get the result
          Optional<T> optionalResult = getResultFromFutureWithErrorHandling(future);
          if (optionalResult.isPresent()) {
            // set the result
            result = optionalResult.get();
          }
          // remove the future from future assessment
          iterator.remove();
        }
      }
      Thread.sleep(100);
    }
    // we have a result so cancel all the other futures
    for (Future<T> future : futures) {
      future.cancel(true);
    }
    if (result == null) {
      throw new TimeoutException();
    }
    return result;
  }

  /**
   * gets the result from a future, logging and supressing errors
   *
   * @param future the future to check
   * @param <T> the return type of the future
   * @return the optional result of the future, if it completed successfully
   * @throws InterruptedException if during the time we waited for the future we got interrupted
   *     (this should never happen if we already checked it was done)
   */
  private <T> Optional<T> getResultFromFutureWithErrorHandling(Future<T> future)
          throws InterruptedException {
    T result = null;
    try {
      result = future.get();
    } catch (ExecutionException | RuntimeException e) {
      logger.log(Level.INFO, "got an exception during invokeAll", e);
    }
    return Optional.ofNullable(result);
  }

  /**
   * Executes the given command at some time in the future. The command may execute in a new thread,
   * in a pooled thread, or in the calling thread, at the discretion of the {@code Executor}
   * implementation.
   *
   * @param command the runnable task
   * @throws RejectedExecutionException if this task cannot be accepted for execution
   * @throws NullPointerException if command is null
   */
  @Override
  public void execute(Runnable command) {
    schedule(command, 0, SECONDS);
  }

  /**
   * remove a repeating task from our list of tasks once is has been cancelled
   *
   * @param repeatingTask the task to remove
   */
  public void removeRepeatingTask(RepeatingTask repeatingTask) {
    repeatingTasks.remove(repeatingTask);
  }

  @VisibleForTesting
  String getServiceTag() {
    return serviceTag;
  }

  @VisibleForTesting
  LockableList<RepeatingTask> getRepeatingTasks() {
    return repeatingTasks;
  }

  @VisibleForTesting
  Logger getLogger() {
    return logger;
  }

  @VisibleForTesting
  ScheduledTaskFactory getScheduledTaskFactory() {
    return scheduledTaskFactory;
  }

  @VisibleForTesting
  long getLastProcessedInterval() {
    return lastProcessedInterval;
  }

  @VisibleForTesting
  void setLastProcessedInterval(long lastProcessedInterval) {
    this.lastProcessedInterval = lastProcessedInterval;
  }

  @VisibleForTesting
  Map<Long, LockableList<ScheduledTask>> getScheduledTasks() {
    return scheduledTasks;
  }

  @VisibleForTesting
  List<ScheduledTask> getOverflowBucket() {
    return new ArrayList<>(overflowBucket);
  }

  @VisibleForTesting
  int getMillisPerInterval() {
    return millisPerInterval;
  }

  @VisibleForTesting
  Map<Long, AtomicInteger> getNumberOfTasksRemainingPerInterval() {
    return numberOfTasksRemainingPerInterval;
  }

  @VisibleForTesting
  int getNumberOfPeers() {
    return numberOfPeers;
  }

  /**
   * update how many peers exist, so we can adjust TPS accordingly
   *
   * @param numberOfPeers how many peers (including this one) are detected
   */
  void setNumberOfPeers(int numberOfPeers) {
    recalculateRemainingTPS(maxTPS, maxTPS, this.numberOfPeers, numberOfPeers);
    this.numberOfPeers = numberOfPeers;
  }
}
