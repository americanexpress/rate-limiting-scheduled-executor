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

import com.americanexpress.ratelimitedscheduler.exceptions.CollectionHasBeenEmptiedException;
import com.americanexpress.ratelimitedscheduler.task.ScheduledTask;

import com.google.common.annotations.VisibleForTesting;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

class IntervalRunner {
  private static final long NANO_PRESTART_LENGTH = 100_000_000;
  private final Long intervalToRun;
  private final LockableList<ScheduledTask> tasks;
  private final AtomicInteger counter;
  private final CompletableFuture<Void> future;
  private final ScheduledExecutorService scheduledExecutorService;
  private final Clock clock;
  private final Logger logger;
  private final Executor executor;
  private final Instant startInstant;
  private final int millisPerInterval;
  private ScheduledFuture taskFuture;
  private List<ScheduledTask> tasksToRun;
  /**
   * creates a new runner
   *
   * @param intervalToRun the interval in which these tasks should be run
   * @param tasks the tasks to run
   * @param configuredComponentSupplier this is used to run the actual task itself
   * @param clock a time source to figure when in the future to start the tasks (externalised for
   * @param logger to log things with
   */
  IntervalRunner(
          long intervalToRun,
          List<ScheduledTask> tasks,
          int millisPerInterval,
          ConfiguredComponentSupplier configuredComponentSupplier,
          Clock clock,
          Logger logger) {
    this.intervalToRun = intervalToRun;
    this.tasks = new LockableList<>(tasks);
    scheduledExecutorService = configuredComponentSupplier.getScheduledExecutorService();
    executor = configuredComponentSupplier.getExecutor();
    this.clock = clock;
    this.logger = logger;
    this.millisPerInterval = millisPerInterval;
    counter = new AtomicInteger(0);
    future = new CompletableFuture<>();
    startInstant = Instant.ofEpochMilli(intervalToRun * millisPerInterval);
  }

  /**
   * submit the initial task to the scheduler
   *
   * @return a future which is completed once all the tasks are run
   */
  CompletableFuture<Void> submitTasksToScheduler() {
    // work out when to start the initial task - we start this 0.1 seconds early to ensure it has
    // time to complete before actually running
    long nanosTillWarmMoment =
            Duration.between(Instant.now(clock), startInstant).toNanos() - NANO_PRESTART_LENGTH;
    logger.finer("sending " + tasks.size() + " tasks to the scheduler");
    scheduledExecutorService.schedule(runWarmupTask(), nanosTillWarmMoment, TimeUnit.NANOSECONDS);
    return future;
  }

  /**
   * add a task to the runner, if it has not already started
   *
   * @param scheduledTask the task to add
   * @throws RejectedExecutionException if the runner has already started
   */
  void addTask(ScheduledTask scheduledTask) {
    try {
      tasks.add(scheduledTask);
    } catch (CollectionHasBeenEmptiedException ignored) {
      throw new RejectedExecutionException("this interval runner is already running");
    }
  }

  /**
   * runs the initial task. This is scheduled for 0.1 seconds before the interval starts so that
   * tasks can be added right to the last moment
   *
   * @return the Runnable wrapping the warmup activity
   */
  private Runnable runWarmupTask() {
    return () -> {
      // wrap into an arraylist and lock the old collection
      tasksToRun = new ArrayList<>(tasks.empty());
      // randomly shuffle the list so a given exectors tasks are randomly distributed
      Collections.shuffle(this.tasks);
      final int numberOfTasks = tasksToRun.size();
      logger.finer("running initial task with " + numberOfTasks + " tasks");
      // don't bother scheduling anything if there are no tasks
      if (numberOfTasks > 0) {
        // work out how often to run something
        long nanosPerRequest = millisPerInterval * 1_000_000 / numberOfTasks;
        // work out when to start the tasks
        long nanosTillStartMoment = Duration.between(Instant.now(clock), startInstant).toNanos();
        taskFuture =
                scheduledExecutorService.scheduleAtFixedRate(
                        runNextTask(numberOfTasks),
                        nanosTillStartMoment,
                        nanosPerRequest,
                        TimeUnit.NANOSECONDS);
      } else {
        // just complete the future to say we are all done
        future.complete(null);
      }
    };
  }

  @SuppressWarnings({"squid:S2629"})
  private Runnable runNextTask(int numberOfTasks) {
    return () -> {
      int counterValue = counter.getAndIncrement();
      // if we haven't finished
      if (counterValue < numberOfTasks) {
        ScheduledTask scheduledTask = tasksToRun.get(counterValue);
        //
        if (scheduledTask.isDone()) {
          // skip the task if it was cancelled last minute
          logger.finest(String.format("task %s was cancelled so not running it", scheduledTask));
        } else {
          // execute the task on the executor service
          scheduledTask.runAsync(executor);
        }
      } else {
        // we have run out of tasks
        logger.finest(
                "completed all tasks so cancelling runNextTask future and completing our individual future");
        // we should never have null here
        if (taskFuture != null) {
          taskFuture.cancel(false);
        }
        // mark the run as complete
        future.complete(null);
      }
    };
  }

  @VisibleForTesting
  Long getIntervalToRun() {
    return intervalToRun;
  }

  @VisibleForTesting
  List<ScheduledTask> getTasks() {
    return new ArrayList<>(tasks);
  }

  @VisibleForTesting
  ScheduledExecutorService getScheduledExecutorService() {
    return scheduledExecutorService;
  }

  @VisibleForTesting
  Executor getExecutor() {
    return executor;
  }

  @VisibleForTesting
  Logger getLogger() {
    return logger;
  }

  @VisibleForTesting
  int getMillisPerInterval() {
    return millisPerInterval;
  }
}
