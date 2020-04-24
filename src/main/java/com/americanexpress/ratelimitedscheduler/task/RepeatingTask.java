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

import com.google.common.annotations.VisibleForTesting;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 * this represents a scheduled repetetive task. This task emits other ScheduledRunnableTasks at the
 * correct time this is the result of scheduleAtFixedRate\scheduleWithFixedDelay
 */
@SuppressWarnings("NullableProblems")
public class RepeatingTask implements ScheduledFuture<Void> {

  private final Runnable runnable;
  private final Duration perTaskDelay;
  private final RepeatingType repeatingType;
  private final Clock clock;
  private final ScheduledTaskFactory scheduledTaskFactory;
  private final RateLimitedScheduledExecutor rateLimitedScheduledExecutor;
  private final Logger logger;
  private final CompletableFuture<Void> completableFuture;
  private final int millisPerInterval;
  private final boolean ensureNoTaskDispatchedEarly;
  // whether or not a delay task is currently running
  private AtomicBoolean isRunning;
  // when this should next be attempted
  private long nextAttemptMillis;

  /**
   * initialise the repeating task. This is normally done by the factory
   *
   * @param runnable the runnable to wrap
   * @param initialDelay how long to delay the first time
   * @param perTaskDelay how long to delay betwen tasks
   * @param ensureNoTaskDispatchedEarly should be false if tasks should be dispatched in the
   *     interval in which they're scheduled, true if we must wait for the interval after that
   * @param repeatingType whether to schedule at a fixed rate (eg every 1 second) or a fixed delay
   *     (eg start 1 second after last one finished)
   * @param millisPerInterval how long each loop interval is, so we know whether to return a task
   *     for interval x or not
   * @param scheduledTaskFactory a factory to generate the ScheduledRunnableTasks when needed
   * @param rateLimitedScheduledExecutor the executor which tasks are shceduled on
   * @param clock a time source, externalised for testing
   * @param logger a logger, externalised for testing
   */
  @SuppressWarnings("squid:S00107")
  RepeatingTask(
          Runnable runnable,
          Duration initialDelay,
          Duration perTaskDelay,
          boolean ensureNoTaskDispatchedEarly,
          RepeatingType repeatingType,
          int millisPerInterval,
          ScheduledTaskFactory scheduledTaskFactory,
          RateLimitedScheduledExecutor rateLimitedScheduledExecutor,
          Clock clock,
          Logger logger) {
    this.ensureNoTaskDispatchedEarly = ensureNoTaskDispatchedEarly;
    this.millisPerInterval = millisPerInterval;
    if (perTaskDelay.toMillis() < millisPerInterval) {
      throw new IllegalArgumentException(
              "we can not schedule repeating tasks more than once per interval");
    }
    if (repeatingType == RepeatingType.DELAY && perTaskDelay.toMillis() < 5 * millisPerInterval) {
      throw new IllegalArgumentException(
              "we can not schedule repeat with fixed delay tasks more than once per 5 intervals");
    }
    this.runnable = runnable;
    this.perTaskDelay = perTaskDelay;
    this.repeatingType = repeatingType;
    this.clock = clock;
    this.scheduledTaskFactory = scheduledTaskFactory;
    this.rateLimitedScheduledExecutor = rateLimitedScheduledExecutor;
    this.logger = logger;
    nextAttemptMillis = Instant.now(clock).plus(initialDelay).toEpochMilli();
    isRunning = new AtomicBoolean(false);
    completableFuture = new CompletableFuture<>();
  }

  /**
   * gets a task for a given interval - this resets the time at which a task should be run so
   * repeated calls will give different results. This is syncronized to avoid issues with being
   * called before the next scheduled time has been calculated correctly
   *
   * @param intervalNumber the interval since the epoch to get events for
   * @return optional task, if the task should not be scheduled to occur within the interval, this
   *     will be empty, otherwise it will contain a scheduledrunnabletask wrapping the task
   */
  public synchronized Optional<ScheduledRunnableTask<Void>> getTaskForIntervalNumber(
          long intervalNumber) {
    // check we're not cancelled and don't provide a task if so
    if (completableFuture.isDone()) {
      return Optional.empty();
    } else {
      // find the boundries of the interval. if we are in the don't dispatch early mode, we only
      // find tasks scheduled in the previous interval
      long startMillis =
              ensureNoTaskDispatchedEarly
                      ? millisPerInterval * (intervalNumber - 1)
                      : millisPerInterval * intervalNumber;

      long endMillis = startMillis + millisPerInterval;
      // work out the timing and return the correct type of task
      if (repeatingType == RepeatingType.RATE) {
        return getRateTaskBetweenMillis(startMillis, endMillis);
      } else {
        return getDelayTaskBetweenMillis(startMillis, endMillis);
      }
    }
  }

  private Optional<ScheduledRunnableTask<Void>> getRateTaskBetweenMillis(
          long startMillis, long endMillis) {
    // work out if the task should occur in this interval
    if (nextAttemptMillis < endMillis) {
      if (nextAttemptMillis < startMillis) {
        logger.fine(
                "we seemed to have missed the correct interval for this task - was it triggered in the past?");
      }
      // recalculate the next interval
      long thisAttemptMillis = nextAttemptMillis;
      nextAttemptMillis = nextAttemptMillis + perTaskDelay.toMillis();
      // create a task and return it
      return Optional.of(
              scheduledTaskFactory.getRepeatingScheduledTask(
                      runnable,
                      Duration.between(Instant.now(clock), Instant.ofEpochMilli(thisAttemptMillis))));
    }
    // no task for this interval
    return Optional.empty();
  }

  private Optional<ScheduledRunnableTask<Void>> getDelayTaskBetweenMillis(
          long startMillis, long endMillis) {
    // work out if the task should occur in this interval, and that it isn't already running
    if (nextAttemptMillis < endMillis && !isRunning.get()) {
      if (nextAttemptMillis < startMillis) {
        logger.fine(
                "we seemed to have missed the correct interval for this task - was it triggered in the past?");
      }
      // mark us as running
      isRunning.set(true);
      // create the task to return
      ScheduledRunnableTask<Void> scheduledTask =
              scheduledTaskFactory.getRepeatingScheduledTask(
                      runnable,
                      Duration.between(Instant.now(clock), Instant.ofEpochMilli(nextAttemptMillis)));
      // recalculate the next interval when the task is complete
      scheduledTask.whenComplete((ignoredVoid, ignoredThrowable) -> rateBasedTaskComplete());
      return Optional.of(scheduledTask);
    }
    // no task for this interval
    return Optional.empty();
  }

  /**
   * triggered by the end of a task, this marks the task as not running and recalculates the next
   * run time
   */
  private void rateBasedTaskComplete() {
    if (isRunning.get()) {
      isRunning.set(false);
    } else {
      logger.warning("told we are no longer running when we are already no longer running");
    }
    nextAttemptMillis = Instant.now(clock).plus(perTaskDelay).toEpochMilli();
  }

  /**
   * Returns the remaining delay associated with this object, in the given time unit.
   *
   * @param unit the time unit
   * @return the remaining delay; zero or negative values indicate that the delay has already
   *     elapsed
   */
  @Override
  public long getDelay(TimeUnit unit) {
    return unit.convert(
            nextAttemptMillis - Instant.now(clock).toEpochMilli(), TimeUnit.MILLISECONDS);
  }

  /**
   * Compares this object with the specified object for order. Returns a negative integer, zero, or
   * a positive integer as this object is less than, equal to, or greater than the specified object.
   *
   * <p>The implementor must ensure {@code sgn(x.compareTo(y)) == -sgn(y.compareTo(x))} for all
   * {@code x} and {@code y}. (This implies that {@code x.compareTo(y)} must throw an exception iff
   * {@code y.compareTo(x)} throws an exception.)
   *
   * <p>The implementor must also ensure that the relation is transitive: {@code (x.compareTo(y) > 0
   * && y.compareTo(z) > 0)} implies {@code x.compareTo(z) > 0}.
   *
   * <p>Finally, the implementor must ensure that {@code x.compareTo(y)==0} implies that {@code
   * sgn(x.compareTo(z)) == sgn(y.compareTo(z))}, for all {@code z}.
   *
   * <p>It is strongly recommended, but <i>not</i> strictly required that {@code (x.compareTo(y)==0)
   * == (x.equals(y))}. Generally speaking, any class that implements the {@code Comparable}
   * interface and violates this condition should clearly indicate this fact. The recommended
   * language is "Note: this class has a natural ordering that is inconsistent with equals."
   *
   * <p>In the foregoing description, the notation {@code sgn(}<i>expression</i>{@code )} designates
   * the mathematical <i>signum</i> function, which is defined to return one of {@code -1}, {@code
   * 0}, or {@code 1} according to whether the value of <i>expression</i> is negative, zero, or
   * positive, respectively.
   *
   * @param otherDelayed the object to be compared.
   * @return a negative integer, zero, or a positive integer as this object is less than, equal to,
   *     or greater than the specified object.
   * @throws NullPointerException if the specified object is null
   * @throws ClassCastException if the specified object's type prevents it from being compared to
   *     this object.
   */
  @Override
  public int compareTo(Delayed otherDelayed) {
    return Long.compare(
            this.getDelay(TimeUnit.MILLISECONDS), otherDelayed.getDelay(TimeUnit.MILLISECONDS));
  }

  /**
   * Attempts to cancel execution of this task. This attempt will fail if the task has already
   * completed, has already been cancelled, or could not be cancelled for some other reason. If
   * successful, and this task has not started when {@code cancel} is called, this task should never
   * run. If the task has already started, then the {@code mayInterruptIfRunning} parameter
   * determines whether the thread executing this task should be interrupted in an attempt to stop
   * the task.
   *
   * <p>After this method returns, subsequent calls to {@link #isDone} will always return {@code
   * true}. Subsequent calls to {@link #isCancelled} will always return {@code true} if this method
   * returned {@code true}.
   *
   * @param mayInterruptIfRunning {@code true} if the thread executing this task should be
   *     interrupted; otherwise, in-progress tasks are allowed to complete
   * @return {@code false} if the task could not be cancelled, typically because it has already
   *     completed normally; {@code true} otherwise
   */
  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    // we make no attempt to actually cancel scheduled tasks here
    rateLimitedScheduledExecutor.removeRepeatingTask(this);
    return completableFuture.cancel(mayInterruptIfRunning);
  }

  /**
   * Returns {@code true} if this task was cancelled before it completed normally.
   *
   * @return {@code true} if this task was cancelled before it completed
   */
  @Override
  public boolean isCancelled() {
    return completableFuture.isCancelled();
  }

  /**
   * Returns {@code true} if this task completed.
   *
   * <p>Completion may be due to normal termination, an exception, or cancellation -- in all of
   * these cases, this method will return {@code true}.
   *
   * @return {@code true} if this task completed
   */
  @Override
  public boolean isDone() {
    return completableFuture.isDone();
  }

  /**
   * Waits if necessary for the computation to complete, and then retrieves its result.
   *
   * @return the computed result
   * @throws CancellationException if the computation was cancelled
   * @throws ExecutionException if the computation threw an exception
   * @throws InterruptedException if the current thread was interrupted while waiting
   */
  @Override
  public Void get() throws InterruptedException, ExecutionException {
    return completableFuture.get();
  }

  /**
   * Waits if necessary for at most the given time for the computation to complete, and then
   * retrieves its result, if available.
   *
   * @param timeout the maximum time to wait
   * @param unit the time unit of the timeout argument
   * @return the computed result
   * @throws CancellationException if the computation was cancelled
   * @throws ExecutionException if the computation threw an exception
   * @throws InterruptedException if the current thread was interrupted while waiting
   * @throws TimeoutException if the wait timed out
   */
  @Override
  public Void get(long timeout, TimeUnit unit)
          throws InterruptedException, ExecutionException, TimeoutException {
    return completableFuture.get(timeout, unit);
  }

  @Override
  public String toString() {
    return "RepeatingTask{"
            + "runnable="
            + runnable
            + ", intervalMillis="
            + perTaskDelay.toMillis()
            + ", repeatingType="
            + repeatingType
            + ", isRunning="
            + isRunning.get()
            + ", nextAttemptMillis="
            + nextAttemptMillis
            + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RepeatingTask that = (RepeatingTask) o;
    return Objects.equals(runnable, that.runnable)
            && Objects.equals(perTaskDelay, that.perTaskDelay)
            && repeatingType == that.repeatingType
            && Objects.equals(rateLimitedScheduledExecutor, that.rateLimitedScheduledExecutor)
            && Objects.equals(nextAttemptMillis, that.nextAttemptMillis);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
            runnable, perTaskDelay, repeatingType, rateLimitedScheduledExecutor, nextAttemptMillis);
  }

  @VisibleForTesting
  Runnable getRunnable() {
    return runnable;
  }

  @VisibleForTesting
  Duration getPerTaskDelay() {
    return perTaskDelay;
  }

  @VisibleForTesting
  RepeatingType getRepeatingType() {
    return repeatingType;
  }

  @VisibleForTesting
  int getMillisPerInterval() {
    return millisPerInterval;
  }

  @VisibleForTesting
  boolean getEnsureNoTaskDispatchedEarly() {
    return ensureNoTaskDispatchedEarly;
  }

  public enum RepeatingType {
    DELAY, // this has been created by calling scheduleWithFixedDelay
    RATE // this has been created by calling scheduleAtFixedRate
  }
}
