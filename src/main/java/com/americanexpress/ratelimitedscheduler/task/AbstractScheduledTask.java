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

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * contains logic which is generic between the ScheduledCallableTask and the ScheduledRunnableTask
 */
@SuppressWarnings("NullableProblems")
public abstract class AbstractScheduledTask<V> implements ScheduledTask<V> {

  /**
   * the time at which this task was scheduled
   */
  final long scheduledTimeMillis;
  /** whether the task was requested for immediate execution (can be used by sorting */
  final boolean wasRequestedImmediately;
  /** whether this task is from a repeating source */
  final boolean isRepeating;
  /** this is a cheat for managing the state of the task */
  final CompletableFuture<V> taskStateFuture;
  /** time source to manage testing better */
  private final Clock clock;
  /** manages the state of the task once submitted for execution */
  CompletableFuture<Void> taskCompletableFuture;

  /**
   * initialise the abstract task
   *
   * @param delay how far in the future to execute the task
   * @param isRepeating whether it is from a repeating source
   *     (scheduleAtFixedRate\scheduleWithFixedDelay
   * @param clock an externalised time source
   */
  AbstractScheduledTask(Duration delay, boolean isRepeating, Clock clock) {
    if (delay.isNegative()) {
      delay = Duration.ofMillis(1);
    }
    this.clock = clock;
    this.isRepeating = isRepeating;
    scheduledTimeMillis = Instant.now(clock).toEpochMilli() + delay.toMillis();
    wasRequestedImmediately = delay.isZero();
    taskStateFuture = new CompletableFuture<>();
  }

  /**
   * get the time at which the record should be tried
   *
   * @return the schedule time
   */
  @Override
  public long getScheduledTimeMillis() {
    return scheduledTimeMillis;
  }

  /**
   * returns whether the task has been completed exceptionally (or cancelled)
   *
   * @return true if the task has executed and broke, or was cancelled
   */
  @Override
  public boolean isCompletedExceptionally() {
    return taskStateFuture.isCompletedExceptionally();
  }

  /** forces the task to be cancelled without execution because the time limit for it has passed */
  @Override
  public void timeOut() {
    taskStateFuture.completeExceptionally(
            new TimeoutException("this tasks was not executed prior to being timed out"));
  }

  /**
   * an action to take once the activity has completed
   *
   * @param action a biconsumer that has either V or a throwable, depending on whether the task was
   *     executed successfully or not
   * @return a completable future which has wrapped that task
   */
  @Override
  public CompletableFuture<V> whenComplete(BiConsumer<? super V, ? super Throwable> action) {
    return taskStateFuture.whenComplete(action);
  }

  /**
   * whether the task has come from a repeating source
   *
   * @return true if the source of this task is repeating
   *     (scheduleAtFixedRate\scheduleWithFixedDelay)
   */
  @Override
  public boolean isRepeating() {
    return isRepeating;
  }

  /**
   * was the scheduled task requested immediately or at some time in the future
   *
   * @return true if it was requested immediately (this might be used for sorting)
   */
  @Override
  public boolean wasRequestedImmediately() {
    return wasRequestedImmediately;
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
            scheduledTimeMillis - Instant.now(clock).toEpochMilli(), TimeUnit.MILLISECONDS);
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
    return Long.compare(getDelay(NANOSECONDS), otherDelayed.getDelay(NANOSECONDS));
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
    taskStateFuture.cancel(true);
    if (taskCompletableFuture != null) {
      return taskCompletableFuture.cancel(mayInterruptIfRunning);
    }
    return true;
  }

  /**
   * Returns {@code true} if this task was cancelled before it completed normally.
   *
   * @return {@code true} if this task was cancelled before it completed
   */
  @Override
  public boolean isCancelled() {
    return taskStateFuture.isCancelled();
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
    return taskStateFuture.isDone();
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
  public V get() throws InterruptedException, ExecutionException {
    return taskStateFuture.get();
  }

  /**
   * Waits if necessary for at most the given time for the computation to complete, and then
   * retrieves its result, if available.
   *
   * @param timeout the maximum time to wait
   * @param unit the time unit of the timeout argument
   * @return the computed result
   * @throws ExecutionException if the computation threw an exception
   * @throws InterruptedException if the current thread was interrupted while waiting
   * @throws TimeoutException if the wait timed out
   */
  @Override
  public V get(long timeout, TimeUnit unit)
          throws InterruptedException, ExecutionException, TimeoutException {
    return taskStateFuture.get(timeout, unit);
  }
}
