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

import com.google.common.annotations.VisibleForTesting;

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * a wrapper for a callable, that represents a scheduled version of it
 *
 * @param <V> the return type of the callable
 */
public class ScheduledCallableTask<V> extends AbstractScheduledTask<V> {

  private final Callable<V> callable;

  /**
   * Create a new scheduled task wrapping a generic runnable
   *
   * @param callable the runnable to schedule
   * @param delay how long in the future to run the task
   * @param clock a clock (externalised for testing)
   */
  ScheduledCallableTask(Callable<V> callable, Duration delay, Clock clock) {
    super(delay, false, clock);
    this.callable = callable;
  }

  /**
   * run the callable asynchronously on an executor
   *
   * @param executor the executor to run the callable on
   */
  @Override
  public void runAsync(Executor executor) {
    // push to the taskCompletableFuture so it can be cancelled if needed
    taskCompletableFuture =
            CompletableFuture.runAsync(
                    () -> {
                      try {
                        V result = callable.call();
                        // update the future if it worked correctly
                        taskStateFuture.complete(result);
                      } catch (Throwable throwable) {
                        // otherwise mark it as exceptioned
                        taskStateFuture.completeExceptionally(throwable);
                      }
                    },
                    executor);
  }

  @Override
  public String toString() {
    return "ScheduledCallableTask{"
            + "callable="
            + callable
            + ", scheduledTime="
            + scheduledTimeMillis
            + ", wasRequestedImmediately="
            + wasRequestedImmediately
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
    ScheduledCallableTask<?> that = (ScheduledCallableTask<?>) o;
    return wasRequestedImmediately == that.wasRequestedImmediately
            && Objects.equals(callable, that.callable)
            && Objects.equals(scheduledTimeMillis, that.scheduledTimeMillis);
  }

  @Override
  public int hashCode() {
    return Objects.hash(callable, scheduledTimeMillis, wasRequestedImmediately);
  }

  @VisibleForTesting
  Callable<V> getCallable() {
    return callable;
  }
}
