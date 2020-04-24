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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public class ScheduledRunnableTask<V> extends AbstractScheduledTask<V> {

  private final Runnable runnable;

  private final V returnObject;
  /**
   * Create a new scheduled task wrapping a generic runnable
   *
   * @param runnable the runnable to schedule
   * @param delay how long in the future to run the task
   * @param clock a clock (externalised for testing)
   */
  ScheduledRunnableTask(Runnable runnable, Duration delay, boolean isRepeating, Clock clock) {
    this(runnable, null, delay, isRepeating, clock);
  }
  /**
   * Create a new scheduled task wrapping a runnable with a return object
   *
   * @param runnable the runnable to schedule
   * @param returnObject the object to return on completion - null for typical runnables
   * @param delay how long in the future to run the task
   * @param clock a clock (externalised for testing)
   */
  ScheduledRunnableTask(
          Runnable runnable, V returnObject, Duration delay, boolean isRepeating, Clock clock) {
    super(delay, isRepeating, clock);
    this.runnable = runnable;
    this.returnObject = returnObject;
  }

  public Runnable getRunnable() {
    return runnable;
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
                        runnable.run();
                        // update the future if it worked correctly
                        taskStateFuture.complete(returnObject);
                      } catch (Throwable throwable) {
                        // capture the exception
                        taskStateFuture.completeExceptionally(throwable);
                      }
                    },
                    executor);
  }

  @Override
  public String toString() {
    return "ScheduledRunnableTask{"
            + "runnable="
            + runnable
            + ", returnObject="
            + returnObject
            + ", scheduledTime="
            + scheduledTimeMillis
            + ", wasRequestedImmediately="
            + wasRequestedImmediately
            + ", isRepeating="
            + isRepeating
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
    ScheduledRunnableTask<?> that = (ScheduledRunnableTask<?>) o;
    return wasRequestedImmediately == that.wasRequestedImmediately
            && isRepeating == that.isRepeating
            && Objects.equals(runnable, that.runnable)
            && Objects.equals(returnObject, that.returnObject)
            && Objects.equals(scheduledTimeMillis, that.scheduledTimeMillis);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
            runnable, returnObject, scheduledTimeMillis, wasRequestedImmediately, isRepeating);
  }

  @VisibleForTesting
  Object getReturnObject() {
    return returnObject;
  }
}
