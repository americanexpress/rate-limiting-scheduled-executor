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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.function.BiConsumer;

@SuppressWarnings("unused")
public interface ScheduledTask<V> extends ScheduledFuture<V> {

  /**
   * when this task is scheduled for
   *
   * @return the scheduled time (local clock) in millis since the epoch
   */
  long getScheduledTimeMillis();

  /**
   * an action to take once the activity has completed
   *
   * @param action a biconsumer that has either V or a throwable, depending on whether the task was
   *     executed successfully or not
   * @return a completable future which has wrapped that task
   */
  CompletableFuture<V> whenComplete(BiConsumer<? super V, ? super Throwable> action);

  /**
   * forces the task to be cancelled without execution because the time limit for it has passed
   */
  void timeOut();
  /**
   * was the scheduled task requested immediately or at some time in the future
   *
   * @return true if it was requested immediately (this might be used for sorting)
   */
  boolean wasRequestedImmediately();
  /**
   * whether the task has come from a repeating source
   *
   * @return true if the source of this task is repeating
   *     (scheduleAtFixedRate\scheduleWithFixedDelay)
   */
  boolean isRepeating();

  /**
   * run the callable asynchronously on an executor
   *
   * @param executor the executor to run the callable on
   */
  void runAsync(Executor executor);

  /**
   * returns whether the task has been completed exceptionally (or cancelled)
   *
   * @return true if the task has executed and broke, or was cancelled
   */
  boolean isCompletedExceptionally();
}
