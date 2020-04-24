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

import com.americanexpress.ratelimitedscheduler.ClockSupplier;
import com.americanexpress.ratelimitedscheduler.RateLimitedScheduledExecutor;

import com.google.inject.Inject;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.logging.Logger;

/**
 * creates scheduled tasks of various types. Externalised to make testing easier
 */
@SuppressWarnings("WeakerAccess")
@Component
public class ScheduledTaskFactory {
  private final Clock clock;
  private int millisPerInterval;

  /**
   * creates the factory
   *
   * @param clockSupplier a timesource
   */
  @Inject
  public ScheduledTaskFactory(ClockSupplier clockSupplier) {
    this.clock = clockSupplier.getClock();
  }

  /**
   * gets a scheduledRunnableTask which has come from a repeating source
   *
   * @param task the task to wrap
   * @param delay when to execute it
   * @return the wrapped task
   */
  public ScheduledRunnableTask<Void> getRepeatingScheduledTask(
          @Nonnull Runnable task, @Nonnull Duration delay) {
    return new ScheduledRunnableTask<>(task, delay, true, clock);
  }
  /**
   * gets a scheduledRunnableTask which has come from a non-repeating source
   *
   * @param task the task to wrap
   * @param delay when to execute it
   * @return the wrapped task
   */
  public ScheduledRunnableTask<Void> getScheduledTask(
          @Nonnull Runnable task, @Nonnull Duration delay) {
    return new ScheduledRunnableTask<>(task, delay, false, clock);
  }

  /**
   * gets a scheduledRunnableTask which has come from a non-repeating source and has a pre-defined
   * return object when get is called on the task
   *
   * @param <V> the object to return type
   * @param task the task to wrap
   * @param returnObject the object to return when we are complete
   * @param delay when to execute it
   * @return the wrapped task
   */
  public <V> ScheduledRunnableTask<V> getScheduledTask(
          @Nonnull Runnable task, @Nonnull V returnObject, @Nonnull Duration delay) {
    return new ScheduledRunnableTask<>(task, returnObject, delay, false, clock);
  }

  /**
   * gets a scheduledCallableTask wrapping a callable
   *
   * @param <V> the type of object returned from the callable
   * @param task the callable to wrap
   * @param delay when to execute it
   * @return the wrapped task
   */
  public <V> ScheduledCallableTask<V> getScheduledTask(
          @Nonnull Callable<V> task, @Nonnull Duration delay) {
    return new ScheduledCallableTask<>(task, delay, clock);
  }

  /**
   * get an object that represents a repeating task
   *
   * @param task the runnable to wrap
   * @param initialDelay how long to wait before the first run
   * @param intervalDuration how long to wait between runs
   * @param ensureNoTaskDispatchedEarly should be false if tasks should be dispatched in the *
   *     interval in which they're scheduled, true if we must wait for the interval after that
   * @param repeatingType whether to time from the start of run 1 to the start of run 2, or from the
   *     end of run 1 to the start of run 2
   * @param rateLimitedScheduledExecutor where to execute it
   * @return the wrapped task
   */
  public RepeatingTask getRepeatingTask(
          @Nonnull Runnable task,
          @Nonnull Duration initialDelay,
          @Nonnull Duration intervalDuration,
          boolean ensureNoTaskDispatchedEarly,
          @Nonnull RepeatingTask.RepeatingType repeatingType,
          @Nonnull RateLimitedScheduledExecutor rateLimitedScheduledExecutor) {
    return new RepeatingTask(
            task,
            initialDelay,
            intervalDuration,
            ensureNoTaskDispatchedEarly,
            repeatingType,
            millisPerInterval,
            this,
            rateLimitedScheduledExecutor,
            clock,
            Logger.getLogger(RepeatingTask.class.getName()));
  }

  /**
   * set how many milliseconds is in each loop interval. this is used by the repeatingtasks to
   * assess whether a task should be repeated in this interval
   *
   * @param millisPerInterval how many milliseconds in each interval
   */
  public void setMillisPerInterval(int millisPerInterval) {
    this.millisPerInterval = millisPerInterval;
  }
}
