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

import com.americanexpress.ratelimitedscheduler.task.ScheduledTaskFactory;

import com.google.inject.Inject;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.logging.Logger;

/**
 * a component that creates RateLimitedScheduledExecutors
 */
@Component
public class RateLimitedScheduledExecutorFactory {
  private final ScheduledTaskFactory scheduledTaskFactory;
  private final Clock clock;
  private final Logger logger;
  private final LockableListFactory lockableListFactory;
  private int millisPerInterval;

  /**
   * initialise the factory
   *
   * @param scheduledTaskFactory the factory used by RateLimitedScheduledExecutors to generate tasks
   * @param lockableListFactory the factory used by RateLimitedScheduledExecutors to generate
   *     LockableLists
   * @param clockSupplier the supplier of clocks used by the RateLimitedScheduledExecutors
   */
  @Inject
  public RateLimitedScheduledExecutorFactory(
          ScheduledTaskFactory scheduledTaskFactory,
          LockableListFactory lockableListFactory,
          ClockSupplier clockSupplier) {
    this.scheduledTaskFactory = scheduledTaskFactory;
    this.lockableListFactory = lockableListFactory;
    this.clock = clockSupplier.getClock();
    this.logger = Logger.getLogger(RateLimitedScheduledExecutor.class.getName());
    millisPerInterval = 1000;
  }

  /**
   * get a single instance of a RateLimitedScheduledExecutor
   *
   * @param serviceTag used by logging in the RateLimitedScheduledExecutor to identify which service
   *     we are referring to
   * @param rateLimitedScheduledExecutorManager the manager which can supply sort orders to the
   *     RateLimitedScheduledExecutor
   * @param ensureNoTaskDispatchedEarly sets a task will be attempted in the correct window (so may
   *     dispatch up to 1 interval * worth * of time early) or in the interval after it is scheduled
   *     (so will never dispatch early)
   * @return the RateLimitedScheduledExecutor
   */
  RateLimitedScheduledExecutor getRateLimitedScheduledExecutorService(
          String serviceTag,
          int numberOfPeers,
          boolean ensureNoTaskDispatchedEarly,
          RateLimitedScheduledExecutorManager rateLimitedScheduledExecutorManager) {
    return new RateLimitedScheduledExecutor(
            serviceTag,
            ensureNoTaskDispatchedEarly,
            numberOfPeers,
            millisPerInterval,
            scheduledTaskFactory,
            lockableListFactory,
            rateLimitedScheduledExecutorManager,
            logger,
            clock);
  }

  /**
   * set how many milliseconds is in each interval
   *
   * @param millisPerInterval how many milliseconds to pass on
   */
  void setMillisPerInterval(int millisPerInterval) {
    this.millisPerInterval = millisPerInterval;
  }
}
