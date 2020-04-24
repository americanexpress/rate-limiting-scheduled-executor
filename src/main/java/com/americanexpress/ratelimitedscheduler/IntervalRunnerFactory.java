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

import com.americanexpress.ratelimitedscheduler.task.ScheduledTask;

import com.google.inject.Inject;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.List;
import java.util.logging.Logger;

@Component
/* a factory for creating IntervalRunners */
public class IntervalRunnerFactory {

  private final Clock clock;
  private final Logger logger;
  private final ConfiguredComponentSupplier configuredComponentSupplier;
  private int millisPerInterval;

  /**
   * initialise the factory
   *
   * @param configuredComponentSupplier the supplier of executors for both running the task and scheduling the
   *     trigger
   * @param clockSupplier as a time source
   */
  @Inject
  public IntervalRunnerFactory(ConfiguredComponentSupplier configuredComponentSupplier, ClockSupplier clockSupplier) {
    logger = Logger.getLogger(IntervalRunner.class.getName());
    this.configuredComponentSupplier = configuredComponentSupplier;
    this.clock = clockSupplier.getClock();
  }

  /**
   * get a runner for a given interval
   *
   * @param intervalNumber which inverval to time for
   * @param tasks the list of tasks to run
   * @return the runner
   */
  IntervalRunner getIntervalRunner(long intervalNumber, List<ScheduledTask> tasks) {
    return new IntervalRunner(
            intervalNumber, tasks, millisPerInterval, configuredComponentSupplier, clock, logger);
  }

  /**
   * set how many miliseconds is in each interval. this is used both to time the start, and to time
   * how often to run a task
   *
   * @param millisPerInterval how many milliseconds is in each interval
   */
  void setMillisPerInterval(int millisPerInterval) {
    this.millisPerInterval = millisPerInterval;
  }
}
