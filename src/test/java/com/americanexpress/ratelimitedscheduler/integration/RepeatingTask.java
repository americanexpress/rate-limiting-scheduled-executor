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

package com.americanexpress.ratelimitedscheduler.integration;

import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * a task that is meant for use in scheduleAtAFixedRate and scheduleWithAFixedDelay - it just
 * creates and stores a task with a sleep timer
 */
@SuppressWarnings("SameParameterValue")
class RepeatingTask implements Runnable {
  private final TaskTracker taskTracker;
  private final AtomicInteger counter;
  private final Clock clock;
  private final CountDownLatch countDownLatch;
  private final long sleepMillis;

  RepeatingTask(long sleepMillis) {
    this(null, sleepMillis);
  }

  RepeatingTask(TaskTracker taskTracker, long sleepMillis) {
    this(taskTracker, null, sleepMillis);
  }

  RepeatingTask(TaskTracker taskTracker, CountDownLatch countDownLatch, long sleepMillis) {
    this.taskTracker = taskTracker;
    this.countDownLatch = countDownLatch;
    this.sleepMillis = sleepMillis;
    counter = new AtomicInteger();
    clock = Clock.systemUTC();
  }

  @Override
  public void run() {
    RunnableTask task =
            new RunnableTask(
                    Instant.now(), counter.getAndIncrement(), countDownLatch, clock, sleepMillis);
    if (taskTracker != null) {
      taskTracker.addTask(task);
    }
    task.run();
  }
}
