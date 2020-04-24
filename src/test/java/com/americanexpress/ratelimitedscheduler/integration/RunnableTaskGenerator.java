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

import com.americanexpress.ratelimitedscheduler.RateLimitedScheduledExecutor;

import java.time.Clock;
import java.time.Instant;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

class RunnableTaskGenerator {
  private final TaskTracker taskTracker;
  private final RateLimitedScheduledExecutor rateLimitedScheduledExecutor;
  private final int totalTasks;
  private final int minDifferenceMillis;
  private final Random random;
  private final int millisRange;
  private final Clock clock;
  private CountDownLatch countDownLatch;

  RunnableTaskGenerator(
          TaskTracker taskTracker,
          RateLimitedScheduledExecutor rateLimitedScheduledExecutor,
          int totalTasks,
          int minDifferenceMillis,
          int maxDifferenceMillis,
          Clock clock) {
    this.taskTracker = taskTracker;
    this.rateLimitedScheduledExecutor = rateLimitedScheduledExecutor;
    this.totalTasks = totalTasks;
    this.minDifferenceMillis = minDifferenceMillis;
    this.clock = clock;
    random = new Random();
    countDownLatch = new CountDownLatch(totalTasks);
    millisRange = maxDifferenceMillis - minDifferenceMillis;
  }

  @SuppressWarnings("SameParameterValue")
  RunnableTaskGenerator(
          RateLimitedScheduledExecutor rateLimitedScheduledExecutor,
          int totalTasks,
          int minDifferenceMillis,
          int maxDifferenceMillis,
          Clock clock) {
    this(
            null,
            rateLimitedScheduledExecutor,
            totalTasks,
            minDifferenceMillis,
            maxDifferenceMillis,
            clock);
  }

  CountDownLatch generateTasks() {
    countDownLatch = new CountDownLatch(totalTasks);
    for (int i = 0; i < totalTasks; i++) {
      generateTask(i);
    }
    return countDownLatch;
  }

  @SuppressWarnings("SameReturnValue")
  private void generateTask(int i) {
    RunnableTask task = new RunnableTask(getScheduledDateTime(), i, countDownLatch, clock, 0);
    if (taskTracker != null) {
      taskTracker.addTask(task);
    }
    ScheduledFuture scheduledFuture =
            rateLimitedScheduledExecutor.schedule(
                    task, task.getDelay().toMillis(), TimeUnit.MILLISECONDS);
    new Thread(
            () -> {
              try {
                scheduledFuture.get();
              } catch (InterruptedException | ExecutionException e) {
                System.out.println(
                        "task " + i + " completed exceptionally because " + e.getMessage());
                countDownLatch.countDown();
              }
            })
            .start();
  }

  private Instant getScheduledDateTime() {
    long nanosToAdd = (random.nextInt(millisRange) + minDifferenceMillis) * 1_000_000L;
    return Instant.now(clock).plusNanos(nanosToAdd);
  }
}
