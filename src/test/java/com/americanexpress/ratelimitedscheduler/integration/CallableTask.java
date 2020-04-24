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

import org.springframework.lang.Nullable;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

@SuppressWarnings({"SameParameterValue", "unused"})
public class CallableTask implements Callable<Integer>, Task {
  private final Instant scheduleDateTime;
  private final int id;
  private final CountDownLatch countDownLatch;
  private final Clock clock;
  private final long sleepMillis;
  private Instant triggeredDateTime;
  private Instant completedDateTime;

  CallableTask(
          Instant scheduleDateTime,
          int id,
          CountDownLatch countDownLatch,
          Clock clock,
          long sleepMillis) {
    this.scheduleDateTime = scheduleDateTime;
    this.id = id;
    this.countDownLatch = countDownLatch;
    this.clock = clock;
    this.sleepMillis = sleepMillis;
  }

  public Duration getDelay() {
    return Duration.between(Instant.now(clock), scheduleDateTime);
  }

  @Override
  public int getId() {
    return id;
  }

  @Nullable
  @Override
  public Instant getTriggeredDateTime() {
    return triggeredDateTime;
  }

  @Nullable
  @Override
  public Instant getCompletedDateTime() {
    return completedDateTime;
  }

  @Override
  public long getTimeDifferenceMillis() {
    if (triggeredDateTime == null) {
      return 0;
    } else {
      return Duration.between(scheduleDateTime, triggeredDateTime).toMillis();
    }
  }

  public long getTimeDifferenceSeconds() {
    if (triggeredDateTime == null) {
      return 0;
    } else {
      // add a millisecond to avoid extreme clock scenarios
      return (triggeredDateTime.plusNanos(1_000_000).toEpochMilli()
              - scheduleDateTime.toEpochMilli())
              / 1000;
    }
  }

  @Override
  public String toString() {
    return "Task{"
            + "scheduleDateTime="
            + scheduleDateTime
            + ", id="
            + id
            + ", completedDateTime="
            + triggeredDateTime
            + ", differenceMillis="
            + getTimeDifferenceMillis()
            + ", differenceWholeSeconds="
            + getTimeDifferenceSeconds()
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
    CallableTask task = (CallableTask) o;
    return Objects.equals(scheduleDateTime, task.scheduleDateTime)
            && Objects.equals(id, task.id)
            && Objects.equals(triggeredDateTime, task.triggeredDateTime);
  }

  @Override
  public int hashCode() {
    return Objects.hash(scheduleDateTime, id, triggeredDateTime);
  }

  @Override
  public Integer call() {

    triggeredDateTime = Instant.now(clock);
    if (countDownLatch != null) {
      countDownLatch.countDown();
    }
    try {
      Thread.sleep(sleepMillis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    completedDateTime = Instant.now(clock);
    return id;
  }
}
