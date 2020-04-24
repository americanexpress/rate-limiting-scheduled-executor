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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.americanexpress.ratelimitedscheduler.task.TaskSorters.SORTED_BY_SCHEDULED_TIME_EARLIEST_FIRST;
import static com.americanexpress.ratelimitedscheduler.task.TaskSorters.SORTED_BY_SCHEDULED_TIME_EARLIEST_FIRST_WITH_REPEATING_FIRST_THEN_IMMEDIATE_THEN_DELAYED;
import static com.americanexpress.ratelimitedscheduler.task.TaskSorters.SORTED_BY_SCHEDULED_TIME_LATEST_FIRST;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TaskSortersTest {

  @Mock
  Runnable runnable;
  @Mock
  Clock clock;

  @Test
  public void testSortOrders() {
    when(clock.instant()).thenReturn(Instant.ofEpochSecond(100));
    ScheduledRunnableTask<Void> scheduledTask10SecondsTimeNotRepeating =
            new ScheduledRunnableTask<>(runnable, Duration.ofSeconds(10), false, clock);
    ScheduledRunnableTask<Void> scheduledTask5SecondsTimeNotRepeating =
            new ScheduledRunnableTask<>(runnable, Duration.ofSeconds(5), false, clock);
    ScheduledRunnableTask<Void> scheduledTask11SecondsTimeRepeating =
            new ScheduledRunnableTask<>(runnable, Duration.ofSeconds(11), true, clock);
    ScheduledRunnableTask<Void> scheduledTask6SecondsTimeRepeating =
            new ScheduledRunnableTask<>(runnable, Duration.ofSeconds(6), true, clock);

    when(clock.instant()).thenReturn(Instant.ofEpochSecond(107));
    ScheduledRunnableTask<Void> scheduledTaskImmediateNotRepeating =
            new ScheduledRunnableTask<>(runnable, Duration.ofSeconds(0), false, clock);
    when(clock.instant()).thenReturn(Instant.ofEpochSecond(108));
    ScheduledRunnableTask<Void> scheduledTaskImmediateRepeating =
            new ScheduledRunnableTask<>(runnable, Duration.ofSeconds(0), true, clock);

    List<ScheduledRunnableTask<Void>> taskList =
            List.of(
                    scheduledTask10SecondsTimeNotRepeating,
                    scheduledTask5SecondsTimeNotRepeating,
                    scheduledTask11SecondsTimeRepeating,
                    scheduledTask6SecondsTimeRepeating,
                    scheduledTaskImmediateNotRepeating,
                    scheduledTaskImmediateRepeating);

    List<ScheduledRunnableTask<Void>> taskListReversed = new ArrayList<>(taskList);
    Collections.reverse(taskListReversed);

    List<ScheduledRunnableTask<Void>> sortedByLatest = new ArrayList<>(taskList);
    sortedByLatest.sort(SORTED_BY_SCHEDULED_TIME_LATEST_FIRST);

    assertEquals(scheduledTask11SecondsTimeRepeating, sortedByLatest.get(0));
    assertEquals(scheduledTask10SecondsTimeNotRepeating, sortedByLatest.get(1));
    assertEquals(scheduledTaskImmediateRepeating, sortedByLatest.get(2));
    assertEquals(scheduledTaskImmediateNotRepeating, sortedByLatest.get(3));
    assertEquals(scheduledTask6SecondsTimeRepeating, sortedByLatest.get(4));
    assertEquals(scheduledTask5SecondsTimeNotRepeating, sortedByLatest.get(5));

    sortedByLatest.sort(SORTED_BY_SCHEDULED_TIME_LATEST_FIRST);

    assertEquals(scheduledTask11SecondsTimeRepeating, sortedByLatest.get(0));
    assertEquals(scheduledTask10SecondsTimeNotRepeating, sortedByLatest.get(1));
    assertEquals(scheduledTaskImmediateRepeating, sortedByLatest.get(2));
    assertEquals(scheduledTaskImmediateNotRepeating, sortedByLatest.get(3));
    assertEquals(scheduledTask6SecondsTimeRepeating, sortedByLatest.get(4));
    assertEquals(scheduledTask5SecondsTimeNotRepeating, sortedByLatest.get(5));

    List<ScheduledRunnableTask<Void>> sortedByEarliest = new ArrayList<>(taskList);

    sortedByEarliest.sort(SORTED_BY_SCHEDULED_TIME_EARLIEST_FIRST);

    assertEquals(scheduledTask5SecondsTimeNotRepeating, sortedByEarliest.get(0));
    assertEquals(scheduledTask6SecondsTimeRepeating, sortedByEarliest.get(1));
    assertEquals(scheduledTaskImmediateNotRepeating, sortedByEarliest.get(2));
    assertEquals(scheduledTaskImmediateRepeating, sortedByEarliest.get(3));
    assertEquals(scheduledTask10SecondsTimeNotRepeating, sortedByEarliest.get(4));
    assertEquals(scheduledTask11SecondsTimeRepeating, sortedByEarliest.get(5));

    sortedByEarliest = new ArrayList<>(taskListReversed);

    sortedByEarliest.sort(SORTED_BY_SCHEDULED_TIME_EARLIEST_FIRST);

    assertEquals(scheduledTask5SecondsTimeNotRepeating, sortedByEarliest.get(0));
    assertEquals(scheduledTask6SecondsTimeRepeating, sortedByEarliest.get(1));
    assertEquals(scheduledTaskImmediateNotRepeating, sortedByEarliest.get(2));
    assertEquals(scheduledTaskImmediateRepeating, sortedByEarliest.get(3));
    assertEquals(scheduledTask10SecondsTimeNotRepeating, sortedByEarliest.get(4));
    assertEquals(scheduledTask11SecondsTimeRepeating, sortedByEarliest.get(5));

    List<ScheduledRunnableTask<Void>> sortedByEarliestWithRepeatingThenImmediate =
            new ArrayList<>(taskList);

    sortedByEarliestWithRepeatingThenImmediate.sort(
            SORTED_BY_SCHEDULED_TIME_EARLIEST_FIRST_WITH_REPEATING_FIRST_THEN_IMMEDIATE_THEN_DELAYED);

    assertEquals(
            scheduledTaskImmediateRepeating, sortedByEarliestWithRepeatingThenImmediate.get(0));
    assertEquals(
            scheduledTask6SecondsTimeRepeating, sortedByEarliestWithRepeatingThenImmediate.get(1));
    assertEquals(
            scheduledTask11SecondsTimeRepeating, sortedByEarliestWithRepeatingThenImmediate.get(2));
    assertEquals(
            scheduledTaskImmediateNotRepeating, sortedByEarliestWithRepeatingThenImmediate.get(3));
    assertEquals(
            scheduledTask5SecondsTimeNotRepeating, sortedByEarliestWithRepeatingThenImmediate.get(4));
    assertEquals(
            scheduledTask10SecondsTimeNotRepeating, sortedByEarliestWithRepeatingThenImmediate.get(5));

    sortedByEarliestWithRepeatingThenImmediate = new ArrayList<>(taskListReversed);
    sortedByEarliestWithRepeatingThenImmediate.sort(
            SORTED_BY_SCHEDULED_TIME_EARLIEST_FIRST_WITH_REPEATING_FIRST_THEN_IMMEDIATE_THEN_DELAYED);

    assertEquals(
            scheduledTaskImmediateRepeating, sortedByEarliestWithRepeatingThenImmediate.get(0));
    assertEquals(
            scheduledTask6SecondsTimeRepeating, sortedByEarliestWithRepeatingThenImmediate.get(1));
    assertEquals(
            scheduledTask11SecondsTimeRepeating, sortedByEarliestWithRepeatingThenImmediate.get(2));
    assertEquals(
            scheduledTaskImmediateNotRepeating, sortedByEarliestWithRepeatingThenImmediate.get(3));
    assertEquals(
            scheduledTask5SecondsTimeNotRepeating, sortedByEarliestWithRepeatingThenImmediate.get(4));
    assertEquals(
            scheduledTask10SecondsTimeNotRepeating, sortedByEarliestWithRepeatingThenImmediate.get(5));
  }
}
