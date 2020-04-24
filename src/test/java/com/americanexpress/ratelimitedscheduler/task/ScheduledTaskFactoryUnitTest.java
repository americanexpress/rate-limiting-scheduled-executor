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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static com.americanexpress.ratelimitedscheduler.task.RepeatingTask.RepeatingType.DELAY;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ScheduledTaskFactoryUnitTest {
  @Mock
  RateLimitedScheduledExecutor rateLimitedScheduledExecutor;
  @Mock
  private Clock clock;
  @Mock
  private Runnable runnable;
  @Mock
  private Callable<String> callable;
  @Mock
  private ClockSupplier clockSupplier;
  private ScheduledTaskFactory scheduledTaskFactory;

  @Before
  public void setUp() {
    when(clockSupplier.getClock()).thenReturn(clock);
    when(clock.instant()).thenReturn(Instant.ofEpochSecond(100));
    scheduledTaskFactory = new ScheduledTaskFactory(clockSupplier);
  }

  @Test
  public void testGetScheduledTask() {

    ScheduledRunnableTask scheduledTask =
            scheduledTaskFactory.getScheduledTask(runnable, Duration.ofHours(1));
    assertEquals(runnable, scheduledTask.getRunnable());
    assertEquals(3700000L, scheduledTask.getScheduledTimeMillis());
  }

  @Test
  public void testGetRepeatingScheduledTask() {
    ScheduledRunnableTask scheduledTask =
            scheduledTaskFactory.getRepeatingScheduledTask(runnable, Duration.ofHours(1));
    assertEquals(runnable, scheduledTask.getRunnable());
    assertEquals(3700000L, scheduledTask.getScheduledTimeMillis());
    assertTrue(scheduledTask.isRepeating());
  }

  @Test
  public void testGetTaskWithReturnObject() {
    ScheduledRunnableTask<String> scheduledTask =
            scheduledTaskFactory.getScheduledTask(runnable, "hello", Duration.ofHours(1));
    assertEquals("hello", scheduledTask.getReturnObject());
  }

  @Test
  public void testScheduledCallable() {
    ScheduledCallableTask<String> scheduledTask =
            scheduledTaskFactory.getScheduledTask(callable, Duration.ofHours(1));

    assertEquals(callable, scheduledTask.getCallable());
    assertEquals(3700000L, scheduledTask.getScheduledTimeMillis());
  }

  @Test
  public void testGetRepeatingTask() {
    scheduledTaskFactory.setMillisPerInterval(100);
    RepeatingTask repeatingTask =
            scheduledTaskFactory.getRepeatingTask(
                    runnable,
                    Duration.ofHours(1),
                    Duration.ofHours(2),
                    false,
                    DELAY,
                    rateLimitedScheduledExecutor);
    assertEquals(runnable, repeatingTask.getRunnable());
    assertEquals(Duration.ofHours(2), repeatingTask.getPerTaskDelay());
    assertEquals(1, repeatingTask.getDelay(TimeUnit.HOURS));
    assertEquals(DELAY, repeatingTask.getRepeatingType());
    assertEquals(100, repeatingTask.getMillisPerInterval());
    assertFalse(repeatingTask.getEnsureNoTaskDispatchedEarly());
  }
}
