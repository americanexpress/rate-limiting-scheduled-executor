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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Clock;
import java.time.Instant;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RateLimitedScheduledExecutorFactoryUnitTest {
  @Mock
  private Clock clock;
  @Mock
  private ScheduledTaskFactory scheduledTaskFactory;
  @Mock
  private LockableListFactory lockableListFactory;
  @Mock
  private ClockSupplier clockSupplier;
  @Mock
  private RateLimitedScheduledExecutorManager rateLimitedScheduledExecutorManager;

  @Test
  public void testRateLimitedScheduledExecutorFactory() {
    when(clockSupplier.getClock()).thenReturn(clock);
    when(clock.instant()).thenReturn(Instant.ofEpochSecond(100));
    RateLimitedScheduledExecutorFactory rateLimitedScheduledExecutorFactory =
            new RateLimitedScheduledExecutorFactory(
                    scheduledTaskFactory, lockableListFactory, clockSupplier);

    RateLimitedScheduledExecutor rateLimitedScheduledExecutor =
            rateLimitedScheduledExecutorFactory.getRateLimitedScheduledExecutorService(
                    "tag", 2, true, rateLimitedScheduledExecutorManager);

    assertEquals("tag", rateLimitedScheduledExecutor.getServiceTag());
    assertEquals(
            "com.americanexpress.ratelimitedscheduler.RateLimitedScheduledExecutor",
            rateLimitedScheduledExecutor.getLogger().getName());
    assertEquals(scheduledTaskFactory, rateLimitedScheduledExecutor.getScheduledTaskFactory());
    assertEquals(95, rateLimitedScheduledExecutor.getLastProcessedInterval());
    assertEquals(1000, rateLimitedScheduledExecutor.getMillisPerInterval());
    assertEquals(2, rateLimitedScheduledExecutor.getNumberOfPeers());
    assertTrue(rateLimitedScheduledExecutor.isEnsureNoTaskDispatchedEarly());
  }

  @Test
  public void testSetMillisWorks() {
    when(clockSupplier.getClock()).thenReturn(clock);
    when(clock.instant()).thenReturn(Instant.ofEpochSecond(100));
    RateLimitedScheduledExecutorFactory rateLimitedScheduledExecutorFactory =
            new RateLimitedScheduledExecutorFactory(
                    scheduledTaskFactory, lockableListFactory, clockSupplier);
    rateLimitedScheduledExecutorFactory.setMillisPerInterval(100);
    RateLimitedScheduledExecutor rateLimitedScheduledExecutor =
            rateLimitedScheduledExecutorFactory.getRateLimitedScheduledExecutorService(
                    "tag", 1, false, rateLimitedScheduledExecutorManager);

    assertEquals("tag", rateLimitedScheduledExecutor.getServiceTag());
    assertEquals(
            "com.americanexpress.ratelimitedscheduler.RateLimitedScheduledExecutor",
            rateLimitedScheduledExecutor.getLogger().getName());
    assertEquals(scheduledTaskFactory, rateLimitedScheduledExecutor.getScheduledTaskFactory());
    assertEquals(950, rateLimitedScheduledExecutor.getLastProcessedInterval());
    assertEquals(100, rateLimitedScheduledExecutor.getMillisPerInterval());
    assertEquals(1, rateLimitedScheduledExecutor.getNumberOfPeers());
    assertFalse(rateLimitedScheduledExecutor.isEnsureNoTaskDispatchedEarly());
  }
}
