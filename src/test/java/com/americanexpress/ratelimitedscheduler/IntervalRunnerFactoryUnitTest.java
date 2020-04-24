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

import com.americanexpress.ratelimitedscheduler.task.ScheduledRunnableTask;
import com.americanexpress.ratelimitedscheduler.task.ScheduledTask;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Clock;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class IntervalRunnerFactoryUnitTest {
  @Mock
  private ClockSupplier clockSupplier;
  @Mock
  private Clock clock;
  @Mock
  private ConfiguredComponentSupplier configuredComponentSupplier;
  @Mock
  private Executor executor;
  @Mock
  private ScheduledExecutorService scheduledExecutorService;
  @Mock
  private ScheduledRunnableTask taskA;
  @Mock
  private ScheduledRunnableTask taskB;

  @Test
  public void testFactory() {
    when(clockSupplier.getClock()).thenReturn(clock);
    when(configuredComponentSupplier.getScheduledExecutorService()).thenReturn(scheduledExecutorService);
    when(configuredComponentSupplier.getExecutor()).thenReturn(executor);
    List<ScheduledTask> tasks = List.of(taskA, taskB);
    IntervalRunnerFactory intervalRunnerFactory =
            new IntervalRunnerFactory(configuredComponentSupplier, clockSupplier);
    intervalRunnerFactory.setMillisPerInterval(100);
    IntervalRunner intervalRunner = intervalRunnerFactory.getIntervalRunner(100, tasks);

    assertTrue(tasks.containsAll(intervalRunner.getTasks()));
    assertTrue(intervalRunner.getTasks().containsAll(tasks));
    assertEquals(Long.valueOf(100), intervalRunner.getIntervalToRun());
    assertEquals(executor, intervalRunner.getExecutor());
    assertEquals(scheduledExecutorService, intervalRunner.getScheduledExecutorService());
    assertEquals(100, intervalRunner.getMillisPerInterval());
    assertEquals(
            "com.americanexpress.ratelimitedscheduler.IntervalRunner", intervalRunner.getLogger().getName());
  }
}
