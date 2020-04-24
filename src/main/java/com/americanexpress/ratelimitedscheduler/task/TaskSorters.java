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

import java.util.Comparator;

/**
 * some sort helpers that can be used to sort scheduled tasks. this is important when TPS is set, as
 * some tasks may not be executed within a given interval based on the sort order. Feel free to
 * implement your own
 *
 * <p>the implementation of these is painful due to having to cast from generic Delayed objects
 */
@SuppressWarnings("WeakerAccess")
public class TaskSorters {

  /**
   * a comparator which will give you repeating tasks first, then tasks scheduled with zero delay,
   * then regular tasks. Within each of those groups, we have sorted by the earliest scheduled task
   */
  public static final Comparator<ScheduledTask>
          SORTED_BY_SCHEDULED_TIME_EARLIEST_FIRST_WITH_REPEATING_FIRST_THEN_IMMEDIATE_THEN_DELAYED =
          (scheduled1, scheduled2) -> {
            if (scheduled1.isRepeating() && !scheduled2.isRepeating()) {
              return -1;
            } else if (!scheduled1.isRepeating() && scheduled2.isRepeating()) {
              return 1;
            } else {
              if (scheduled1.wasRequestedImmediately() && !scheduled2.wasRequestedImmediately()) {
                return -1;
              } else if (!scheduled1.wasRequestedImmediately()
                      && scheduled2.wasRequestedImmediately()) {
                return 1;
              } else {
                return Long.compare(
                        scheduled1.getScheduledTimeMillis(), scheduled2.getScheduledTimeMillis());
              }
            }
          };

  /**
   * sorts the list by the earliest scheduled time
   */
  public static final Comparator<ScheduledTask> SORTED_BY_SCHEDULED_TIME_EARLIEST_FIRST =
          Comparator.comparingLong(ScheduledTask::getScheduledTimeMillis);

  /** sorts the list so the latest scheduled items get priority */
  public static final Comparator<ScheduledTask> SORTED_BY_SCHEDULED_TIME_LATEST_FIRST =
          (scheduled1, scheduled2) ->
                  Long.compare(scheduled2.getScheduledTimeMillis(), scheduled1.getScheduledTimeMillis());

  private TaskSorters() {}
}
