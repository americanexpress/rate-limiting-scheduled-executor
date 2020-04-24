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

import org.springframework.stereotype.Component;

import java.time.Clock;

/**
 * supplies a clock. Externalised into a neat class file so we don't have to worry about a
 * guiceBinding module or spring configuration file
 */
@Component
public class ClockSupplier {
  private final Clock clock;

  /**
   * supplies the system clock. This is externalised to enable good unit testing
   */
  ClockSupplier() {
    clock = Clock.systemUTC();
  }

  /**
   * get the clock
   *
   * @return a system UTC clock
   */
  public Clock getClock() {
    return clock;
  }
}
