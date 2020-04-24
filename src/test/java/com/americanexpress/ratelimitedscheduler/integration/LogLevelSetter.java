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

import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * handy method for setting JVM log level
 */
class LogLevelSetter {
  static void setLevel(Level targetLevel) {
    Logger root = Logger.getLogger("");
    root.setLevel(targetLevel);
    for (Handler handler : root.getHandlers()) {
      handler.setLevel(targetLevel);
    }
    System.out.println("level set: " + targetLevel.getName());
  }
}
