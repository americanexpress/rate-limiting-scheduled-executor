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

package com.americanexpress.ratelimitedscheduler.sample;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

class ExtraRunnable implements Runnable {
  private final int number;
  private final String serviceName;
  private final CountDownLatch countDownLatch;
  private final Logger logger = Logger.getLogger("ExtraRunnable");

  ExtraRunnable(int number, CountDownLatch countDownLatch, String serviceName) {
    this.number = number;
    this.countDownLatch = countDownLatch;
    this.serviceName = serviceName;
  }

  @Override
  public void run() {
    addRandomDelay();
    logger.fine("running " + number + " for service " + serviceName);
    countDownLatch.countDown();
  }

  private void addRandomDelay() {
    try {
      Thread.sleep(new Random().nextInt(990) + 10L);
    } catch (InterruptedException e) {
      logger.log(Level.WARNING, "got an error whilst sleeping", e);
      Thread.currentThread().interrupt();
    }
  }
}
