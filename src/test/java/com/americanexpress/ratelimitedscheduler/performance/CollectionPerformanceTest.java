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

package com.americanexpress.ratelimitedscheduler.performance;

import com.americanexpress.ratelimitedscheduler.LockableList;
import com.americanexpress.ratelimitedscheduler.LockableSet;

import org.junit.Ignore;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings({"SameParameterValue", "squid:S1607"})
public class CollectionPerformanceTest {
  // set this number to 10_000_000 for a better picture of performance - it is lower for BAU to
  // ensure tests are expedient
  private static final int NUMBER_OF_WRITES = 1_000_000_000;
  private static final int NUMBER_OF_THREADS = 32;
  private static final int WRITES_BEFORE_CLEAR = 10_000_000;

  private static void testCollectionMultipleTimes(
          Collection<Long> collectionToTest, long numberOfWrites, int numberOfThreads)
          throws InterruptedException {
    System.gc();
    Instant startTime = Instant.now();
    long numberOfTries = numberOfWrites / WRITES_BEFORE_CLEAR;
    for (long i = 0; i < numberOfTries; i++) {
      collectionToTest.clear();
      testCollectionPerformance(collectionToTest, WRITES_BEFORE_CLEAR, numberOfThreads);
      System.out.print(".");
      assertEquals(WRITES_BEFORE_CLEAR, collectionToTest.size());
    }
    long time = Duration.between(startTime, Instant.now()).toMillis();
    System.out.println(
            "\nCollection "
                    + collectionToTest.getClass()
                    + " was done in "
                    + time
                    + "ms with "
                    + (numberOfTries * WRITES_BEFORE_CLEAR)
                    + " records");
  }

  private static void testCollectionPerformance(
          Collection<Long> collectionToTest, long numberOfWrites, int numberOfThreads)
          throws InterruptedException {
    long writesPerThread = numberOfWrites / numberOfThreads;
    CountDownLatch countDownLatch = new CountDownLatch(numberOfThreads);

    for (int i = 0; i < numberOfThreads; i++) {
      new Thread(
              getRunnable(collectionToTest, writesPerThread, writesPerThread * i, countDownLatch))
              .start();
    }
    countDownLatch.await();
  }

  private static Runnable getRunnable(
          Collection<Long> collectionToTest,
          long writesPerThread,
          long startNumber,
          CountDownLatch countDownLatch) {
    return () -> {
      for (long i = 0; i < writesPerThread; i++) {
        collectionToTest.add(startNumber + i);
      }
      countDownLatch.countDown();
    };
  }

  @Test
  @Ignore
  public void testPerformance() throws InterruptedException {
    LockableSet<Long> lockableSet = new LockableSet<>();
    testCollectionMultipleTimes(lockableSet, NUMBER_OF_WRITES, NUMBER_OF_THREADS);
    ConcurrentSkipListSet<Long> concurrentSkipListSet = new ConcurrentSkipListSet<>();
    testCollectionMultipleTimes(concurrentSkipListSet, NUMBER_OF_WRITES, NUMBER_OF_THREADS);
    Collection<Long> copyOnWriteArrayList = Collections.synchronizedList(new ArrayList<>());
    testCollectionMultipleTimes(copyOnWriteArrayList, NUMBER_OF_WRITES, NUMBER_OF_THREADS);
    Collection<Long> lockableList = new LockableList<>();
    testCollectionMultipleTimes(lockableList, NUMBER_OF_WRITES, NUMBER_OF_THREADS);
    // we don't want to actually fail if the performance is out of order
    assertTrue(true);
  }
}
