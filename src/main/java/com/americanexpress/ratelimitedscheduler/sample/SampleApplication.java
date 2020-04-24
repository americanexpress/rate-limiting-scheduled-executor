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

import com.americanexpress.ratelimitedscheduler.ConfiguredComponentSupplier;
import com.americanexpress.ratelimitedscheduler.RateLimitedScheduledExecutor;
import com.americanexpress.ratelimitedscheduler.RateLimitedScheduledExecutorManager;
import com.americanexpress.ratelimitedscheduler.exceptions.ConfiguredComponentSupplierHasBeenAccessedException;
import com.americanexpress.ratelimitedscheduler.exceptions.RateLimitedScheduledExecutorManagerHasAlreadyStartedException;
import com.americanexpress.ratelimitedscheduler.peers.PeerFinderRedisImpl;
import com.americanexpress.ratelimitedscheduler.task.TaskSorters;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.logging.Logger;

@SuppressWarnings("ConstantConditions")
public class SampleApplication {
  private static final Logger logger = Logger.getLogger(SampleApplication.class.getName());

  @SuppressWarnings({"squid:S1148"})
  public static void main(String[] args)
          throws InterruptedException, RateLimitedScheduledExecutorManagerHasAlreadyStartedException,
          ConfiguredComponentSupplierHasBeenAccessedException {
    try (JedisPool jedisPool =
                 new JedisPool(new GenericObjectPoolConfig(), "redisHost", 12345, 30_000, "redisPassword")) {

      Supplier<Jedis> jedisSupplier = jedisPool::getResource;
      ConfiguredComponentSupplier configuredComponentSupplier = new ConfiguredComponentSupplier();
      if (false) {
        // this never runs, as the Jedis config isn't valid
        configuredComponentSupplier.setPeerFinder(new PeerFinderRedisImpl(jedisSupplier));
      }
      int attempts = 1000;
      AtomicInteger atomicInteger = new AtomicInteger();

      RateLimitedScheduledExecutorManager manager =
              new RateLimitedScheduledExecutorManager(configuredComponentSupplier);
      manager.setBufferSize(5);
      manager.setMillisPerInterval(100);
      RateLimitedScheduledExecutor executorA = manager.getRateLimitedScheduledExecutor("serviceA");
      executorA.setMaxTPS(100);
      executorA.setTaskTimeout(Duration.ofSeconds(5));
      executorA.setSortMethod(TaskSorters.SORTED_BY_SCHEDULED_TIME_LATEST_FIRST);

      CountDownLatch countDownLatchServiceA = new CountDownLatch(attempts);
      for (int i = 0; i < attempts; i++) {
        executorA.schedule(
                new ExtraRunnable(i, countDownLatchServiceA, "serviceA"), i, TimeUnit.MILLISECONDS);
      }
      ScheduledFuture<?> fixedRateFuture =
              executorA.scheduleAtFixedRate(
                      () -> logger.fine("!!!!!!!!hello " + atomicInteger.getAndIncrement() + " "),
                      200,
                      1000,
                      TimeUnit.MILLISECONDS);

      RateLimitedScheduledExecutor executorB = manager.getRateLimitedScheduledExecutor("serviceB");
      executorB.setMaxTPS(10);
      executorB.setTaskTimeout(Duration.ofSeconds(10));

      CountDownLatch countDownLatchServiceB = new CountDownLatch(attempts);
      for (int i = 0; i < attempts; i++) {
        executorB.schedule(
                new ExtraRunnable(i, countDownLatchServiceB, "serviceB"), i, TimeUnit.MILLISECONDS);
      }

      if (countDownLatchServiceA.await(11, TimeUnit.SECONDS)) {
        logger.fine(
                "a did "
                        + (attempts - countDownLatchServiceA.getCount())
                        + " attempts and is finished");
      } else {
        logger.fine(
                "a did "
                        + (attempts - countDownLatchServiceA.getCount())
                        + " attempts but is not finished");
      }
      fixedRateFuture.cancel(true);

      logger.fine("b did " + (attempts - countDownLatchServiceB.getCount()) + " attempts");
    }
  }
}
