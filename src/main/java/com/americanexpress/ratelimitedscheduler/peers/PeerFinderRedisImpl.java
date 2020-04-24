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
package com.americanexpress.ratelimitedscheduler.peers;

import com.google.common.annotations.VisibleForTesting;
import redis.clients.jedis.Jedis;

import javax.annotation.Nullable;
import java.time.Clock;
import java.time.Instant;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.logging.Logger;

/**
 * a peer finder which uses redis for service discovery to detect the number of remote peers
 */
public class PeerFinderRedisImpl implements PeerFinder {
  private final Logger logger;
  private final Supplier<Jedis> jedisSupplier;
  private final String keyName;
  private final String thisServerIdentifier;
  private final Clock clock;

  @VisibleForTesting
  PeerFinderRedisImpl(
          String keyName, Supplier<Jedis> jedisSupplier, Logger logger, Clock clock, Random random) {
    this.clock = clock;
    this.keyName = keyName;
    this.jedisSupplier = jedisSupplier;
    this.logger = logger;
    thisServerIdentifier = Integer.toString(random.nextInt());
  }

  /**
   * create the redis implementation of the PeerFinder
   *
   * @param keyName       the key to use within redis for identifying the servers which are alive
   * @param jedisSupplier a supplier of Jedis objects, which is used to communicate with Redis
   */
  public PeerFinderRedisImpl(String keyName, Supplier<Jedis> jedisSupplier) {
    this(
            keyName,
            jedisSupplier,
            Logger.getLogger(PeerFinderRedisImpl.class.getName()),
            Clock.systemUTC(),
            new Random());
  }

  /**
   * create the redis implementation of the PeerFinder
   *
   * @param jedisSupplier a supplier of Jedis objects, which is used to communicate with Redis
   */
  public PeerFinderRedisImpl(Supplier<Jedis> jedisSupplier) {
    this("rateLimitedScheduler.lastPing", jedisSupplier);
  }

  /**
   * gets the number of peers, including this one, which are detected on the network. This is called
   * every 5 seconds
   *
   * @return how many peers are on the network
   */
  @Override
  public int getTotalNumberOfPeers() {
    Set<String> keys = jedisSupplier.get().keys(constructKey(keyName, null));
    AtomicInteger counter = new AtomicInteger();
    long currentTimeMillis = Instant.now(clock).toEpochMilli();
    keys.parallelStream()
            .forEach(
                    key -> {
                      if (isServerCurrent(key, currentTimeMillis)) {
                        counter.getAndIncrement();
                      }
                    });
    return counter.get();
  }

  /**
   * works out if a given server is currently active, and deletes the record of it if not
   *
   * @param serverKey the key to the server in redis
   * @param currentTimeMillis the current time in milliseconds
   * @return true if it is current, false if it has timed out
   */
  private boolean isServerCurrent(String serverKey, long currentTimeMillis) {
    long updateMoment = Long.parseLong(jedisSupplier.get().get(serverKey));

    if (currentTimeMillis - updateMoment > 20_000) {
      // server is offline
      logger.finest(
              "server at key "
                      + serverKey
                      + " last updated "
                      + (currentTimeMillis - updateMoment)
                      + "ms ago, so is considered offline");
      jedisSupplier.get().del(serverKey);
      return false;
    } else {
      logger.finest(
              "server at key "
                      + serverKey
                      + " last updated "
                      + (currentTimeMillis - updateMoment)
                      + "ms ago, so is considered alive");
      return true;
    }
  }

  /** updates the network with our presence. This is called every 5 seconds. */
  @Override
  public void updateNetworkWithThisInstance() {
    jedisSupplier
            .get()
            .set(
                    constructKey(keyName, thisServerIdentifier),
                    Long.toString(Instant.now(clock).toEpochMilli()));
  }

  /**
   * construct a key in redis from its parts
   *
   * @param keyPart  the start of the key
   * @param hostName the name of the host. if this is null, its replaced with a wildcard for use in
   *                 'keys'
   * @return the key which can be used in get() or keys()
   */
  private String constructKey(String keyPart, @Nullable String hostName) {
    if (hostName == null) {
      return keyPart + ":*";
    } else {
      return keyPart + ":" + hostName;
    }
  }
}
