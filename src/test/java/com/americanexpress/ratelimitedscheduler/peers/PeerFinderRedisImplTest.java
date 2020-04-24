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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import redis.clients.jedis.Jedis;

import java.time.Clock;
import java.time.Instant;
import java.util.Random;
import java.util.Set;
import java.util.function.Supplier;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PeerFinderRedisImplTest {
  @Mock
  private Supplier<Jedis> jedisSupplier;
  @Mock
  private Logger logger;
  @Mock
  private Clock clock;
  @Mock
  private Random random;
  @Mock
  private Jedis jedis;
  private PeerFinderRedisImpl peerFinder;

  @Before
  public void setUp() {
    when(clock.instant()).thenReturn(Instant.ofEpochSecond(100));
    when(random.nextInt()).thenReturn(12345);
    when(jedisSupplier.get()).thenReturn(jedis);
    peerFinder = new PeerFinderRedisImpl("lastPing", jedisSupplier, logger, clock, random);
  }

  @Test
  public void testAlternateConstructor() {
    peerFinder = new PeerFinderRedisImpl(jedisSupplier);
    when(jedis.keys("rateLimitedScheduler.lastPing:*")).thenReturn(Set.of("rateLimitedScheduler.lastPing:serverA"));
    when(jedis.get("rateLimitedScheduler.lastPing:serverA")).thenReturn(Long.toString(Instant.now().toEpochMilli()));
    peerFinder.updateNetworkWithThisInstance();
    verify(jedis).set(anyString(), anyString());
    assertEquals(1, peerFinder.getTotalNumberOfPeers());
    verify(jedis).keys("rateLimitedScheduler.lastPing:*");
    verify(jedis).get("rateLimitedScheduler.lastPing:serverA");
  }

  @Test
  public void getTotalNumberOfPeers() {
    when(jedis.keys("lastPing:*"))
            .thenReturn(Set.of("lastPing:serverA", "lastPing:serverB", "lastPing:serverC"));
    // 1 second ago
    when(jedis.get("lastPing:serverA")).thenReturn("99000");
    // 10 seconds ago
    when(jedis.get("lastPing:serverB")).thenReturn("90000");
    // 30 seoconds ago
    when(jedis.get("lastPing:serverC")).thenReturn("70000");
    assertEquals(2, peerFinder.getTotalNumberOfPeers());
    verify(jedis).keys("lastPing:*");
    verify(jedis).get("lastPing:serverA");
    verify(jedis).get("lastPing:serverB");
    verify(jedis).get("lastPing:serverC");
    verify(jedis).del("lastPing:serverC");
    verify(logger)
            .finest("server at key lastPing:serverA last updated 1000ms ago, so is considered alive");
    verify(logger)
            .finest("server at key lastPing:serverB last updated 10000ms ago, so is considered alive");
    verify(logger)
            .finest(
                    "server at key lastPing:serverC last updated 30000ms ago, so is considered offline");
  }

  @Test
  public void updateHostWithThisInstance() {
    peerFinder.updateNetworkWithThisInstance();
    verify(jedis).set("lastPing:12345", "100000");
  }
}
