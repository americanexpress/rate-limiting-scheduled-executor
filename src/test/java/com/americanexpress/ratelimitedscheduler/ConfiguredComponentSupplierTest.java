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

import com.americanexpress.ratelimitedscheduler.exceptions.ConfiguredComponentSupplierHasBeenAccessedException;
import com.americanexpress.ratelimitedscheduler.peers.PeerFinder;
import com.americanexpress.ratelimitedscheduler.peers.PeerFinderStaticImpl;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class ConfiguredComponentSupplierTest {
  @Mock
  private ScheduledExecutorService scheduledExecutorService;

  @Mock
  private Executor executor;

  private ConfiguredComponentSupplier configuredComponentSupplier;
  @Mock
  private PeerFinder peerFinder;

  @Before
  public void setUp() {

    configuredComponentSupplier = new ConfiguredComponentSupplier();
  }

  @Test
  public void testSetAndGetScheduledExecutorService()
          throws ConfiguredComponentSupplierHasBeenAccessedException {
    configuredComponentSupplier.setScheduledExecutorService(scheduledExecutorService);
    assertEquals(
            scheduledExecutorService, configuredComponentSupplier.getScheduledExecutorService());
  }

  @Test
  public void testGetScheduledExecutorServiceWhenNotSet() {
    assertNotNull(configuredComponentSupplier.getScheduledExecutorService());
  }

  @Test
  public void testGetAndSetPeerFinder() throws ConfiguredComponentSupplierHasBeenAccessedException {
    configuredComponentSupplier.setPeerFinder(peerFinder);
    assertEquals(peerFinder, configuredComponentSupplier.getPeerFinder());
  }

  @Test
  public void testGetPeerFinderWhenNotSet() {
    assertEquals(new PeerFinderStaticImpl(1), configuredComponentSupplier.getPeerFinder());
  }

  @Test
  public void testSetAndGetExecutor() throws ConfiguredComponentSupplierHasBeenAccessedException {
    configuredComponentSupplier.setExecutor(executor);
    assertEquals(executor, configuredComponentSupplier.getExecutor());
  }

  @Test
  public void testGetExecutorWhenNotSet() {
    assertEquals(ForkJoinPool.commonPool(), configuredComponentSupplier.getExecutor());
  }

  @Test
  public void testShutDownWithInternalGeneratedScheduledExecutorService() {
    ScheduledExecutorService scheduledExecutorService =
            configuredComponentSupplier.getScheduledExecutorService();
    configuredComponentSupplier.shutdown();
    assertTrue(scheduledExecutorService.isShutdown());
  }

  @Test
  public void testShutDownWithSuppliedScheduledExecutorService()
          throws ConfiguredComponentSupplierHasBeenAccessedException {
    configuredComponentSupplier.setScheduledExecutorService(scheduledExecutorService);
    configuredComponentSupplier.shutdown();
    verify(scheduledExecutorService, times(0)).shutdown();
  }

  @Test
  public void testSetExecutorAfterGettingThrowsException() {
    configuredComponentSupplier.getExecutor();
    try {
      configuredComponentSupplier.setExecutor(executor);
      fail();
    } catch (Exception e) {
      assertEquals(ConfiguredComponentSupplierHasBeenAccessedException.class, e.getClass());
    }
  }

  @Test
  public void testSetScheduledExecutorServiceAfterGettingThrowsException() {
    configuredComponentSupplier.getScheduledExecutorService();
    try {
      configuredComponentSupplier.setScheduledExecutorService(scheduledExecutorService);
      fail();
    } catch (Exception e) {
      assertEquals(ConfiguredComponentSupplierHasBeenAccessedException.class, e.getClass());
    }
  }

  @Test
  public void testSetPeerFinderAfterGettingThrowsException() {
    configuredComponentSupplier.getPeerFinder();
    try {
      configuredComponentSupplier.setPeerFinder(peerFinder);
      fail();
    } catch (Exception e) {
      assertEquals(ConfiguredComponentSupplierHasBeenAccessedException.class, e.getClass());
    }
  }
}
