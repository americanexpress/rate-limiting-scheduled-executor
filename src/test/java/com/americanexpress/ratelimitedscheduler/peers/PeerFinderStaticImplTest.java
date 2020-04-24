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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class PeerFinderStaticImplTest {
  @Test
  public void testInitialisation() {
    PeerFinderStaticImpl peerFinder = new PeerFinderStaticImpl(1);
    assertEquals(1, peerFinder.getTotalNumberOfPeers());
  }

  @Test
  public void testSetter() {
    PeerFinderStaticImpl peerFinder = new PeerFinderStaticImpl(1);
    peerFinder.setTotalNumberOfPeers(2);
    assertEquals(2, peerFinder.getTotalNumberOfPeers());
  }

  @Test
  public void testEqualsAndHashCode() {
    PeerFinderStaticImpl peerFinder1 = new PeerFinderStaticImpl(1);
    PeerFinderStaticImpl peerFinder2 = new PeerFinderStaticImpl(1);
    assertEquals(peerFinder1, peerFinder1);
    assertEquals(peerFinder1, peerFinder2);
    assertEquals(peerFinder1.hashCode(), peerFinder2.hashCode());
    assertNotEquals(peerFinder1, null);
  }

  @Test
  public void testToString() {
    PeerFinderStaticImpl peerFinder = new PeerFinderStaticImpl(1);
    assertEquals("PeerFinderStaticImpl{totalNumberOfPeers=1}", peerFinder.toString());
  }
}
