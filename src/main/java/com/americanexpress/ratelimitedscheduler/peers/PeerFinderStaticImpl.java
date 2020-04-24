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

import java.util.Objects;

/**
 * a peer finder which returns a static number of remote peers
 */
public class PeerFinderStaticImpl implements PeerFinder {

  private int totalNumberOfPeers;

  /**
   * create the peer finder
   *
   * @param totalNumberOfPeers how many peers to return when queried
   */
  public PeerFinderStaticImpl(int totalNumberOfPeers) {
    this.totalNumberOfPeers = totalNumberOfPeers;
  }

  /**
   * gets the number of peers, including this one, which are detected on the network. This is called
   * every 5 seconds
   *
   * @return how many peers are on the network
   */
  @Override
  public int getTotalNumberOfPeers() {
    return totalNumberOfPeers;
  }

  /**
   * update the number of peers which are returned when queried
   *
   * @param numberOfPeers how many peers to say are on the network
   */
  public void setTotalNumberOfPeers(int numberOfPeers) {
    this.totalNumberOfPeers = numberOfPeers;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PeerFinderStaticImpl that = (PeerFinderStaticImpl) o;
    return totalNumberOfPeers == that.totalNumberOfPeers;
  }

  @Override
  public int hashCode() {
    return Objects.hash(totalNumberOfPeers);
  }

  @Override
  public String toString() {
    return "PeerFinderStaticImpl{" + "totalNumberOfPeers=" + totalNumberOfPeers + '}';
  }
}
