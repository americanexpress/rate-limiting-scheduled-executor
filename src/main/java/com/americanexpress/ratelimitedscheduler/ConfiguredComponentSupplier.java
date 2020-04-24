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

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;

/**
 * this manages the Executor and ScheduledExecutorService used at the backend for
 * scheduling/executing tasks. If none are supplied, we use sensible defaults instead
 */
@SuppressWarnings("WeakerAccess")
@Component
public class ConfiguredComponentSupplier {

  private ScheduledExecutorService scheduledExecutorService;
  private Executor executor;
  private PeerFinder peerFinder;
  private boolean executorServiceWasCreatedInternally;
  private boolean hasBeenAccessed;

  public ConfiguredComponentSupplier() {
    executorServiceWasCreatedInternally = false;
    hasBeenAccessed = false;
  }

  void shutdown() {
    if (executorServiceWasCreatedInternally) {
      scheduledExecutorService.shutdown();
    }
  }

  /**
   * get the scheduled executor service. If none has been set, a default is created
   *
   * @return the scheduled executor service
   */
  public ScheduledExecutorService getScheduledExecutorService() {
    hasBeenAccessed = true;
    if (scheduledExecutorService == null) {
      executorServiceWasCreatedInternally = true;
      Logger.getLogger(this.getClass().getName())
              .fine(
                      "no scheduledExecutorService has been injected, so creating and returning the default using 4 threads");
      scheduledExecutorService = Executors.newScheduledThreadPool(4);
    }
    return scheduledExecutorService;
  }

  /**
   * set the scheduled executor service which is used for scheduling short-running low-intensity
   * activities regularly. If this is called after the application has been started, it may not take
   * effect.
   *
   * @param scheduledExecutorService the scheduled executor service for the
   *     RateLimitedScheduledExecutor to use
   * @throws ConfiguredComponentSupplierHasBeenAccessedException if the getter for the executor, peerFinder or
   *     scheduledExecutorService has already been called. It is not possible to change the executor
   *     or scehduledExecutorService after the manager has started
   */
  @Inject(optional = true)
  @Autowired(required = false)
  @Qualifier("RateLimitedScheduledExecutor.taskScheduler")
  public void setScheduledExecutorService(
          @Named("RateLimitedScheduledExecutor.taskScheduler")
                  ScheduledExecutorService scheduledExecutorService)
          throws ConfiguredComponentSupplierHasBeenAccessedException {
    if (hasBeenAccessed) {
      throw new ConfiguredComponentSupplierHasBeenAccessedException();
    }
    this.scheduledExecutorService = scheduledExecutorService;
  }

  /**
   * get the executor. If none has been set, a default is created
   *
   * @return the scheduled executor service
   */
  public Executor getExecutor() {
    hasBeenAccessed = true;
    if (executor == null) {
      Logger.getLogger(this.getClass().getName())
              .fine(
                      "no executor has been injected, so creating and returning the default using forkJoinPool");
      executor = ForkJoinPool.commonPool();
    }

    return executor;
  }

  /**
   * set the executor service which is used for executing all the tasks. If this is called after the
   * application has been started, it may not take effect.
   *
   * @param executor the executor for the RateLimitedScheduledExecutor to use
   * @throws ConfiguredComponentSupplierHasBeenAccessedException if the getter for the executor, peerFinder or
   *     scheduledExecutorService has already been called. It is not possible to change the executor
   *     or scehduledExecutorService after the manager has started
   */
  @Inject(optional = true)
  @Autowired(required = false)
  @Qualifier("RateLimitedScheduledExecutor.taskExecutor")
  public void setExecutor(@Named("RateLimitedScheduledExecutor.taskExecutor") Executor executor)
          throws ConfiguredComponentSupplierHasBeenAccessedException {
    if (hasBeenAccessed) {
      throw new ConfiguredComponentSupplierHasBeenAccessedException();
    }
    this.executor = executor;
  }

  /**
   * get the peerFinder. If none has been set, a default is created
   *
   * @return the peer finder
   */
  public PeerFinder getPeerFinder() {
    hasBeenAccessed = true;
    if (peerFinder == null) {
      Logger.getLogger(this.getClass().getName())
              .fine(
                      "no peerFinder has been injected, so creating and returning the default using NoPeers");
      peerFinder = new PeerFinderStaticImpl(1);
    }

    return peerFinder;
  }

  /**
   * set the peer finder which is used for determining how many peers are available to share the
   * load
   *
   * @param peerFinder the PeerFinder for the RateLimitedScheduledExecutorManager to use
   * @throws ConfiguredComponentSupplierHasBeenAccessedException if the getter for the executor, peerFinder or
   *                                                             scheduledExecutorService has already been called. It is not possible to change the executor
   *                                                             or scehduledExecutorService after the manager has started
   */
  @Inject(optional = true)
  @Autowired(required = false)
  @Qualifier("RateLimitedScheduledExecutor.peerFinder")
  public void setPeerFinder(@Named("RateLimitedScheduledExecutor.peerFinder") PeerFinder peerFinder)
          throws ConfiguredComponentSupplierHasBeenAccessedException {
    if (hasBeenAccessed) {
      throw new ConfiguredComponentSupplierHasBeenAccessedException();
    }
    this.peerFinder = peerFinder;
  }
}
