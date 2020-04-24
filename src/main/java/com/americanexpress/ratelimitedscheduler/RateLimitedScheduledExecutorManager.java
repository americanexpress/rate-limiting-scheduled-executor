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

import com.americanexpress.ratelimitedscheduler.exceptions.RateLimitedScheduledExecutorManagerHasAlreadyStartedException;
import com.americanexpress.ratelimitedscheduler.peers.PeerFinder;
import com.americanexpress.ratelimitedscheduler.task.ScheduledTask;
import com.americanexpress.ratelimitedscheduler.task.ScheduledTaskFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.logging.Level.WARNING;

/**
 * this is a rate-limited scheduler that allows for tasks to be scheduled at a maxium TPS. The
 * manager allows for overall management of several schedulers each of which has a single TPS the
 * tasks from all those schedulers are combined here and executed together
 */
@SuppressWarnings({"WeakerAccess", "squid:UnusedPrivateMethod"})
@Component
public class RateLimitedScheduledExecutorManager {
  private final List<RateLimitedScheduledExecutor> serviceList;
  private final IntervalRunnerFactory intervalRunnerFactory;
  private final AtomicLong intervalCounter;
  private final RateLimitedScheduledExecutorFactory rateLimitedScheduledExecutorFactory;
  private final Logger logger;
  private final Clock clock;
  private final ConfiguredComponentSupplier configuredComponentSupplier;
  private final Map<Long, IntervalRunner> intervalRunners;
  private final ScheduledTaskFactory scheduledTaskFactory;
  private PeerFinder peerFinder;
  private int numberOfPeers;
  private int bufferSize;
  private boolean hasStarted;
  private int millisPerInterval;
  private int peerFinderIntervalMs;

  /**
   * constructor for use without an injection library
   */
  public RateLimitedScheduledExecutorManager() {
    this(new ConfiguredComponentSupplier());
  }

  /**
   * constructor if you want to supply your own executor services
   *
   * @param configuredComponentSupplier a supplier of executors
   */
  public RateLimitedScheduledExecutorManager(
          ConfiguredComponentSupplier configuredComponentSupplier) {
    // create the clocksupplier
    this(configuredComponentSupplier, new ClockSupplier());
  }

  private RateLimitedScheduledExecutorManager(
          ConfiguredComponentSupplier configuredComponentSupplier, ClockSupplier clockSupplier) {
    // create the scheduledTaskFactory
    this(configuredComponentSupplier, clockSupplier, new ScheduledTaskFactory(clockSupplier));
  }
  // this is just a way of singleton-ing the clock factory
  private RateLimitedScheduledExecutorManager(
          ConfiguredComponentSupplier configuredComponentSupplier,
          ClockSupplier clockSupplier,
          ScheduledTaskFactory scheduledTaskFactory) {
    this(
            configuredComponentSupplier,
            new IntervalRunnerFactory(configuredComponentSupplier, clockSupplier),
            scheduledTaskFactory,
            new RateLimitedScheduledExecutorFactory(
                    scheduledTaskFactory, new LockableListFactory(), clockSupplier),
            clockSupplier,
            null);
  }

  /**
   * constructor for the executor, with a default buffer of 2 intervals
   *
   * @param configuredComponentSupplier provides executors for scheduling
   * @param intervalRunnerFactory a factory for tasks in an interval
   * @param scheduledTaskFactory a generator of scheduled tasks
   * @param rateLimitedScheduledExecutorFactory the executorServiceFactory which generated
   * @param clockSupplier time source (externalised for testing)
   * @param logger for logging - can be null
   */
  @Inject
  @Autowired
  public RateLimitedScheduledExecutorManager(
          ConfiguredComponentSupplier configuredComponentSupplier,
          IntervalRunnerFactory intervalRunnerFactory,
          ScheduledTaskFactory scheduledTaskFactory,
          RateLimitedScheduledExecutorFactory rateLimitedScheduledExecutorFactory,
          ClockSupplier clockSupplier,
          @Nullable Logger logger) {

    hasStarted = false;
    this.scheduledTaskFactory = scheduledTaskFactory;

    this.intervalRunnerFactory = intervalRunnerFactory;
    // default the logger if it's null
    this.logger =
            (logger == null)
                    ? Logger.getLogger(RateLimitedScheduledExecutorManager.class.getName())
                    : logger;

    this.rateLimitedScheduledExecutorFactory = rateLimitedScheduledExecutorFactory;
    serviceList = new CopyOnWriteArrayList<>();
    intervalCounter = new AtomicLong();
    clock = clockSupplier.getClock();
    this.configuredComponentSupplier = configuredComponentSupplier;
    intervalRunners = new ConcurrentHashMap<>();
    numberOfPeers = 0;
    try {
      setPeerFinderIntervalMs(5000);
      setBufferSize(2);
      setMillisPerInterval(1000);
    } catch (RateLimitedScheduledExecutorManagerHasAlreadyStartedException ignored) {
      this.logger.warning("somehow initialised the class after it had already started");
    }
  }

  /**
   * get a service for a given key
   *
   * @param key whatever object you want to use for the key (could be a string, etc)
   * @return the RateLimitedScheduledExecutor, which can then be used to schedule tasks, set TPS,
   *     set timeout and change the sort order
   */
  public RateLimitedScheduledExecutor getRateLimitedScheduledExecutor(String key) {
    initializeIfNeeded();
    RateLimitedScheduledExecutor rateLimitedScheduledExecutorService =
            rateLimitedScheduledExecutorFactory.getRateLimitedScheduledExecutorService(
                    key, numberOfPeers, false, this);
    serviceList.add(rateLimitedScheduledExecutorService);
    return rateLimitedScheduledExecutorService;
  }

  /**
   * get a service for a given key with extra parameters
   *
   * @param key whatever object you want to use for the key (could be a string, etc)
   * @param maxTPS set the maximum transactions this executor should trigger in an interval. zero
   *     removes the limit and will trigger as many as possible
   * @param taskTimeout how long to attempt a task for before it is timed out * note that it may be
   *     attempted shortly after this, if the executor thread is very busy (it may be submitted, but
   *     not yet executed)
   * @param ensureNoTaskDispatchedEarly sets a task will be attempted in the correct window (so may
   *     dispatch up to 1 interval worth of time early) or in the interval after it is scheduled (so
   *     will never dispatch early)
   * @return the RateLimitedScheduledExecutor, which can then be used to schedule tasks, set TPS,
   *     set timeout and change the sort order
   */
  public RateLimitedScheduledExecutor getRateLimitedScheduledExecutor(
          String key, int maxTPS, Duration taskTimeout, boolean ensureNoTaskDispatchedEarly) {
    initializeIfNeeded();
    RateLimitedScheduledExecutor rateLimitedScheduledExecutorService =
            rateLimitedScheduledExecutorFactory.getRateLimitedScheduledExecutorService(
                    key, numberOfPeers, ensureNoTaskDispatchedEarly, this);
    rateLimitedScheduledExecutorService.setMaxTPS(maxTPS);
    rateLimitedScheduledExecutorService.setTaskTimeout(taskTimeout);
    serviceList.add(rateLimitedScheduledExecutorService);
    return rateLimitedScheduledExecutorService;
  }

  @VisibleForTesting
  synchronized void initializeIfNeeded() {
    if (!hasStarted) {
      hasStarted = true;
      // start the interval counter for buffer intervals from now. We use a counter to be sure we
      // never
      // miss a record
      long currentMillis = Instant.now(clock).toEpochMilli();
      long immediateInterval = currentMillis / millisPerInterval;
      intervalCounter.set(immediateInterval + bufferSize);
      long startDelay = currentMillis - (immediateInterval * millisPerInterval);
      peerFinder = configuredComponentSupplier.getPeerFinder();
      configuredComponentSupplier
          .getScheduledExecutorService()
          .scheduleAtFixedRate(
              processTasksForNextInterval(), startDelay, millisPerInterval, TimeUnit.MILLISECONDS);
      configuredComponentSupplier
              .getScheduledExecutorService()
              .scheduleWithFixedDelay(checkNumberOfPeers(), 0, peerFinderIntervalMs, MILLISECONDS);
    }
  }

  @VisibleForTesting
  List<ScheduledTask> getAllTasksForInterval(long intervalNumber) {
    List<ScheduledTask> taskList = new ArrayList<>();
    Iterator<RateLimitedScheduledExecutor> serviceIterator = serviceList.iterator();
    while (serviceIterator.hasNext()) {
      RateLimitedScheduledExecutor rateLimitedScheduledExecutor = serviceIterator.next();
      if (rateLimitedScheduledExecutor.isTerminated()) {
        // remove dead services
        serviceIterator.remove();
      } else {
        taskList.addAll(rateLimitedScheduledExecutor.getTasksForIntervalNumber(intervalNumber));
      }
    }
    return taskList;
  }

  /**
   * a runnable that updates the network with our existance, then updates all the services with how
   * many peers are detected
   *
   * @return a runnable for scheduling
   */
  private Runnable checkNumberOfPeers() {
    return () -> {
      peerFinder.updateNetworkWithThisInstance();
      numberOfPeers = peerFinder.getTotalNumberOfPeers();
      for (RateLimitedScheduledExecutor rateLimitedScheduledExecutor : serviceList) {
        rateLimitedScheduledExecutor.setNumberOfPeers(numberOfPeers);
      }
    };
  }

  @SuppressWarnings({"squid:S1181", "squid:S2629"})
  private Runnable processTasksForNextInterval() {
    return () -> {
      try {
        long thisInterval = intervalCounter.getAndIncrement();
        List<ScheduledTask> allTasksForInterval = getAllTasksForInterval(thisInterval);
        logger.finer(
                String.format(
                        "processing tasks for %s, got %s items in total",
                        thisInterval, allTasksForInterval.size()));

        IntervalRunner intervalRunner =
                intervalRunnerFactory.getIntervalRunner(thisInterval, allTasksForInterval);
        intervalRunner
            .submitTasksToScheduler()
            .thenAccept(ignoredVoid -> removeRecordsForInterval(thisInterval));
        intervalRunners.put(thisInterval, intervalRunner);
      } catch (Throwable t) {
        logger.log(WARNING, "got a throwable of ", t);
      }
    };
  }

  private void removeRecordsForInterval(long intervalNumber) {
    intervalRunners.remove(intervalNumber);
    for (RateLimitedScheduledExecutor rateLimitedScheduledExecutor : serviceList) {
      rateLimitedScheduledExecutor.removeCountsForInterval(intervalNumber);
    }
  }

  boolean addTaskForInterval(ScheduledTask task, long intervalNumber) {
    if (intervalRunners.containsKey(intervalNumber)) {
      try {
        intervalRunners.get(intervalNumber).addTask(task);
        return true;
      } catch (RejectedExecutionException e) {
        logger.finer(
                "tried to add task to list for interval "
                        + intervalNumber
                + " but it was already being executed");
      }
    }
    return false;
  }

  @VisibleForTesting
  List<RateLimitedScheduledExecutor> getServiceList() {
    return serviceList;
  }

  @VisibleForTesting
  long getIntervalCounter() {
    return intervalCounter.get();
  }

  @VisibleForTesting
  Logger getLogger() {
    return logger;
  }

  @VisibleForTesting
  int getBufferSize() {
    return bufferSize;
  }
  /**
   * Tune how many intervals are buffered (ready to run) at any time
   *
   * <p>Setting the buffer higher means there is less likelyhood of scheduling being interrupted by
   * things like long running GC. Setting it lower means the scheduler is more responsive - if you
   * set it to 1, things scheduled within a second will happen in 1-2 intervals time, rather than
   * 2-3 intervals time, and items which are cancelled last-second are removed from the TPS limit
   * more efficiently. We do not recommend a setting of 0 as it means that first part of each
   * interval will be spent collecting tasks together.
   *
   * @param bufferSize how many buffers to save
   * @throws RateLimitedScheduledExecutorManagerHasAlreadyStartedException if you set this after you
   *     have called getRateLimitedScheduledExecutor
   */
  public void setBufferSize(int bufferSize)
      throws RateLimitedScheduledExecutorManagerHasAlreadyStartedException {
    if (bufferSize < 0) {
      throw new IllegalArgumentException("bufferSize must be greater than 0");
    }
    if (hasStarted) {
      throw new RateLimitedScheduledExecutorManagerHasAlreadyStartedException();
    } else {
      this.bufferSize = bufferSize;
    }
    logger.finer("bufferSize is " + bufferSize);
  }

  @VisibleForTesting
  int getMillisPerInterval() {
    return millisPerInterval;
  }

  /**
   * Tune how many milliseconds are within each interval on the system.
   *
   * <p>Setting the millis per second lower improves responsiveness at the cost of (slight) load,
   * setting it higher reduces load but also reduces responsiveness. Also note that we calculate TPS
   * based on the interval size - so a very small interval (ie 10ms) would limit your max TPS to AT
   * LEAST 100, and would only effectively shedule in 100TPS intervals from there up
   *
   * @param millisPerInterval how many milliseconds between each interval
   * @throws RateLimitedScheduledExecutorManagerHasAlreadyStartedException if you set this after you
   *     have called getRateLimitedScheduledExecutor
   */
  public void setMillisPerInterval(int millisPerInterval)
      throws RateLimitedScheduledExecutorManagerHasAlreadyStartedException {
    if (millisPerInterval < 10) {
      throw new IllegalArgumentException("millisPerInterval must be at least 10");
    }
    if (hasStarted) {
      throw new RateLimitedScheduledExecutorManagerHasAlreadyStartedException();
    } else {
      this.millisPerInterval = millisPerInterval;
      rateLimitedScheduledExecutorFactory.setMillisPerInterval(millisPerInterval);
      intervalRunnerFactory.setMillisPerInterval(millisPerInterval);
      scheduledTaskFactory.setMillisPerInterval(millisPerInterval);
    }
    logger.finer("millisPerInterval is " + millisPerInterval);
  }

  /**
   * this shuts down the ScheduledExecutorService running at the back of this applicaiton - this
   * would not normally be needed, but if the executor has been created by this module and you want
   * your program to exit cleanly, you need to call this
   */
  public void shutdown() {
    configuredComponentSupplier.shutdown();
  }

  @VisibleForTesting
  public int getPeerFinderIntervalMs() {
    return peerFinderIntervalMs;
  }

  /**
   * Tune how many often calls are made to the peer finder
   *
   * @param peerFinderIntervalMs how long to wait between calls to the peer finder
   * @throws RateLimitedScheduledExecutorManagerHasAlreadyStartedException if you set this after you
   *                                                                       have called getRateLimitedScheduledExecutor
   */
  public void setPeerFinderIntervalMs(int peerFinderIntervalMs)
          throws RateLimitedScheduledExecutorManagerHasAlreadyStartedException {
    if (hasStarted) {
      throw new RateLimitedScheduledExecutorManagerHasAlreadyStartedException();
    } else {
      this.peerFinderIntervalMs = peerFinderIntervalMs;
    }
    logger.finer("peerFinderIntervalMs is " + peerFinderIntervalMs);
  }
}
