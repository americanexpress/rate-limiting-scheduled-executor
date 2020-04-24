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

package com.americanexpress.ratelimitedscheduler.integration;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

class TaskTracker {
  private final List<Task> taskList;

  TaskTracker() {
    taskList = new ArrayList<>();
  }

  void addTask(Task task) {
    taskList.add(task);
  }

  int getMaxTPS() {
    Map<Long, AtomicInteger> countMap = new TreeMap<>();
    for (Task task : taskList) {

      // if the task wasn't triggered then ignore it
      if (task.getTriggeredDateTime() != null) {
        long second = task.getTriggeredDateTime().toEpochMilli() / 1000;
        countMap.putIfAbsent(second, new AtomicInteger());
        countMap.get(second).getAndIncrement();
      }
    }
    int maxTPS = 0;
    for (Map.Entry<Long, AtomicInteger> entry : countMap.entrySet()) {
      Instant now = Instant.ofEpochMilli(entry.getKey() * 1000);
      int tps = entry.getValue().get();
      System.out.println(now + " " + tps);
      if (maxTPS < tps) {
        maxTPS = tps;
      }
    }
    return maxTPS;
  }

  long getMinimumDifferenceBetweenScheduledTimeAndExecuteTimeInSeconds() {
    long difference = Long.MAX_VALUE;
    for (Task task : taskList) {
      if (task.getTimeDifferenceSeconds() < difference) {
        difference = task.getTimeDifferenceSeconds();
      }
    }
    return difference;
  }

  long getMinimumDifferenceBetweenScheduledTimeAndExecuteTimeInMilliSeconds() {
    long difference = Long.MAX_VALUE;
    for (Task task : taskList) {
      if (task.getTimeDifferenceMillis() < difference) {
        difference = task.getTimeDifferenceMillis();
      }
    }
    return difference;
  }

  long getAverageDifferenceBetweenTaskTriggersMs() {
    long totalDifference = 0;
    long count = 0;
    Instant lastTriggeredTime = null;
    for (Task task : taskList) {
      Instant thisTriggeredDateTime = task.getTriggeredDateTime();
      if (lastTriggeredTime != null) {
        long difference = Duration.between(lastTriggeredTime, thisTriggeredDateTime).toMillis();
        totalDifference += difference;
        count++;
      }
      lastTriggeredTime = thisTriggeredDateTime;
    }
    return totalDifference / count;
  }

  long getMinimumDifferenceBetweenTaskEndAndTriggersMs() {
    long shortestDifference = Long.MAX_VALUE;
    Instant lastEndTime = Instant.ofEpochSecond(0);
    for (Task task : taskList) {
      Instant thisTriggeredDateTime = task.getTriggeredDateTime();
      long difference = Duration.between(lastEndTime, thisTriggeredDateTime).toMillis();
      if (difference < shortestDifference) {
        shortestDifference = difference;
      }
      if (task.getCompletedDateTime() != null) {
        lastEndTime = task.getCompletedDateTime();
      }
    }
    return shortestDifference;
  }

  long getMaximumDifferenceBetweenScheduledTimeAndExecuteTime() {
    long difference = Long.MIN_VALUE;
    for (Task task : taskList) {
      if (task.getTimeDifferenceSeconds() > difference) {
        difference = task.getTimeDifferenceSeconds();
      }
    }
    return difference;
  }

  private Map<Long, Set<Task>> getTasksPerSecond() {
    Map<Long, Set<Task>> returnMap = new HashMap<>();
    for (Task task : taskList) {
      if (task.getTriggeredDateTime() != null) {
        long second = task.getTriggeredDateTime().toEpochMilli() / 1000;
        returnMap.putIfAbsent(second, new HashSet<>());
        returnMap.get(second).add(task);
      }
    }
    System.out.println(returnMap);
    return returnMap;
  }

  SortedMap<Long, Float> getAverageIDPerSecond() {
    SortedMap<Long, Float> returnMap = new TreeMap<>();
    Map<Long, Set<Task>> tasksPerSecond = getTasksPerSecond();
    for (Map.Entry<Long, Set<Task>> entry : tasksPerSecond.entrySet()) {
      int totalID = 0;
      for (Task task : entry.getValue()) {
        totalID += task.getId();
      }
      returnMap.put(entry.getKey(), totalID / (float) entry.getValue().size());
    }
    return returnMap;
  }

  void printAllTasks() {
    for (Task task : taskList) {
      System.out.println(task);
    }
  }
}
