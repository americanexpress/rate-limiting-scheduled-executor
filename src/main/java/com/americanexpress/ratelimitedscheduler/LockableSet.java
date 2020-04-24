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

import com.americanexpress.ratelimitedscheduler.exceptions.CollectionHasBeenEmptiedException;

import com.google.common.annotations.VisibleForTesting;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * this is a set which is lockable, and can be emptied. For write-many read-once, this should have
 * much higher performance than existing concurrent sets
 *
 * @param <T> the type of the collection we are storing
 */
@SuppressWarnings({"WeakerAccess", "unused", "NullableProblems"})
public class LockableSet<T> extends HashSet<T> {
  private static final String LOCK_ACQUIRED_EXCEPTION_MESSAGE =
          "lock needs to be acquired manually before running this process";
  private ReentrantLock lock = new ReentrantLock();
  private boolean isEmptied = false;

  public LockableSet() {
    super();
  }

  public LockableSet(Collection<? extends T> c) {
    super(c);
  }

  public LockableSet(int initialCapacity, float loadFactor) {
    super(initialCapacity, loadFactor);
  }

  public LockableSet(int initialCapacity) {
    super(initialCapacity);
  }

  /**
   * lock the collection for use. Please make sure to unlock it in a finally block each of the
   * methods individually locks, but this allows the lock to run over a block of code
   *
   * @throws CollectionHasBeenEmptiedException if the set has already been emptied
   */
  public void lock() {
    if (isEmptied) {
      throw new CollectionHasBeenEmptiedException();
    }
    lock.lock();
  }

  // force the lock - for internal use to avoid exceptions during the finally block
  private void forceLock() {
    if (lock == null) {
      lock = new ReentrantLock();
    }
    lock.lock();
    if (isEmptied) {
      throw new CollectionHasBeenEmptiedException();
    }
  }

  /**
   * unlocks the collection if the lock has been acquired separately. *
   *
   * @throws IllegalMonitorStateException if the current thread does not * hold this lock
   */
  public void unlock() {
    lock.unlock();
  }

  /**
   * empties the set (so it can not be used again) and returns it's contents in a set. this can be
   * useful if you don't want the set to be re-used after emptying
   *
   * @return the contents of this set
   * @throws CollectionHasBeenEmptiedException if the set has already been emptied
   */
  public Set<T> empty() {
    try {
      forceLock();
      HashSet<T> returnSet = new HashSet<>(this);
      clear();
      isEmptied = true;
      return returnSet;
    } finally {
      unlock();
    }
  }

  public boolean hasBeenEmptied() {
    return isEmptied;
  }

  @VisibleForTesting
  void setLock(ReentrantLock lock) {
    this.lock = lock;
  }

  @Override
  public Iterator<T> iterator() {
    if (lock.isHeldByCurrentThread()) {
      return super.iterator();
    } else {
      throw new UnsupportedOperationException(LOCK_ACQUIRED_EXCEPTION_MESSAGE);
    }
  }

  @Override
  public void forEach(Consumer<? super T> action) {
    try {
      forceLock();
      super.forEach(action);
    } finally {
      unlock();
    }
  }

  @Override
  public boolean add(T t) {
    try {
      forceLock();
      return super.add(t);
    } finally {
      unlock();
    }
  }

  @Override
  public void clear() {
    try {
      forceLock();
      super.clear();
    } finally {
      unlock();
    }
  }

  @Override
  public Spliterator<T> spliterator() {
    if (lock.isHeldByCurrentThread()) {
      return super.spliterator();
    } else {
      throw new UnsupportedOperationException(LOCK_ACQUIRED_EXCEPTION_MESSAGE);
    }
  }

  @Override
  public Stream<T> stream() {
    if (lock.isHeldByCurrentThread()) {
      return super.stream();
    } else {
      throw new UnsupportedOperationException(LOCK_ACQUIRED_EXCEPTION_MESSAGE);
    }
  }

  @Override
  public Stream<T> parallelStream() {
    if (lock.isHeldByCurrentThread()) {
      return super.parallelStream();
    } else {
      throw new UnsupportedOperationException(LOCK_ACQUIRED_EXCEPTION_MESSAGE);
    }
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    try {
      forceLock();
      return super.removeAll(c);
    } finally {
      unlock();
    }
  }

  @Override
  public boolean removeIf(Predicate<? super T> filter) {
    try {
      forceLock();
      return super.removeIf(filter);
    } finally {
      unlock();
    }
  }

  @Override
  public Object[] toArray() {
    try {
      forceLock();
      return super.toArray();
    } finally {
      unlock();
    }
  }

  @Override
  public <S> S[] toArray(S[] a) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T1> T1[] toArray(IntFunction<T1[]> generator) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean remove(Object o) {
    try {
      forceLock();
      return super.remove(o);
    } finally {
      unlock();
    }
  }

  @Override
  public boolean addAll(Collection<? extends T> c) {
    try {
      forceLock();
      return super.addAll(c);
    } finally {
      unlock();
    }
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    try {
      forceLock();
      return super.retainAll(c);
    } finally {
      unlock();
    }
  }

  @Override
  public boolean equals(Object o) {
    try {
      forceLock();
      return super.equals(o);
    } finally {
      unlock();
    }
  }

  @Override
  public int hashCode() {
    try {
      forceLock();
      return super.hashCode();
    } finally {
      unlock();
    }
  }
}
