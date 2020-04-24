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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Spliterator;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

/**
 * this is a list which is lockable, and can be emptied. For write-many read-once, this should have
 * much higher performance than existing concurrent lists
 *
 * @param <T> the type of the collection we are storing
 */
@SuppressWarnings({"WeakerAccess", "unused", "NullableProblems"})
public class LockableList<T> extends ArrayList<T> {
  private static final String LOCK_ACQUIRED_EXCEPTION_MESSAGE =
          "lock needs to be acquired manually before running this process";
  private ReentrantLock lock = new ReentrantLock();
  private boolean isEmptied = false;

  /**
   * create an empty lockable list
   */
  public LockableList() {
    super();
  }

  /**
   * create a lockable list from an existing collection
   *
   * @param c the existing collection
   */
  public LockableList(Collection<? extends T> c) {
    super(c);
  }

  /**
   * create a lockable list with an initial capacity (we are backed by an arraylist)
   *
   * @param initialCapacity how many slots to put in the lockableList initially
   */
  public LockableList(int initialCapacity) {
    super(initialCapacity);
  }

  /**
   * whether the list has already been emptied
   *
   * @return true if 'empty()' has already been called
   */
  public boolean hasBeenEmptied() {
    return isEmptied;
  }

  @VisibleForTesting
  void setLock(ReentrantLock lock) {
    this.lock = lock;
  }

  /**
   * lock the collection for use. Please make sure to unlock it in a finally block each of the
   * methods individually locks, but this allows the lock to run over a block of code
   *
   * @throws CollectionHasBeenEmptiedException if the list has already been emptied
   */
  public void lock() {
    if (isEmptied) {
      throw new CollectionHasBeenEmptiedException();
    }
    lock.lock();
  }

  /** force the lock - for internal use to avoid exceptions during the finally block */
  private void forceLock() {
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
   * Queries if this list's lock is held by any thread. This method is designed for use in
   * monitoring of the system state, not for synchronization control.
   *
   * @return {@code true} if any thread holds this lock and {@code false} otherwise
   */
  public boolean isLocked() {
    return lock.isLocked();
  }
  /**
   * Queries if this lock is held by the current thread.
   *
   * <p>Analogous to the {@link Thread#holdsLock(Object)} method for built-in monitor locks, this
   * method is typically used for debugging and testing. For example, a method that should only be
   * called while a lock is held can assert that this is the case
   *
   * @return {@code true} if current thread holds this lock and {@code false} otherwise
   */
  public boolean isLockedByCurrentThread() {
    return lock.isHeldByCurrentThread();
  }

  /**
   * empties the list (so it can not be used again) and returns it's contents in a list. this can be
   * useful if you don't want the list to be re-used after emptying
   *
   * @return the contents of this list
   * @throws CollectionHasBeenEmptiedException if the list has already been emptied
   */
  public List<T> empty() {
    try {
      forceLock();
      List<T> returnList = new ArrayList<>(this);
      clear();
      isEmptied = true;
      return returnList;
    } finally {
      unlock();
    }
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
  public boolean containsAll(Collection<?> c) {
    try {
      forceLock();
      return super.containsAll(c);
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
  public boolean addAll(int index, Collection<? extends T> c) {
    try {
      forceLock();
      return super.addAll(index, c);
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
  public void replaceAll(UnaryOperator<T> operator) {
    try {
      forceLock();
      super.replaceAll(operator);
    } finally {
      unlock();
    }
  }

  @Override
  public void sort(Comparator<? super T> c) {
    try {
      forceLock();
      super.sort(c);
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

  @Override
  public T get(int index) {
    try {
      forceLock();
      return super.get(index);
    } finally {
      unlock();
    }
  }

  @Override
  public T set(int index, T element) {
    try {
      forceLock();
      return super.set(index, element);
    } finally {
      unlock();
    }
  }

  @Override
  public void add(int index, T element) {
    try {
      forceLock();
      super.add(index, element);
    } finally {
      unlock();
    }
  }

  @Override
  public T remove(int index) {
    try {
      forceLock();
      return super.remove(index);
    } finally {
      unlock();
    }
  }

  @Override
  public ListIterator<T> listIterator() {
    if (lock.isHeldByCurrentThread()) {
      return super.listIterator();
    } else {
      throw new UnsupportedOperationException(LOCK_ACQUIRED_EXCEPTION_MESSAGE);
    }
  }

  @Override
  public ListIterator<T> listIterator(int index) {
    if (lock.isHeldByCurrentThread()) {
      return super.listIterator(index);
    } else {
      throw new UnsupportedOperationException(LOCK_ACQUIRED_EXCEPTION_MESSAGE);
    }
  }

  @Override
  public List<T> subList(int fromIndex, int toIndex) {
    try {
      forceLock();
      return super.subList(fromIndex, toIndex);
    } finally {
      unlock();
    }
  }
}
