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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.locks.ReentrantLock;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({
        "ResultOfMethodCallIgnored",
        "MismatchedQueryAndUpdateOfStringBuilder",
        "ConstantConditions",
        "SimplifyStreamApiCallChains"
})
@RunWith(MockitoJUnitRunner.class)
public class LockableSetUnitTest {
  private LockableSet<String> lockableSet;
  @Mock
  private ReentrantLock lock;

  @Before
  public void setUp() {
    lockableSet = new LockableSet<>();
    lockableSet.setLock(lock);
  }

  @Test
  public void testWithInitialEntries() {
    lockableSet = new LockableSet<>(List.of("hello", "world"));
    assertEquals(2, lockableSet.size());
    assertTrue(lockableSet.contains("hello"));
    assertTrue(lockableSet.contains("world"));
  }

  @Test
  public void testWithInitialCapacity() {
    lockableSet = new LockableSet<>(10);
    assertEquals(0, lockableSet.size());
  }

  @Test
  public void testWithInitialCapacityAndLoadFactor() {
    lockableSet = new LockableSet<>(10, 3.5F);
    assertEquals(0, lockableSet.size());
  }

  @Test
  public void testLockHappyPath() {
    lockableSet.lock();
    verify(lock).lock();
  }

  @Test
  public void testLockAfterEmptied() {
    when(lock.isHeldByCurrentThread()).thenReturn(true);
    lockableSet.empty();
    try {
      lockableSet.lock();
      fail();
    } catch (RuntimeException e) {
      assertEquals(CollectionHasBeenEmptiedException.class, e.getClass());
    }
  }

  @Test
  public void testUnlock() {
    lockableSet.unlock();
    verify(lock).unlock();
  }

  @Test
  public void testEmpty() {
    lockableSet.add("hello");
    when(lock.isHeldByCurrentThread()).thenReturn(true);
    Set<String> result = lockableSet.empty();
    assertEquals(1, result.size());
    assertTrue(result.contains("hello"));
    assertTrue(lockableSet.hasBeenEmptied());
    assertTrue(lockableSet.isEmpty());
    // should fail a second time
    try {
      lockableSet.empty();
      fail();
    } catch (RuntimeException e) {
      assertEquals(CollectionHasBeenEmptiedException.class, e.getClass());
    }
  }

  @Test
  public void testIteratorWithoutLock() {
    try {
      lockableSet.iterator();
      fail();
    } catch (UnsupportedOperationException e) {
      assertEquals(
              "lock needs to be acquired manually before running this process", e.getMessage());
    }
  }

  @Test
  public void testIteratorWithLock() {
    when(lock.isHeldByCurrentThread()).thenReturn(true);
    lockableSet.add("hello");
    lockableSet.add("world");
    Iterator<String> iterator = lockableSet.iterator();
    assertTrue(iterator.hasNext());
    String answer = iterator.next();
    assertTrue(answer.equalsIgnoreCase("hello") || answer.equalsIgnoreCase("world"));
    assertTrue(iterator.hasNext());
    answer = iterator.next();
    assertTrue(answer.equalsIgnoreCase("hello") || answer.equalsIgnoreCase("world"));
    assertFalse(iterator.hasNext());
  }

  @Test
  public void testForEach() {
    lockableSet.add("hello");
    lockableSet.add("world");
    when(lock.isHeldByCurrentThread()).thenReturn(true);
    StringBuilder stringBuilder = new StringBuilder();
    lockableSet.forEach(stringBuilder::append);
    assertTrue(stringBuilder.toString().contains("hello"));
    assertTrue(stringBuilder.toString().contains("world"));
  }

  @Test
  public void testForEachAfterEmpty() {
    when(lock.isHeldByCurrentThread()).thenReturn(true);
    lockableSet.empty();
    try {
      StringBuilder stringBuilder = new StringBuilder();
      lockableSet.forEach(stringBuilder::append);
      fail();
    } catch (RuntimeException e) {
      assertEquals(CollectionHasBeenEmptiedException.class, e.getClass());
    }
  }

  @Test
  public void testClear() {
    lockableSet.add("hello");
    assertFalse(lockableSet.isEmpty());
    lockableSet.clear();
    assertTrue(lockableSet.isEmpty());
  }

  @Test
  public void testClearAfterEmpty() {
    when(lock.isHeldByCurrentThread()).thenReturn(true);
    lockableSet.empty();
    try {
      lockableSet.clear();
      fail();
    } catch (RuntimeException e) {
      assertEquals(CollectionHasBeenEmptiedException.class, e.getClass());
    }
  }

  @Test
  public void testSpliteratorWithoutLock() {
    try {
      lockableSet.spliterator();
      fail();
    } catch (UnsupportedOperationException e) {
      assertEquals(
              "lock needs to be acquired manually before running this process", e.getMessage());
    }
  }

  @Test
  public void testSpliteratorWithLock() {
    when(lock.isHeldByCurrentThread()).thenReturn(true);
    lockableSet.add("hello");
    lockableSet.add("world");
    Spliterator<String> spliterator = lockableSet.spliterator();
    assertEquals(2, spliterator.estimateSize());
  }

  @Test
  public void testStreamWithoutLock() {
    try {
      lockableSet.stream();
      fail();
    } catch (UnsupportedOperationException e) {
      assertEquals(
              "lock needs to be acquired manually before running this process", e.getMessage());
    }
  }

  @Test
  public void testStreamWithLock() {
    when(lock.isHeldByCurrentThread()).thenReturn(true);
    lockableSet.add("hello");
    lockableSet.add("world");
    StringBuilder stringBuilder = new StringBuilder();
    lockableSet.stream().forEach(stringBuilder::append);
    assertTrue(stringBuilder.toString().contains("hello"));
    assertTrue(stringBuilder.toString().contains("world"));
  }

  @Test
  public void testParallelStreamWithoutLock() {
    try {
      lockableSet.parallelStream();
      fail();
    } catch (UnsupportedOperationException e) {
      assertEquals(
              "lock needs to be acquired manually before running this process", e.getMessage());
    }
  }

  @Test
  public void testParallelStreamWithLock() {
    when(lock.isHeldByCurrentThread()).thenReturn(true);
    lockableSet.add("hello");
    lockableSet.add("world");
    assertEquals(2, lockableSet.parallelStream().count());
  }

  @Test
  public void testRemoveAll() {
    lockableSet.add("hello");
    lockableSet.add("world");
    lockableSet.add("again");
    lockableSet.removeAll(List.of("hello", "world"));
    assertEquals(1, lockableSet.size());
    assertTrue(lockableSet.contains("again"));
  }

  @Test
  public void testRemoveAllAfterEmpty() {
    when(lock.isHeldByCurrentThread()).thenReturn(true);
    lockableSet.empty();
    try {
      lockableSet.removeAll(List.of("hello", "world"));
      fail();
    } catch (RuntimeException e) {
      assertEquals(CollectionHasBeenEmptiedException.class, e.getClass());
    }
  }

  @Test
  public void testRemoveIf() {
    lockableSet.add("hello");
    lockableSet.add("world");
    when(lock.isHeldByCurrentThread()).thenReturn(true);
    lockableSet.removeIf(s -> s.equalsIgnoreCase("HELLO"));
    assertEquals(1, lockableSet.size());
    assertTrue(lockableSet.contains("world"));
  }

  @Test
  public void testRemoveIfEmpty() {
    when(lock.isHeldByCurrentThread()).thenReturn(true);
    lockableSet.empty();
    try {
      lockableSet.removeIf(s -> s.equalsIgnoreCase("HELLO"));
      fail();
    } catch (RuntimeException e) {
      assertEquals(CollectionHasBeenEmptiedException.class, e.getClass());
    }
  }

  @Test
  public void testToArray() {
    when(lock.isHeldByCurrentThread()).thenReturn(true);
    lockableSet.add("hello");
    lockableSet.add("world");
    assertEquals(2, lockableSet.toArray().length);
  }

  @Test
  public void testToArrayfterEmpty() {
    when(lock.isHeldByCurrentThread()).thenReturn(true);
    lockableSet.empty();
    try {
      lockableSet.toArray();
      fail();
    } catch (RuntimeException e) {
      assertEquals(CollectionHasBeenEmptiedException.class, e.getClass());
    }
  }

  @Test
  public void testToArrayWithArrayInput() {
    try {
      lockableSet.toArray(new String[5]);
      fail();
    } catch (RuntimeException e) {
      assertEquals(UnsupportedOperationException.class, e.getClass());
    }
  }

  @Test
  public void testToArrayWithFunctionInput() {
    try {
      lockableSet.toArray(String[]::new);
      fail();
    } catch (RuntimeException e) {
      assertEquals(UnsupportedOperationException.class, e.getClass());
    }
  }

  @Test
  public void testRemove() {
    lockableSet.add("hello");
    lockableSet.add("world");
    lockableSet.remove("hello");
    assertFalse(lockableSet.contains("hello"));
    assertTrue(lockableSet.contains("world"));
  }

  @Test
  public void testRemoveAfterEmpty() {
    when(lock.isHeldByCurrentThread()).thenReturn(true);
    lockableSet.empty();
    try {
      lockableSet.remove("hello");
      fail();
    } catch (RuntimeException e) {
      assertEquals(CollectionHasBeenEmptiedException.class, e.getClass());
    }
  }

  @Test
  public void testAddAllAfterEmpty() {
    when(lock.isHeldByCurrentThread()).thenReturn(true);
    lockableSet.empty();
    try {
      lockableSet.addAll(List.of("hello", "to", "the"));
      fail();
    } catch (RuntimeException e) {
      assertEquals(CollectionHasBeenEmptiedException.class, e.getClass());
    }
  }

  @Test
  public void testAddAfterEmpty() {
    when(lock.isHeldByCurrentThread()).thenReturn(true);
    lockableSet.empty();
    try {
      lockableSet.add("hello");
      fail();
    } catch (RuntimeException e) {
      assertEquals(CollectionHasBeenEmptiedException.class, e.getClass());
    }
  }

  @Test
  public void testRetainAll() {
    when(lock.isHeldByCurrentThread()).thenReturn(true);
    lockableSet.addAll(List.of("hello", "world"));
    lockableSet.retainAll(List.of("hello", "to", "the"));
    assertTrue(lockableSet.contains("hello"));
    assertEquals(1, lockableSet.size());
  }

  @Test
  public void testRetainAllAfterEmpty() {
    when(lock.isHeldByCurrentThread()).thenReturn(true);
    lockableSet.empty();
    try {
      lockableSet.retainAll(List.of("hello", "to", "the"));
      fail();
    } catch (RuntimeException e) {
      assertEquals(CollectionHasBeenEmptiedException.class, e.getClass());
    }
  }

  @Test
  public void testEquals() {
    lockableSet.addAll(List.of("hello", "world"));
    HashSet<String> hashSet = new HashSet<>(List.of("hello", "world"));
    assertEquals(lockableSet, hashSet);
  }

  @Test
  public void testEqualsAfterEmpty() {
    when(lock.isHeldByCurrentThread()).thenReturn(true);
    lockableSet.empty();
    try {
      HashSet<String> hashSet = new HashSet<>(List.of("hello", "world"));
      assertEquals(lockableSet, hashSet);
      fail();
    } catch (RuntimeException e) {
      assertEquals(CollectionHasBeenEmptiedException.class, e.getClass());
    }
  }

  @Test
  public void testHashCode() {
    when(lock.isHeldByCurrentThread()).thenReturn(true);
    lockableSet.addAll(List.of("hello", "world"));
    HashSet<String> hashSet = new HashSet<>(List.of("hello", "world"));
    assertEquals(hashSet.hashCode(), lockableSet.hashCode());
  }

  @Test
  public void testHashCodeAfterEmpty() {
    when(lock.isHeldByCurrentThread()).thenReturn(true);
    lockableSet.empty();
    try {
      lockableSet.hashCode();
      fail();
    } catch (RuntimeException e) {
      assertEquals(CollectionHasBeenEmptiedException.class, e.getClass());
    }
  }
}
