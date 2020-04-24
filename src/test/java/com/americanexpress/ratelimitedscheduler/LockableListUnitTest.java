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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
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
public class LockableListUnitTest {
  private LockableList<String> lockableList;
  @Mock
  private ReentrantLock lock;

  @Before
  public void setUp() {
    lockableList = new LockableList<>();
    lockableList.setLock(lock);
  }

  @Test
  public void testWithInitialEntries() {
    lockableList = new LockableList<>(List.of("hello", "world"));
    assertEquals(2, lockableList.size());
    assertEquals("hello", lockableList.get(0));
    assertEquals("world", lockableList.get(1));
  }

  @Test
  public void testWithInitialCapacity() {
    lockableList = new LockableList<>(10);
    assertEquals(0, lockableList.size());
  }

  @Test
  public void testLockHappyPath() {
    lockableList.lock();
    verify(lock).lock();
  }

  @Test
  public void testNotLocked() {
    assertFalse(lockableList.isLocked());
    assertFalse(lockableList.isLockedByCurrentThread());
  }

  @Test
  public void testIsLocked() {
    when(lock.isHeldByCurrentThread()).thenReturn(true);
    when(lock.isLocked()).thenReturn(true);
    assertTrue(lockableList.isLocked());
    assertTrue(lockableList.isLockedByCurrentThread());
  }

  @Test
  public void testLockAfterEmptied() {
    lockableList.empty();
    try {
      lockableList.lock();
      fail();
    } catch (RuntimeException e) {
      assertEquals(CollectionHasBeenEmptiedException.class, e.getClass());
    }
  }

  @Test
  public void testUnlock() {
    lockableList.unlock();
    verify(lock).unlock();
  }

  @Test
  public void testEmpty() {
    lockableList.add("hello");
    List<String> result = lockableList.empty();
    assertEquals(1, result.size());
    assertEquals("hello", result.get(0));
    assertTrue(lockableList.hasBeenEmptied());
    assertTrue(lockableList.isEmpty());
    // should fail a second time
    try {
      lockableList.empty();
      fail();
    } catch (RuntimeException e) {
      assertEquals(CollectionHasBeenEmptiedException.class, e.getClass());
    }
  }

  @Test
  public void testIteratorWithoutLock() {
    try {
      lockableList.iterator();
      fail();
    } catch (UnsupportedOperationException e) {
      assertEquals(
              "lock needs to be acquired manually before running this process", e.getMessage());
    }
  }

  @Test
  public void testIteratorWithLock() {
    when(lock.isHeldByCurrentThread()).thenReturn(true);
    lockableList.add("hello");
    lockableList.add("world");
    Iterator<String> iterator = lockableList.iterator();
    assertTrue(iterator.hasNext());
    assertEquals("hello", iterator.next());
    assertTrue(iterator.hasNext());
    assertEquals("world", iterator.next());
    assertFalse(iterator.hasNext());
  }

  @Test
  public void testForEach() {
    lockableList.add("hello");
    lockableList.add("world");
    StringBuilder stringBuilder = new StringBuilder();
    lockableList.forEach(stringBuilder::append);
    assertEquals("helloworld", stringBuilder.toString());
  }

  @Test
  public void testForEachAfterEmpty() {
    lockableList.empty();
    try {
      StringBuilder stringBuilder = new StringBuilder();
      lockableList.forEach(stringBuilder::append);
      fail();
    } catch (RuntimeException e) {
      assertEquals(CollectionHasBeenEmptiedException.class, e.getClass());
    }
  }

  @Test
  public void testClear() {
    lockableList.add("hello");
    assertFalse(lockableList.isEmpty());
    lockableList.clear();
    assertTrue(lockableList.isEmpty());
  }

  @Test
  public void testClearAfterEmpty() {
    lockableList.empty();
    try {
      lockableList.clear();
      fail();
    } catch (RuntimeException e) {
      assertEquals(CollectionHasBeenEmptiedException.class, e.getClass());
    }
  }

  @Test
  public void testSpliteratorWithoutLock() {
    try {
      lockableList.spliterator();
      fail();
    } catch (UnsupportedOperationException e) {
      assertEquals(
              "lock needs to be acquired manually before running this process", e.getMessage());
    }
  }

  @Test
  public void testSpliteratorWithLock() {
    when(lock.isHeldByCurrentThread()).thenReturn(true);
    lockableList.add("hello");
    lockableList.add("world");
    Spliterator<String> spliterator = lockableList.spliterator();
    assertEquals(2, spliterator.estimateSize());
  }

  @Test
  public void testStreamWithoutLock() {
    try {
      lockableList.stream();
      fail();
    } catch (UnsupportedOperationException e) {
      assertEquals(
              "lock needs to be acquired manually before running this process", e.getMessage());
    }
  }

  @Test
  public void testStreamWithLock() {
    when(lock.isHeldByCurrentThread()).thenReturn(true);
    lockableList.add("hello");
    lockableList.add("world");
    StringBuilder stringBuilder = new StringBuilder();
    lockableList.stream().forEach(stringBuilder::append);
    assertEquals("helloworld", stringBuilder.toString());
  }

  @Test
  public void testParallelStreamWithoutLock() {
    try {
      lockableList.parallelStream();
      fail();
    } catch (UnsupportedOperationException e) {
      assertEquals(
              "lock needs to be acquired manually before running this process", e.getMessage());
    }
  }

  @Test
  public void testParallelStreamWithLock() {
    when(lock.isHeldByCurrentThread()).thenReturn(true);
    lockableList.add("hello");
    lockableList.add("world");
    assertEquals(2, lockableList.parallelStream().count());
  }

  @Test
  public void testRemoveAll() {
    lockableList.add("hello");
    lockableList.add("world");
    lockableList.add("again");
    lockableList.removeAll(List.of("hello", "world"));
    assertEquals(1, lockableList.size());
    assertEquals("again", lockableList.get(0));
  }

  @Test
  public void testRemoveAllAfterEmpty() {
    lockableList.empty();
    try {
      lockableList.removeAll(List.of("hello", "world"));
      fail();
    } catch (RuntimeException e) {
      assertEquals(CollectionHasBeenEmptiedException.class, e.getClass());
    }
  }

  @Test
  public void testRemoveIf() {
    lockableList.add("hello");
    lockableList.add("world");
    lockableList.removeIf(s -> s.equalsIgnoreCase("HELLO"));
    assertEquals(1, lockableList.size());
    assertEquals("world", lockableList.get(0));
  }

  @Test
  public void testRemoveIfEmpty() {
    lockableList.empty();
    try {
      lockableList.removeIf(s -> s.equalsIgnoreCase("HELLO"));
      fail();
    } catch (RuntimeException e) {
      assertEquals(CollectionHasBeenEmptiedException.class, e.getClass());
    }
  }

  @Test
  public void testToArray() {
    lockableList.add("hello");
    lockableList.add("world");
    assertEquals(2, lockableList.toArray().length);
  }

  @Test
  public void testToArrayfterEmpty() {
    lockableList.empty();
    try {
      lockableList.toArray();
      fail();
    } catch (RuntimeException e) {
      assertEquals(CollectionHasBeenEmptiedException.class, e.getClass());
    }
  }

  @Test
  public void testToArrayWithArrayInput() {
    try {
      lockableList.toArray(new String[5]);
      fail();
    } catch (RuntimeException e) {
      assertEquals(UnsupportedOperationException.class, e.getClass());
    }
  }

  @Test
  public void testToArrayWithFunctionInput() {
    try {
      lockableList.toArray(String[]::new);
      fail();
    } catch (RuntimeException e) {
      assertEquals(UnsupportedOperationException.class, e.getClass());
    }
  }

  @Test
  public void testRemove() {
    lockableList.add("hello");
    lockableList.add("world");
    lockableList.remove("hello");
    assertFalse(lockableList.contains("hello"));
    assertTrue(lockableList.contains("world"));
  }

  @Test
  public void testRemoveAfterEmpty() {
    lockableList.empty();
    try {
      lockableList.remove("hello");
      fail();
    } catch (RuntimeException e) {
      assertEquals(CollectionHasBeenEmptiedException.class, e.getClass());
    }
  }

  @Test
  public void containsAll() {
    lockableList.addAll(List.of("hello", "world", "again"));
    assertTrue(lockableList.containsAll(List.of("hello", "world")));
  }

  @Test
  public void testContainsAllAfterEmpty() {
    lockableList.empty();
    try {
      lockableList.containsAll(List.of("hello", "to", "the"));
      fail();
    } catch (RuntimeException e) {
      assertEquals(CollectionHasBeenEmptiedException.class, e.getClass());
    }
  }

  @Test
  public void testAddAllWithIndex() {
    lockableList.addAll(List.of("hello", "world"));
    lockableList.addAll(1, List.of("to", "the"));
    assertEquals("hello", lockableList.get(0));
    assertEquals("to", lockableList.get(1));
    assertEquals("the", lockableList.get(2));
    assertEquals("world", lockableList.get(3));
  }

  @Test
  public void testAddAllWithIndexAfterEmpty() {
    lockableList.empty();
    try {
      lockableList.addAll(1, List.of("hello", "to", "the"));
      fail();
    } catch (RuntimeException e) {
      assertEquals(CollectionHasBeenEmptiedException.class, e.getClass());
    }
  }

  @Test
  public void testAddAllAfterEmpty() {
    lockableList.empty();
    try {
      lockableList.addAll(List.of("hello", "to", "the"));
      fail();
    } catch (RuntimeException e) {
      assertEquals(CollectionHasBeenEmptiedException.class, e.getClass());
    }
  }

  @Test
  public void testAddAfterEmpty() {
    lockableList.empty();
    try {
      lockableList.add("hello");
      fail();
    } catch (RuntimeException e) {
      assertEquals(CollectionHasBeenEmptiedException.class, e.getClass());
    }
  }

  @Test
  public void testRetainAll() {
    lockableList.addAll(List.of("hello", "world"));
    lockableList.retainAll(List.of("hello", "to", "the"));
    assertEquals("hello", lockableList.get(0));
    assertEquals(1, lockableList.size());
  }

  @Test
  public void testRetainAllAfterEmpty() {
    lockableList.empty();
    try {
      lockableList.retainAll(List.of("hello", "to", "the"));
      fail();
    } catch (RuntimeException e) {
      assertEquals(CollectionHasBeenEmptiedException.class, e.getClass());
    }
  }

  @Test
  public void testReplaceAll() {
    lockableList.addAll(List.of("hello", "world"));
    lockableList.replaceAll(String::toUpperCase);
    assertEquals("HELLO", lockableList.get(0));
    assertEquals("WORLD", lockableList.get(1));
    assertEquals(2, lockableList.size());
  }

  @Test
  public void testReplaceAllAfterEmpty() {
    lockableList.empty();
    try {
      lockableList.replaceAll(String::toUpperCase);
      fail();
    } catch (RuntimeException e) {
      assertEquals(CollectionHasBeenEmptiedException.class, e.getClass());
    }
  }

  @Test
  public void testSort() {
    lockableList.addAll(List.of("world", "hello"));
    lockableList.sort(Comparator.comparing(String::toString));
    assertEquals("hello", lockableList.get(0));
    assertEquals("world", lockableList.get(1));
    assertEquals(2, lockableList.size());
  }

  @Test
  public void testSortAfterEmpty() {
    lockableList.empty();
    try {
      lockableList.sort(Comparator.comparing(String::toString));
      fail();
    } catch (RuntimeException e) {
      assertEquals(CollectionHasBeenEmptiedException.class, e.getClass());
    }
  }

  @Test
  public void testEquals() {
    lockableList.addAll(List.of("hello", "world"));
    ArrayList<String> arrayList = new ArrayList<>(List.of("hello", "world"));
    assertEquals(lockableList, arrayList);
  }

  @Test
  public void testEqualsAfterEmpty() {
    lockableList.empty();
    try {
      ArrayList<String> arrayList = new ArrayList<>(List.of("hello", "world"));
      assertEquals(lockableList, arrayList);
      fail();
    } catch (RuntimeException e) {
      assertEquals(CollectionHasBeenEmptiedException.class, e.getClass());
    }
  }

  @Test
  public void testHashCode() {
    lockableList.addAll(List.of("hello", "world"));
    ArrayList<String> arrayList = new ArrayList<>(List.of("hello", "world"));
    assertEquals(arrayList.hashCode(), lockableList.hashCode());
  }

  @Test
  public void testHashCodeAfterEmpty() {
    lockableList.empty();
    try {
      lockableList.hashCode();
      fail();
    } catch (RuntimeException e) {
      assertEquals(CollectionHasBeenEmptiedException.class, e.getClass());
    }
  }

  @Test
  public void testGetAfterEmpty() {
    lockableList.empty();
    try {
      lockableList.get(0);
      fail();
    } catch (RuntimeException e) {
      assertEquals(CollectionHasBeenEmptiedException.class, e.getClass());
    }
  }

  @Test
  public void testSetWithIndex() {
    lockableList.addAll(List.of("hello", "world"));
    lockableList.set(1, "hello");
    assertEquals("hello", lockableList.get(1));
    assertEquals("hello", lockableList.get(0));
    assertEquals(2, lockableList.size());
  }

  @Test
  public void testSetWithIndexAfterEmpty() {
    lockableList.empty();
    try {
      lockableList.set(1, "hello");
      fail();
    } catch (RuntimeException e) {
      assertEquals(CollectionHasBeenEmptiedException.class, e.getClass());
    }
  }

  @Test
  public void testAddWithIndex() {
    lockableList.addAll(List.of("hello", "world"));
    lockableList.add(1, "hello");
    assertEquals("hello", lockableList.get(0));
    assertEquals("hello", lockableList.get(1));
    assertEquals("world", lockableList.get(2));
    assertEquals(3, lockableList.size());
  }

  @Test
  public void testAddWithIndexAfterEmpty() {
    lockableList.empty();
    try {
      lockableList.add(1, "hello");
      fail();
    } catch (RuntimeException e) {
      assertEquals(CollectionHasBeenEmptiedException.class, e.getClass());
    }
  }

  @Test
  public void testRemoveWithIndex() {
    lockableList.addAll(List.of("hello", "world"));
    lockableList.remove(1);
    assertEquals("hello", lockableList.get(0));
    assertEquals(1, lockableList.size());
  }

  @Test
  public void testRemoveWithIndexAfterEmpty() {
    lockableList.empty();
    try {
      lockableList.remove(1);
      fail();
    } catch (RuntimeException e) {
      assertEquals(CollectionHasBeenEmptiedException.class, e.getClass());
    }
  }

  @Test
  public void testListIteratorWithoutLock() {
    try {
      lockableList.listIterator();
      fail();
    } catch (UnsupportedOperationException e) {
      assertEquals(
              "lock needs to be acquired manually before running this process", e.getMessage());
    }
  }

  @Test
  public void testListIteratorWithoutLockWithIndex() {
    try {
      lockableList.listIterator(2);
      fail();
    } catch (UnsupportedOperationException e) {
      assertEquals(
              "lock needs to be acquired manually before running this process", e.getMessage());
    }
  }

  @Test
  public void testListIterator() {
    when(lock.isHeldByCurrentThread()).thenReturn(true);
    lockableList.add("hello");
    lockableList.add("world");
    ListIterator<String> iterator = lockableList.listIterator();
    assertTrue(iterator.hasNext());
    assertFalse(iterator.hasPrevious());
    assertEquals("hello", iterator.next());
    assertTrue(iterator.hasNext());
    assertTrue(iterator.hasPrevious());
    assertEquals("world", iterator.next());
    assertFalse(iterator.hasNext());
    assertTrue(iterator.hasPrevious());
    assertEquals("world", iterator.previous());
    assertTrue(iterator.hasNext());
    assertTrue(iterator.hasPrevious());
    assertEquals("hello", iterator.previous());
  }

  @Test
  public void testListIteratorWithIndex() {
    when(lock.isHeldByCurrentThread()).thenReturn(true);
    lockableList.add("hello");
    lockableList.add("world");
    ListIterator<String> iterator = lockableList.listIterator(0);
    assertTrue(iterator.hasNext());
    assertFalse(iterator.hasPrevious());
    assertEquals("hello", iterator.next());
    assertTrue(iterator.hasNext());
    assertTrue(iterator.hasPrevious());
    assertEquals("world", iterator.next());
    assertFalse(iterator.hasNext());
    assertTrue(iterator.hasPrevious());
    assertEquals("world", iterator.previous());
    assertTrue(iterator.hasNext());
    assertTrue(iterator.hasPrevious());
    assertEquals("hello", iterator.previous());
  }

  @Test
  public void testSubList() {
    lockableList.addAll(List.of("hello", "world", "again"));
    List<String> answer = lockableList.subList(1, 3);
    assertEquals(2, answer.size());
    assertEquals("world", answer.get(0));
    assertEquals("again", answer.get(1));
  }

  @Test
  public void testSubListAfterEmpty() {
    lockableList.empty();
    try {
      lockableList.subList(1, 3);
      fail();
    } catch (RuntimeException e) {
      assertEquals(CollectionHasBeenEmptiedException.class, e.getClass());
    }
  }
}
