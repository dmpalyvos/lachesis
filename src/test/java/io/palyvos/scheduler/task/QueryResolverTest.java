package io.palyvos.scheduler.task;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;

public class QueryResolverTest {

  private static final String DEFAULT_SPE = "default";

  private static final Task task1 = new Task("id1", "name1", "job", DEFAULT_SPE);
  private static final Task task2 = new Task("id2", "name2", "job", DEFAULT_SPE);
  private static final Task task3 = new Task("id3", "name3", "job", DEFAULT_SPE);
  private static final Task task4 = new Task("id4", "name4", "job", DEFAULT_SPE);

  @Test(expectedExceptions = IllegalArgumentException.class)
  void noTasks() {
    new QueryResolver(Collections.emptyList());
  }

  @Test
  void oneQueryOneTask() {
    Task task1 = Task.ofSingleSubtask("task1", DEFAULT_SPE);
    List<Task> tasks = Arrays.asList(task1);
    Collection<Query> queries = new QueryResolver(tasks).queries();
    Assert.assertNotNull(queries);
    Assert.assertEquals(queries.size(), 1);
    Query query = queries.iterator().next();
    Assert.assertEquals(query.name(), task1.id(), "name");
    Assert.assertEquals(query.sources(), tasks, "sources");
    Assert.assertEquals(query.sinks(), tasks, "sinks");
    Assert.assertEquals(query.tasks(), tasks, "tasks");
  }

  @Test
  void oneQueryTwoTasks() {
    Task task1 = Task.ofSingleSubtask("task1", DEFAULT_SPE);
    Task task2 = Task.ofSingleSubtask("task2", DEFAULT_SPE);
    task1.downstream().add(task2);
    task2.upstream().add(task1);
    List<Task> tasks = Arrays.asList(task1, task2);
    Collection<Query> queries = new QueryResolver(tasks).queries();
    Assert.assertNotNull(queries);
    Assert.assertEquals(queries.size(), 1);
    Query query = queries.iterator().next();
    Assert.assertEquals(query.name(), task1.id(), "name");
    Assert.assertEquals(query.sources(), asSet(task1), "sources");
    Assert.assertEquals(asSet(query.sinks()), asSet(task2), "sinks");
    Assert.assertEquals(asSet(query.tasks()), tasks, "tasks");
  }

  @Test
  void twoQueriesFourTasks() {
    Task task1 = Task.ofSingleSubtask("task1", DEFAULT_SPE);
    Task task2 = Task.ofSingleSubtask("task2", DEFAULT_SPE);
    Task task3 = Task.ofSingleSubtask("task3", DEFAULT_SPE);
    Task task4 = Task.ofSingleSubtask("task4", DEFAULT_SPE);
    task1.downstream().add(task2);
    task2.upstream().add(task1);
    task3.downstream().add(task4);
    task4.upstream().add(task3);
    List<Task> tasks = Arrays.asList(task1, task2, task3, task4);
    Collection<Query> queries = new QueryResolver(tasks).queries();
    Assert.assertNotNull(queries);
    Iterator<Query> iterator = queries.iterator();
    Query query1 = iterator.next();
    Query query2 = iterator.next();
    Assert.assertEquals(queries.size(), 2);
    Assert.assertEquals(query1.name(), task1.id(), "name");
    Assert.assertEquals(query2.name(), task3.id(), "name");
    Assert.assertEquals(asSet(query1.sources()), asSet(task1), "sources");
    Assert.assertEquals(asSet(query1.sinks()), asSet(task2), "sinks");
    Assert.assertEquals(asSet(query1.tasks()), asSet(task1, task2), "tasks");
    Assert.assertEquals(asSet(query2.sources()), asSet(task3), "sources");
    Assert.assertEquals(asSet(query2.sinks()), asSet(task4), "sinks");
    Assert.assertEquals(asSet(query2.tasks()), asSet(task3, task4), "tasks");
  }

  @Test
  void twoQueriesSinkFork() {
    Task task1 = Task.ofSingleSubtask("task1", DEFAULT_SPE);
    Task task2 = Task.ofSingleSubtask("task2", DEFAULT_SPE);
    Task task3 = Task.ofSingleSubtask("task3", DEFAULT_SPE);
    Task task4 = Task.ofSingleSubtask("task4", DEFAULT_SPE);
    task1.downstream().add(task2);
    task1.downstream().add(task3);
    task2.upstream().add(task1);
    task3.upstream().add(task1);
    List<Task> tasks = Arrays.asList(task1, task2, task3, task4);
    Collection<Query> queries = new QueryResolver(tasks).queries();
    Assert.assertNotNull(queries);
    Iterator<Query> iterator = queries.iterator();
    Query query1 = iterator.next();
    Query query2 = iterator.next();
    Assert.assertEquals(queries.size(), 2);
    Assert.assertEquals(query1.name(), task1.id(), "name");
    Assert.assertEquals(query2.name(), task4.id(), "name");
    Assert.assertEquals(asSet(query1.sources()), asSet(task1), "sources");
    Assert.assertEquals(asSet(query1.sinks()), asSet(task2, task3), "sinks");
    Assert.assertEquals(asSet(query1.tasks()), asSet(task1, task2, task3), "tasks");
    Assert.assertEquals(asSet(query2.sources()), asSet(task4), "sources");
    Assert.assertEquals(asSet(query2.sinks()), asSet(task4), "sinks");
    Assert.assertEquals(asSet(query2.tasks()), asSet(task4), "tasks");
  }

  @Test
  void twoQueriesSourceFork() {
    Task task1 = Task.ofSingleSubtask("task1", DEFAULT_SPE);
    Task task2 = Task.ofSingleSubtask("task2", DEFAULT_SPE);
    Task task3 = Task.ofSingleSubtask("task3", DEFAULT_SPE);
    Task task4 = Task.ofSingleSubtask("task4", DEFAULT_SPE);
    task1.downstream().add(task3);
    task2.downstream().add(task3);
    task3.upstream().add(task1);
    task3.upstream().add(task2);
    List<Task> tasks = Arrays.asList(task1, task2, task3, task4);
    Collection<Query> queries = new QueryResolver(tasks).queries();
    Assert.assertNotNull(queries);
    Iterator<Query> iterator = queries.iterator();
    Query query1 = iterator.next();
    Query query2 = iterator.next();
    Assert.assertEquals(queries.size(), 2);
    // NOTE: Name of query 1 is either that of task 1 or task 2, non-deterministic!
    Assert.assertTrue(task1.id().equals(query1.name()) || task2.id().equals(query1.name()), "name");
    Assert.assertEquals(query2.name(), task4.id(), "name");
    Assert.assertEquals(asSet(query1.sources()), asSet(task1, task2), "sources");
    Assert.assertEquals(asSet(query1.sinks()), asSet(task3), "sinks");
    Assert.assertEquals(asSet(query1.tasks()), asSet(task1, task2, task3), "tasks");
    Assert.assertEquals(asSet(query2.sources()), asSet(task4), "sources");
    Assert.assertEquals(asSet(query2.sinks()), asSet(task4), "sinks");
    Assert.assertEquals(asSet(query2.tasks()), asSet(task4), "tasks");
  }

  private <T> Set<T> asSet(T...values) {
    Set<T> set = new HashSet<>();
    for (T value : values) {
      set.add(value);
    }
    return Collections.unmodifiableSet(set);
  }

  private <T> Set<T> asSet(Collection<T> collection) {
    Set<T> set = new HashSet<>();
    set.addAll(collection);
    return Collections.unmodifiableSet(set);
  }

}