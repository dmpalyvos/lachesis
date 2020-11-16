package io.palyvos.scheduler.util;

import io.palyvos.scheduler.task.Task;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class QueryGraphParserTest {

  private static final String QUERY_DAG_PATH = "src/test/resources/query_dag.yaml";

  @Test
  void loadTasks() {
    final QueryGraphParser parser = new QueryGraphParser();
    Collection<Task> tasks = parser.loadTasks(QUERY_DAG_PATH);
    runAssertions(tasks);
  }

  private void runAssertions(Collection<Task> tasks) {
    Assert.assertEquals(tasks.size(), 5, "Incorrect task number");
    for (Task task : tasks) {
      Collection<Task> upstream = task.upstream();
      Collection<Task> downstream = task.downstream();
      Assert.assertTrue(upstream.size() + downstream.size() > 0, "Orphan task!");
      upstream
          .forEach(t -> Assert.assertTrue(t.downstream().contains(task), "Wrong upstream init!"));
      downstream
          .forEach(t -> Assert.assertTrue(t.upstream().contains(task), "Wrong downstream init"));
    }
  }

  @Test
  void initTaskGraph() {
    final QueryGraphParser parser = new QueryGraphParser();
    List<Task> tasks = Stream.of("SOURCE", "A", "B", "C", "SINK")
        .map(id -> Task.ofSingleSubtask(id)).collect(
            Collectors.toList());
    parser.initTaskGraph(tasks, QUERY_DAG_PATH);
    runAssertions(tasks);
  }

  @Test(expectedExceptions = {IllegalStateException.class})
  void initTaskGraphWithMissingTask() {
    final QueryGraphParser parser = new QueryGraphParser();
    List<Task> tasks = Stream.of("SOURCE", "A", "C", "SINK")
        .map(id -> Task.ofSingleSubtask(id)).collect(
            Collectors.toList());
    parser.initTaskGraph(tasks, QUERY_DAG_PATH);
    runAssertions(tasks);
  }

  @Test(expectedExceptions = {IllegalStateException.class})
  void initTaskGraphWithExtraTask() {
    final QueryGraphParser parser = new QueryGraphParser();
    List<Task> tasks = Stream.of("SOURCE", "A", "B", "C", "D", "SINK")
        .map(id -> Task.ofSingleSubtask(id)).collect(
            Collectors.toList());
    parser.initTaskGraph(tasks, QUERY_DAG_PATH);
  }

}
