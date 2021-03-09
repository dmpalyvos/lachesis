package io.palyvos.scheduler.util;

import io.palyvos.scheduler.task.Task;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class QueryGraphFileParserTest {

  private static final String QUERY_DAG_PATH = "src/test/resources/query_dag.yaml";
  public static final String DEFAULT_SPE = "DEFAULT";

  @Test
  void loadTasks() {
    final QueryGraphFileParser parser = new QueryGraphFileParser();
    Collection<Task> tasks = parser.loadTasks(QUERY_DAG_PATH, id -> Task.ofSingleSubtask(id, DEFAULT_SPE));
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
    final QueryGraphFileParser parser = new QueryGraphFileParser();
    List<Task> tasks = Stream.of("SOURCE", "A", "B", "C", "SINK")
        .map(id -> Task.ofSingleSubtask(id, DEFAULT_SPE)).collect(
            Collectors.toList());
    parser.initTaskGraph(tasks, QUERY_DAG_PATH);
    runAssertions(tasks);
  }

  @Test(expectedExceptions = {IllegalStateException.class})
  void initTaskGraphWithMissingTask() {
    final QueryGraphFileParser parser = new QueryGraphFileParser();
    List<Task> tasks = Stream.of("SOURCE", "A", "C", "SINK")
        .map(id -> Task.ofSingleSubtask(id, DEFAULT_SPE)).collect(
            Collectors.toList());
    parser.initTaskGraph(tasks, QUERY_DAG_PATH);
    runAssertions(tasks);
  }

  @Test(expectedExceptions = {IllegalStateException.class})
  void initTaskGraphWithExtraTask() {
    final QueryGraphFileParser parser = new QueryGraphFileParser();
    List<Task> tasks = Stream.of("SOURCE", "A", "B", "C", "D", "SINK")
        .map(id -> Task.ofSingleSubtask(id, DEFAULT_SPE)).collect(
            Collectors.toList());
    parser.initTaskGraph(tasks, QUERY_DAG_PATH);
  }

}
