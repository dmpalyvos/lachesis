package io.palyvos.scheduler.task;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test
public class TaskGraphTraverserTest {

  private static final Task task1 = new Task("id1", "name1", "job");
  private static final Task task2 = new Task("id2", "name2", "job");
  private static final Task task3 = new Task("id3", "name3", "job");
  private static final Task task4 = new Task("id4", "name4", "job");

  @Test(expectedExceptions = {IllegalArgumentException.class})
  void invalidConstruction() {
    new TaskGraphTraverser(Collections.emptyList());
  }

  @BeforeMethod
  void clearGraph() {
    Stream.of(task1, task2, task3, task4).forEach(task -> {
      task.upstream().clear();
      task.downstream().clear();
      task.subtasks().clear();
      task.subtasks().add(new Subtask(task.id(), task.name(), 0));
    });
  }

  @Test
  void duplicateTask() {
    new TaskGraphTraverser(Arrays.asList(task1, task1));
  }

  @Test
  void oneTask() {
    TaskGraphTraverser traverser = new TaskGraphTraverser(Arrays.asList(task1));
    Assert.assertEquals(traverser.sourceTasks(), Arrays.asList(task1));
    Assert.assertEquals(traverser.sinkTasks(), Arrays.asList(task1));
  }

  @Test
  void twoTasks() {
    task1.downstream().add(task2);
    task2.upstream().add(task1);
    TaskGraphTraverser traverser = new TaskGraphTraverser(Arrays.asList(task1, task2));
    Assert.assertEquals(traverser.sourceTasks(), Arrays.asList(task1));
    Assert.assertEquals(traverser.sinkTasks(), Arrays.asList(task2));
  }

  @Test
  void threeTasks() {
    task1.downstream().add(task2);
    task2.upstream().add(task1);
    task2.downstream().add(task3);
    task3.upstream().add(task2);
    TaskGraphTraverser traverser = new TaskGraphTraverser(Arrays.asList(task1, task2, task3));
    Assert.assertEquals(traverser.sourceTasks(), Arrays.asList(task1));
    Assert.assertEquals(traverser.sinkTasks(), Arrays.asList(task3));
  }

  @Test
  void threeTasksforEachTaskFromSinkBFS() {
    task1.downstream().add(task2);
    task2.upstream().add(task1);
    task2.downstream().add(task3);
    task3.upstream().add(task2);
    TaskGraphTraverser traverser = new TaskGraphTraverser(Arrays.asList(task1, task2, task3));
    traverser.forEachTaskFromSinkBFS(new Consumer<Task>() {
      int taskIndex = 3;

      @Override
      public void accept(Task task) {
        Assert.assertEquals(task.id(), String.format("id%d", taskIndex--));
      }
    });
  }

  @Test
  void threeTasksforEachSubtaskFromSinkBFS() {
    task1.downstream().add(task2);
    task2.upstream().add(task1);
    task2.downstream().add(task3);
    task3.upstream().add(task2);
    task2.subtasks().add(new Subtask(task2.id(), task2.name(), 1));
    TaskGraphTraverser traverser = new TaskGraphTraverser(Arrays.asList(task1, task2, task3));
    List<String> traversalLog = new ArrayList<>();
    traverser.forEachSubtaskFromSinkBFS(subtask -> traversalLog.add(subtask.toString()));
    List<String> expectedTraversal = Stream.of(task3, task2, task1)
        .flatMap(task -> task.subtasks().stream()).map(Subtask::toString).collect(
            Collectors.toList());
    // Test assumes that Set<Subtask> inside Task has a deterministic iteration order
    // if the state of the data is unchanged
    Assert.assertEquals(traversalLog, expectedTraversal);
  }

  @Test
  void threeTasksforEachTaskFromSourceBFS() {
    task1.downstream().add(task2);
    task2.upstream().add(task1);
    task2.downstream().add(task3);
    task3.upstream().add(task2);
    TaskGraphTraverser traverser = new TaskGraphTraverser(Arrays.asList(task1, task2, task3));
    traverser.forEachTaskFromSourceBFS(new Consumer<Task>() {
      int taskIndex = 1;

      @Override
      public void accept(Task task) {
        Assert.assertEquals(task.id(), String.format("id%d", taskIndex++));
      }
    });
  }

  @Test
  void threeTasksforEachSubtaskFromSourceBFS() {
    task1.downstream().add(task2);
    task2.upstream().add(task1);
    task2.downstream().add(task3);
    task3.upstream().add(task2);
    task2.subtasks().add(new Subtask(task2.id(), task2.name(), 1));
    TaskGraphTraverser traverser = new TaskGraphTraverser(Arrays.asList(task1, task2, task3));
    List<String> traversalLog = new ArrayList<>();
    traverser.forEachSubtaskFromSourceBFS(subtask -> traversalLog.add(subtask.toString()));
    List<String> expectedTraversal = Stream.of(task1, task2, task3)
        .flatMap(task -> task.subtasks().stream()).map(Subtask::toString).collect(
            Collectors.toList());
    // Test assumes that Set<Subtask> inside Task has a deterministic iteration order
    // if the state of the data is unchanged
    Assert.assertEquals(traversalLog, expectedTraversal);
  }

  /**
   * Verify that topological traversals work correctly. E.g., imagine the following graph (1 =
   * source, 3,4=sinkTasks).
   * <pre>
   *
   *         1
   *       /   \
   *      2     \
   *        \    \
   *         3 _  \
   *             \ \
   *               4
   * </pre>
   * In the above case, 4 should be traversed before all else for the dependencies to work
   * correctly.
   */
  @Test
  void fourTasksTreeUnbalanced() {
    task1.downstream().add(task2);
    task2.upstream().add(task1);

    task1.downstream().add(task4);
    task4.upstream().add(task1);

    task2.downstream().add(task3);
    task3.upstream().add(task2);

    task3.downstream().add(task4);
    task4.upstream().add(task3);

    TaskGraphTraverser traverser = new TaskGraphTraverser(
        Arrays.asList(task1, task2, task3, task4));
    List<String> traversalLog = new ArrayList<>();
    traverser.forEachSubtaskFromSinkBFS(subtask -> traversalLog.add(subtask.toString()));
    List<String> expectedTraversal = Stream.of(task4, task3, task2, task1)
        .flatMap(task -> task.subtasks().stream()).map(Subtask::toString).collect(
            Collectors.toList());
    // Test assumes that Set<Subtask> inside Task has a deterministic iteration order
    // if the state of the data is unchanged
    Assert.assertEquals(traversalLog, expectedTraversal);
  }

}
