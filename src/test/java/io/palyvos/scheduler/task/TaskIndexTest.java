package io.palyvos.scheduler.task;

import java.util.Arrays;
import java.util.Collections;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class TaskIndexTest {

  @Test(expectedExceptions = {NullPointerException.class})
  void nullConstructor() {
    new TaskIndex(null);
  }

  @Test
  void emptyConstructor() {
    TaskIndex taskIndex = new TaskIndex(Collections.emptyList());
    taskIndex.tasks();
  }

  @Test(expectedExceptions = {IllegalStateException.class})
  void singleTaskNoThreads() {
    final String id = "test";
    Task task = Task.ofSingleSubtask(id);
    TaskIndex taskIndex = new TaskIndex(Arrays.asList(task));
  }

  @Test
  void singleTaskSingleSubtask() {
    Task task = new Task("id", "name", "job");
    Subtask subtask = new Subtask("id", "name", 0);
    ExternalThread thread = new ExternalThread(2, "thread");

    task.subtasks().add(subtask);
    subtask.assignThread(thread);
    TaskIndex taskIndex = new TaskIndex(Arrays.asList(task));

    Assert.assertEquals(taskIndex.tasks(), Arrays.asList(task));
    Assert.assertEquals(taskIndex.subtasks(), Arrays.asList(subtask));
    Assert.assertEquals(taskIndex.pids(subtask.id()), Arrays.asList(thread.pid()));
  }

  @Test()
  void duplicateTask() {
    Task task = new Task("id", "name", "job");
    Subtask subtask = new Subtask("id", "name", 0);
    ExternalThread thread = new ExternalThread(2, "thread");

    task.subtasks().add(subtask);
    subtask.assignThread(thread);
    TaskIndex taskIndex = new TaskIndex(Arrays.asList(task, task));
    Assert.assertEquals(taskIndex.tasks(), Arrays.asList(task));
    Assert.assertEquals(taskIndex.subtasks(), Arrays.asList(subtask));
    Assert.assertEquals(taskIndex.pids(subtask.id()), Arrays.asList(thread.pid()));
  }
}
