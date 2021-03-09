package io.palyvos.scheduler.task;

import io.palyvos.scheduler.util.SchedulerContext;
import java.util.Arrays;
import java.util.Collections;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

@Test
public class TaskIndexTest {

  private static final String DEFAULT_SPE = "default";

  @BeforeTest
  void prepareContext() {
    SchedulerContext.IS_DISTRIBUTED = false;
    SchedulerContext.MAX_REMOTE_TASKS = 0;
  }

  @Test(expectedExceptions = {NullPointerException.class})
  void nullConstructor() {
    new TaskIndex(null);
  }

  @Test(expectedExceptions = {IllegalArgumentException.class})
  void emptyConstructor() {
    TaskIndex taskIndex = new TaskIndex(Collections.emptyList());
  }

  @Test(expectedExceptions = {IllegalStateException.class})
  void singleTaskNoThreads() {
    final String id = "test";
    Task task = Task.ofSingleSubtask(id, DEFAULT_SPE);
    TaskIndex taskIndex = new TaskIndex(Arrays.asList(task));
  }

  @Test(expectedExceptions = {IllegalStateException.class})
  void singleTaskNoThreadsDistributedForbidden() {
    final String id = "test";
    SchedulerContext.IS_DISTRIBUTED = true;
    SchedulerContext.MAX_REMOTE_TASKS = 0;
    Task task = Task.ofSingleSubtask(id, DEFAULT_SPE);
    new TaskIndex(Arrays.asList(task));
  }

  @Test
  void singleTaskNoThreadsDistributedAllowed() {
    final String id = "test";
    SchedulerContext.IS_DISTRIBUTED = true;
    SchedulerContext.MAX_REMOTE_TASKS = 1;
    Task task = Task.ofSingleSubtask(id, DEFAULT_SPE);
    new TaskIndex(Arrays.asList(task));
  }

  @Test
  void singleTaskSingleSubtask() {
    Task task = new Task("id", "name", "job", DEFAULT_SPE);
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
    Task task = new Task("id", "name", "job", DEFAULT_SPE);
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
