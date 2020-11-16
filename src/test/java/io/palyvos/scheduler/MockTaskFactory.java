package io.palyvos.scheduler;

import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.task.Subtask;
import io.palyvos.scheduler.task.Task;
import java.util.ArrayList;
import java.util.List;

public class MockTaskFactory {

  public static final int START_PID = 100;

  public static Task singleSubtaskWithThread(int index) {
    String id = String.valueOf(index);
    Task task = new Task(id, id, "job");
    Subtask subtask = new Subtask(id, id, 0);
    subtask.assignThread(new ExternalThread(index + START_PID, id));
    task.subtasks().add(subtask);
    return task;
  }

  public static List<Task> simpleChain(int length) {
    List<Task> chain = new ArrayList<>();
    for (int i = 0; i < length; i++) {
      chain.add(singleSubtaskWithThread(i));
    }
    for (int i = 1; i < length; i++) {
      chain.get(i).upstream().add(chain.get(i - 1));
    }
    for (int i = 0; i < length - 1; i++) {
      chain.get(i).downstream().add(chain.get(i + 1));
    }
    return chain;
  }


  private MockTaskFactory() {

  }
}
