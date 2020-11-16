package io.palyvos.scheduler.adapters;

import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.task.Task;
import io.palyvos.scheduler.task.TaskIndex;
import java.util.Collection;

public interface SpeAdapter {

  void updateTasks();

  Collection<Task> tasks();

  Collection<ExternalThread> threads();

  TaskIndex taskIndex();

}
