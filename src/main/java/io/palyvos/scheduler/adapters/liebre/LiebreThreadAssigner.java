package io.palyvos.scheduler.adapters.liebre;

import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.task.Task;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.Validate;

class LiebreThreadAssigner {

  public static void assign(Collection<Task> tasks, Collection<ExternalThread> threads) {
    Map<String, ExternalThread> threadIndex = new HashMap<>();
    threads.stream().forEach(thread ->
        Validate.validState(threadIndex.put(thread.name(), thread) == null,
            "Multiple threads with name %s", thread.name()));
    tasks.stream().forEach(task ->
        task.subtasks().forEach(
            subtask -> {
              ExternalThread thread = threadIndex.get(subtask.id());
              Validate.validState(thread != null,
                  "Subtask %s not mapped to any thread!", subtask.id());
              subtask.assignThread(thread);
            }));
  }

  private LiebreThreadAssigner() {
  }
}
