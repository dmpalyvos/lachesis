package io.palyvos.scheduler.adapters.storm;

import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.task.Subtask;
import io.palyvos.scheduler.task.Task;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;

class StormThreadAssigner {

  public static void assign(Collection<Task> tasks, Collection<ExternalThread> threads) {
    final Map<String, Task> taskIndex = new HashMap<>();
    tasks.forEach(task -> taskIndex.put(task.id(), task));
    for (ExternalThread thread : threads) {
      Matcher matcher = StormConstants.EXECUTOR_THREAD_PATTERN.matcher(thread.name());
      if (matcher.matches()) {
        final String taskId = matcher.group(1);
        if (taskId.contains(StormConstants.ACKER_NAME) || taskId.contains(
            StormConstants.METRIC_REPORTER_NAME)) {
          continue;
        }
        //FIXME: Extract JobID? Maybe worker ID?
        //FIXME: Parse and handle parallel instance names/indexes
        Task task = taskIndex.get(taskId);
        Subtask subtask = new Subtask(taskId, taskId, task.subtasks().size());
        subtask.assignThread(thread);
        task.subtasks().add(subtask);
      }
    }
  }

  private StormThreadAssigner() {
  }
}
