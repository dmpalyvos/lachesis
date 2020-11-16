package io.palyvos.scheduler.adapters.storm;

import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.task.Subtask;
import io.palyvos.scheduler.task.Task;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.Validate;

class StormThreadAssigner {

  private static final Pattern EXECUTOR_THREAD_PATTERN = Pattern
      .compile("Thread-\\d+-(.+)-executor\\[\\d+ \\d+\\]");
  private static final String ACKER_NAME = "_acker";

  public static void assign(Collection<Task> tasks, Collection<ExternalThread> threads) {
    final Map<String, Queue<Subtask>> taskIndex = new HashMap<>();
    tasks.forEach(
        task -> taskIndex
            .computeIfAbsent(task.name().trim(), (k) -> new ArrayDeque<>(task.subtasks())));
    for (ExternalThread thread : threads) {
      Matcher matcher = EXECUTOR_THREAD_PATTERN.matcher(thread.name());
      if (matcher.matches()) {
        final String taskName = matcher.group(1);
        if (taskName.contains(ACKER_NAME)) {
          continue;
        }
        Queue<Subtask> subtasks = taskIndex.get(taskName);
        Validate
            .validState(subtasks != null && !subtasks.isEmpty(), "No available subtask for thread %s: %s", thread.name(), taskIndex);
        subtasks.remove().assignThread(thread);
      }
    }
  }

  private StormThreadAssigner() {
  }
}
