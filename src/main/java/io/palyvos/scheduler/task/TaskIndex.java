package io.palyvos.scheduler.task;

import io.palyvos.scheduler.util.SchedulerContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class TaskIndex {

  private static final Logger LOG = LogManager.getLogger();

  private final List<Task> tasks;
  private final List<Subtask> subtasks;
  private final Map<String, List<Integer>> subtaskThreadPids;
  private final Map<Subtask, Collection<Subtask>> upstream;
  private final Map<Subtask, Collection<Subtask>> downstream;

  public TaskIndex(Collection<Task> tasks) {
    Validate.notNull(tasks, "tasks");
    checkForMissingTasks(tasks);
    this.tasks = Collections.unmodifiableList(new ArrayList(new HashSet(tasks)));
    final List<Subtask> subtasks = new ArrayList<>();
    final Map<String, List<Integer>> subtaskThreadPids = new HashMap<>();
    final Map<Subtask, Collection<Subtask>> upstream = new HashMap<>();
    final Map<Subtask, Collection<Subtask>> downstream = new HashMap<>();
    for (Task task : this.tasks) {
      subtasks.addAll(task.subtasks());
      final Collection<Subtask> upstreamSubtasks = task.upstream().stream().
          flatMap(t -> t.subtasks().stream()).collect(Collectors.toList());
      final Collection<Subtask> downstreamSubtasks = task.downstream().stream().
          flatMap(t -> t.subtasks().stream()).collect(Collectors.toList());
      for (Subtask subtask : task.subtasks()) {
        List<Integer> pids = new ArrayList<>();
        if (subtask.thread() == null) {
          if (SchedulerContext.IS_DISTRIBUTED) {
            continue;
          }
          throw new IllegalStateException(
              String.format("Subtask %s has no thread assigned to it.", subtask));
        }
        pids.add(subtask.thread().pid());
        subtask.helpers().forEach(helper -> pids.add(subtask.thread().pid()));
        upstream.put(subtask, upstreamSubtasks);
        downstream.put(subtask, downstreamSubtasks);
        subtaskThreadPids.computeIfAbsent(subtask.id(), id -> new ArrayList<>()).addAll(pids);
      }
    }
    this.subtasks = Collections.unmodifiableList(subtasks);
    this.subtaskThreadPids = Collections.unmodifiableMap(subtaskThreadPids);
    this.upstream = Collections.unmodifiableMap(upstream);
    this.downstream = Collections.unmodifiableMap(downstream);
  }

  private void checkForMissingTasks(Collection<Task> tasks) {
    tasks.forEach(task -> task.checkHasThreads());
    final Collection<Task> missingTasks = tasks.stream().filter(task -> !task.hasThreads())
        .collect(Collectors.toList());
    Validate.validState(missingTasks.size() <= maxMissingTasksAllowed(),
        "More remote tasks than the max allowed (%d): %s",maxMissingTasksAllowed(),
        missingTasks);
  }


  private int maxMissingTasksAllowed() {
    return SchedulerContext.IS_DISTRIBUTED ? SchedulerContext.MAX_REMOTE_TASKS : 0;
  }

  public Collection<Task> tasks() {
    return tasks;
  }

  public Collection<Subtask> subtasks() {
    return subtasks;
  }

  public List<Integer> pids(String subtaskId) {
    return subtaskThreadPids.get(subtaskId);
  }

  public Collection<Subtask> upstream(Subtask subtask) {
    return upstream.get(subtask);
  }


  public Collection<Subtask> downstream(Subtask subtask) {
    return downstream.get(subtask);
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("tasks", tasks)
        .toString();
  }
}
