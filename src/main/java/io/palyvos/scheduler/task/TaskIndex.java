package io.palyvos.scheduler.task;

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

public final class TaskIndex {

  private final List<Task> tasks;
  private final List<Subtask> subtasks;
  private final Map<String, List<Integer>> subtaskThreadPids;
  private final Map<Subtask, Collection<Subtask>> upstream;
  private final Map<Subtask, Collection<Subtask>> downstream;

  public TaskIndex(Collection<Task> tasks) {
    Validate.notNull(tasks, "tasks");
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
        Validate.validState(subtask.thread() != null, "Subtask %s has no thread assigned to it!",
            subtask);
        pids.add(subtask.thread().pid());
        subtask.helpers().forEach(helper -> pids.add(subtask.thread().pid()));
        upstream.put(subtask, upstreamSubtasks);
        downstream.put(subtask, downstreamSubtasks);
        subtaskThreadPids.computeIfAbsent(subtask.id(), id -> new ArrayList<>()).addAll(pids);
      }
    }
    this.subtasks = Collections.unmodifiableList(subtasks);
    this.subtaskThreadPids = Collections.unmodifiableMap(subtaskThreadPids);
    this.upstream = upstream;
    this.downstream = downstream;
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
