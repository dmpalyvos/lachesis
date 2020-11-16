package io.palyvos.scheduler.task;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class TaskGraphTraverser {

  private static final Consumer<? super Task> NOOP_TASK_CONSUMER = task -> {
  };
  private static final Consumer<? super Subtask> NOOP_SUBTASK_CONSUMER = subtask -> {
  };
  private final Set<Task> tasks;
  private final Set<Task> sourceTasks;
  private final Set<Task> sinkTasks;
  private final Collection<Task> topologicalOrderFromSinks;
  private final Collection<Task> topologicalOrderFromSources;
  private final Set<Subtask> sinkSubtasks;
  private final Set<Subtask> sourceSubtasks;

  public TaskGraphTraverser(Collection<Task> tasks) {
    Validate.notEmpty(tasks, "No tasks given!");
    this.tasks = new HashSet<>(tasks);
    this.sourceTasks = tasks.stream().filter(t -> t.upstream().isEmpty())
        .collect(Collectors.toSet());
    this.sinkTasks = tasks.stream().filter(t -> t.downstream().isEmpty())
        .collect(Collectors.toSet());
    this.sourceSubtasks = sourceTasks.stream().flatMap(t -> t.subtasks().stream()).collect(Collectors.toSet());
    this.sinkSubtasks = sinkTasks.stream().flatMap(t -> t.subtasks().stream()).collect(Collectors.toSet());
    this.topologicalOrderFromSinks = topologicalOrder(Task::upstream);
    this.topologicalOrderFromSources = topologicalOrder(Task::downstream);
  }

  public void forEachTaskFromSourceBFS(Consumer<? super Task> consumer) {
    traverse(topologicalOrderFromSources, consumer, NOOP_SUBTASK_CONSUMER);
  }

  public void forEachTaskFromSinkBFS(Consumer<? super Task> consumer) {
    traverse(topologicalOrderFromSinks, consumer, NOOP_SUBTASK_CONSUMER);
  }

  public void forEachSubtaskFromSinkBFS(Consumer<? super Subtask> consumer) {
    traverse(topologicalOrderFromSinks, NOOP_TASK_CONSUMER, consumer);
  }

  public void forEachSubtaskFromSourceBFS(Consumer<? super Subtask> consumer) {
    traverse(topologicalOrderFromSources, NOOP_TASK_CONSUMER, consumer);
  }

  private Collection<Task> topologicalOrder(
      Function<? super Task, Collection<? extends Task>> childFunction) {
    final Deque<Task> order = new ArrayDeque<>();
    final Set<Task> temporary = new HashSet<>();
    final Set<Task> permanent = new HashSet<>();
    while (permanent.size() < tasks.size()) {
      for (Task task : tasks) {
        visit(task, order, temporary, permanent, childFunction);
      }
    }
    return order;
  }

  private void traverse(Collection<Task> order, Consumer<? super Task> taskConsumer,
      Consumer<? super Subtask> subtaskConsumer) {
    for (Task task : order) {
      taskConsumer.accept(task);
      task.subtasks().forEach(subtask -> subtaskConsumer.accept(subtask));
    }
  }

  private void visit(Task task, Deque<Task> order, Set<Task> temporary, Set<Task> permanent,
      Function<? super Task, Collection<? extends Task>> childFunction) {
    if (permanent.contains(task)) {
      return;
    }
    if (temporary.contains(task)) {
      throw new IllegalStateException("Cycle detected in graph!");
    }
    temporary.add(task);
    for (Task child : childFunction.apply(task)) {
      visit(child, order, temporary, permanent, childFunction);
    }
    temporary.remove(task);
    permanent.add(task);
    order.addFirst(task);
  }

  public Set<Task> sourceTasks() {
    return sourceTasks;
  }

  public Set<Task> sinkTasks() {
    return sinkTasks;

  }
  public Set<Subtask> sourceSubtasks() {
    return sourceSubtasks;
  }

  public Set<Subtask> sinkSubtasks() {
    return sinkSubtasks;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("subtasks", tasks)
        .append("sourceTasks", sourceTasks)
        .append("sinkTasks", sinkTasks)
        .toString();
  }
}
