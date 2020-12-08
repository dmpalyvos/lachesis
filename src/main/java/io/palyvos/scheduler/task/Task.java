package io.palyvos.scheduler.task;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class Task {

  public static final String DEFAULT_JOB_ID = "DEFAULT_JOB";
  private final String id;
  private final String internalId;
  private final String jobId;
  private Set<Subtask> subtasks = new HashSet<>();
  private Set<Task> upstream = new HashSet<>();
  private Set<Task> downstream = new HashSet<>();
  private final List<Operator> operators = new ArrayList<>();
  private final Set<HelperTask> helpers = new HashSet<>();
  private final Set<Operator> headOperators = new HashSet<>();
  private final Set<Operator> tailOperators = new HashSet<>();

  public static Task ofSingleSubtask(String id) {
    return ofSingleSubtask(id, id, DEFAULT_JOB_ID);
  }

  public static Task ofSingleSubtask(String id, String name, String jobId) {
    Task task = new Task(id, name, jobId);
    Subtask singleSubtask = new Subtask(id, name, 0);
    task.subtasks.add(singleSubtask);
    return task;
  }

  public Task(String id, String internalId, String jobId) {
    Validate.notBlank(id, "Blank subtask id!");
    Validate.notBlank(internalId, "Blank subtask name!");
    Validate.notBlank(jobId, "Blank subtask job id!");
    this.id = id;
    this.internalId = internalId;
    this.jobId = jobId;
  }

  public String internalId() {
    return internalId;
  }

  public String id() {
    return id;
  }

  public Collection<Subtask> subtasks() {
    return subtasks;
  }

  public List<Operator> operators() {
    return operators;
  }

  public Set<Operator> headOperators() {
    return headOperators;
  }

  public Set<Operator> tailOperators() {
    return tailOperators;
  }

  public Collection<Task> upstream() {
    return upstream;
  }

  public Collection<Task> downstream() {
    return downstream;
  }

  public Set<HelperTask> helpers() {
    return helpers;
  }

  public Collection<ExternalThread> threads() {
    return subtasks().stream().map(subtask -> subtask.thread()).collect(Collectors.toList());
  }

  public int parallelism() {
    return subtasks.size();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Task task = (Task) o;
    return id.equals(task.id) &&
        internalId.equals(task.internalId) &&
        jobId.equals(task.jobId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, internalId, jobId);
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("id", id)
        .append("internalId", internalId)
        .append("jobId", jobId)
        .append("subtasks", subtasks)
        .append("operators", operators)
        .append("helpers", helpers)
        .toString();
  }
}
