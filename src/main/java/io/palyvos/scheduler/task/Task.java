package io.palyvos.scheduler.task;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class Task {

  public static final String DEFAULT_JOB_ID = "DEFAULT_JOB";
  private final String id;
  private final String name;
  private final String jobId;
  private Set<Subtask> subtasks = new HashSet<>();
  private Set<Task> upstream = new HashSet<>();
  private Set<Task> downstream = new HashSet<>();
  private final Set<HelperTask> helpers = new HashSet<>();

  public static Task ofSingleSubtask(String id) {
    return ofSingleSubtask(id, id, DEFAULT_JOB_ID);
  }

  public static Task ofSingleSubtask(String id, String name, String jobId) {
    Task task = new Task(id, name, jobId);
    Subtask singleSubtask = new Subtask(id, name, 0);
    task.subtasks.add(singleSubtask);
    return task;
  }

  public Task(String id, String name, String jobId) {
    Validate.notBlank(id, "Blank subtask id!");
    Validate.notBlank(name, "Blank subtask name!");
    Validate.notBlank(jobId, "Blank subtask job id!");
    this.id = id;
    this.name = name;
    this.jobId = jobId;
  }

  public String name() {
    return name;
  }

  public String id() {
    return id;
  }

  public Collection<Subtask> subtasks() {
    return subtasks;
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
        name.equals(task.name) &&
        jobId.equals(task.jobId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, name, jobId);
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("id", id)
        .append("name", name)
        .append("jobId", jobId)
        .append("subtasks", subtasks)
        .append("helpers", helpers)
        .toString();
  }
}
