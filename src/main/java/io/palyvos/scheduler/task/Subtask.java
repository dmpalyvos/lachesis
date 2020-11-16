package io.palyvos.scheduler.task;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Subtask {

  private static final Logger LOG = LogManager.getLogger(Subtask.class);

  private final String id;
  private final String name;
  private final int index;
  private final Set<Operator> operators = new HashSet<>();
  private final Set<HelperTask> helpers = new HashSet<>();
  private ExternalThread thread;

  public Subtask(String id, String name, int index) {
    Validate.notBlank(id, "Blank subtask id!");
    Validate.notBlank(name, "Blank subtask name!");
    Validate.isTrue(index >= 0, "Negative instance index!");
    this.id = id;
    this.name = name;
    this.index = index;
  }

  public String name() {
    return name;
  }

  public String id() {
    return id;
  }

  public int index() {
    return index;
  }

  public void assignThread(ExternalThread thread) {
    Validate.notNull(thread, "thread");
    Validate.validState(this.thread == null, "Cannot reassigning thread of subtask %s to %s", this,
        thread);
    this.thread = thread;
  }

  public ExternalThread thread() {
    return thread;
  }

  public Collection<HelperTask> helpers() {
    return helpers;
  }

  public Collection<Operator> operators() {
    return operators;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Subtask subtask = (Subtask) o;
    return index == subtask.index &&
        id.equals(subtask.id) &&
        name.equals(subtask.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, name, index);
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("id", id)
        .append("name", name)
        .append("index", index)
        .append("operators", operators)
        .append("helpers", helpers)
        .append("thread", thread)
        .toString();
  }
}
