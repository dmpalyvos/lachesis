package io.palyvos.scheduler.task;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class Query {

  private final List<Task> sources = new ArrayList<>();
  private final List<Task> sinks = new ArrayList<>();
  private final List<Task> tasks = new ArrayList<>();
  private final int index;
  private String name;

  public Query(int index, String name) {
    this.index = index;
    this.name = name;
  }

  public Query(int index) {
    this(index, String.valueOf(index));
  }

  public int index() {
    return index;
  }

  public String name() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Collection<Task> sources() {
    return sources;
  }

  public Collection<Task> sinks() {
    return sinks;
  }

  public Collection<Task> tasks() {
    return tasks;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("index", index)
        .append("name", name)
        .append("sources", sources)
        .append("sinks", sinks)
        .append("tasks", tasks)
        .toString();
  }
}
