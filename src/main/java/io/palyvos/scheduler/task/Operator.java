package io.palyvos.scheduler.task;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class Operator {

  private final String id;

  public Operator(String id) {
    this.id = id;
  }

  public String getId() {
    return id;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("id", id)
        .toString();
  }
}
