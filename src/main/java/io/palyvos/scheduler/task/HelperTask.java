package io.palyvos.scheduler.task;

import java.util.Objects;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class HelperTask {

  private final String id;
  private final ExternalThread thread;

  public HelperTask(ExternalThread thread) {
    Validate.notNull(thread, "thread");
    this.id = helperId(thread);
    this.thread = thread;
  }

  private String helperId(ExternalThread thread) {
    return String.format("%s_%d", thread.name(), thread.pid());
  }

  public String id() {
    return id;
  }

  public ExternalThread thread() {
    return thread;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HelperTask that = (HelperTask) o;
    return id.equals(that.id) &&
        thread.equals(that.thread);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, thread);
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("id", id)
        .append("thread", thread)
        .toString();
  }
}
