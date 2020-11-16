package io.palyvos.scheduler.task;

import java.util.Objects;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * External thread that can be scheduled by the scheduler.
 */
public final class ExternalThread {

  private final int pid;
  private final String name;

  public ExternalThread(int pid, String name) {
    Validate.isTrue(pid > 1, "invalid pid");
    Validate.notBlank(name, "Parameter name is blank");
    this.pid = pid;
    this.name = name;
  }

  public int pid() {
    return pid;
  }

  public String name() {
    return name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ExternalThread that = (ExternalThread) o;
    return pid == that.pid &&
        name.equals(that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pid, name);
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("pid", pid)
        .append("name", name)
        .toString();
  }

}
