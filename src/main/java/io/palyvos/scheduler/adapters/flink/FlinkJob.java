package io.palyvos.scheduler.adapters.flink;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class FlinkJob {

  public static final String RUNNING_STATUS = "RUNNING";
  public final String id;
  public final String status;


  public FlinkJob(String id, String status) {
    this.id = id;
    this.status = status;
  }

  public boolean isRunning() {
    return RUNNING_STATUS.equals(status);
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.NO_CLASS_NAME_STYLE)
        .append("id", id)
        .append("status", status)
        .toString();
  }
}
