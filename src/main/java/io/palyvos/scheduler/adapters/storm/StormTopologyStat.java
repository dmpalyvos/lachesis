package io.palyvos.scheduler.adapters.storm;

import org.apache.commons.lang3.builder.ToStringBuilder;

class StormTopologyStat {

  public String windowPretty;
  public String window;
  public long emitted;
  public long transferred;
  public String completeLatency;
  public long acked;
  public long failed;


  @Override
  public String toString() {
    return new ToStringBuilder(this).append("windowPretty", windowPretty).append("window", window)
        .append("emitted", emitted).append("transferred", transferred)
        .append("completeLatency", completeLatency).append("acked", acked).append("failed", failed)
        .toString();
  }
}
