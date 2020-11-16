package io.palyvos.scheduler.adapters.storm;

import java.util.List;
import org.apache.commons.lang3.builder.ToStringBuilder;

class StormMetricReport {

  public String window;
  public String windowHint;
  public List<StormComponentMetrics> spouts = null;
  public List<StormComponentMetrics> bolts = null;

  @Override
  public String toString() {
    return new ToStringBuilder(this).append("window", window).append("windowHint", windowHint)
        .append("spouts", spouts).append("bolts", bolts)
        .toString();
  }
}
