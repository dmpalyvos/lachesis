package io.palyvos.scheduler.adapters.storm;


import java.util.List;
import org.apache.commons.lang3.builder.ToStringBuilder;

class StormComponentMetrics {

  public String id;
  public List<StormMeasurement> emitted = null;
  public List<StormMeasurement> transferred = null;
  public List<StormMeasurement> acked = null;
  public List<StormMeasurement> failed = null;
  public List<StormMeasurement> processMsAvg = null;
  public List<StormMeasurement> executed = null;
  public List<StormMeasurement> executedMsAvg = null;

  @Override
  public String toString() {
    return new ToStringBuilder(this).append("id", id).append("emitted", emitted)
        .append("transferred", transferred).append("acked", acked).append("failed", failed)
        .append("processMsAvg", processMsAvg).append("executed", executed)
        .append("executedMsAvg", executedMsAvg)
        .toString();
  }

}
