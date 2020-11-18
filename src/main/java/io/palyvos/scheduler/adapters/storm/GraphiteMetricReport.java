package io.palyvos.scheduler.adapters.storm;

import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * JSON Object mapping for LiebreAdapter metrics from Graphite.
 */
//TODO: Generalize and move to util
class GraphiteMetricReport {

  public static final int DATAPOINT_VALUE_INDEX = 0;
  private String target;
  private Map<String, String> tags;
  // Datapoints is a list of lists ((value, ts), ...)
  private List<List<Double>> datapoints;

  public String name() {
    return target;
  }

  public double average() {
    return datapoints.stream().map(dp -> dp.get(DATAPOINT_VALUE_INDEX)).filter(value -> value != null)
        .mapToDouble(v -> v).average().orElse(0);
  }

  public double sum() {
    return datapoints.stream().map(dp -> dp.get(DATAPOINT_VALUE_INDEX)).filter(value -> value != null)
        .mapToDouble(v -> v != null ? v : 0).sum();
  }

  public Double last() {
    return datapoints.get(datapoints.size()-1).get(DATAPOINT_VALUE_INDEX);
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.JSON_STYLE)
        .append("target", target)
        .append("tags", tags)
        .append("datapoints", datapoints)
        .toString();
  }
}
