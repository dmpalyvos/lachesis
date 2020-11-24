package io.palyvos.scheduler.metric.graphite;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.DoubleBinaryOperator;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * JSON Object mapping for metrics from Graphite.
 */
public class GraphiteMetricReport {

  public static final int DATAPOINT_VALUE_INDEX = 0;
  private String target;
  private Map<String, String> tags;
  /** List of lists in the format((value, ts), ...) */
  private List<List<Double>> datapoints;

  public String name() {
    return target;
  }

  public double average(double defaultValue) {
    return datapoints.stream().map(dp -> dp.get(DATAPOINT_VALUE_INDEX))
        .filter(Objects::nonNull)
        .mapToDouble(v -> v).average().orElse(defaultValue);
  }

  public double reduce(double identity, DoubleBinaryOperator operator) {
    return datapoints.stream().map(dp -> dp.get(DATAPOINT_VALUE_INDEX))
        .filter(Objects::nonNull)
        .mapToDouble(Double::doubleValue).reduce(identity, operator);
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
