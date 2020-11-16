package io.palyvos.scheduler.adapters.liebre;

import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * JSON Object mapping for LiebreAdapter metrics from Graphite.
 */
class LiebreMetricReport {

  public static final String NAME_TAG = "name";
  public static final int DATAPOINT_VALUE_INDEX = 0;
  public static final int STREAM_NAME_INDEX = 3;
  private String target;
  private Map<String, String> tags;
  // datapoints is a list of lists ((value, ts), ...)
  private List<List<Double>> datapoints;

  public String name() {
    return tags.get(NAME_TAG);
  }

  public String simpleName() {
    //FIXME: This is specialized only to stream names and specific query!
    String[] name = name().split("\\.");
    return name[name.length- STREAM_NAME_INDEX];
  }

  public double average() {
    return datapoints.stream().map(dp -> dp.get(DATAPOINT_VALUE_INDEX)).filter(value -> value != null)
        .mapToDouble(v -> v).average().orElse(-1);
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
