package io.palyvos.scheduler.metric;

import java.util.Map;
import java.util.function.BiFunction;
import org.apache.commons.lang3.Validate;

public interface MetricProvider<T extends Metric> extends Runnable {

  void register(T metric);

  default Double get(T metric, String key) {
    Map<String, Double> metricValues = get(metric);
    Validate.validState(metricValues != null, "No values for metric %s!", metric);
    return metricValues.get(key);
  }

  Map<String, Double> get(T metric);

  void compute(T metric);

  boolean canProvide(Metric metric);

  T toProvidedMetric(Metric metric);

  void reset();

  Class<T> metricClass();

  void mergeMetricValues(T metric, Map<String, Double> newMetricValues,
      BiFunction<Double, Double, Double> mergeFunction);

  void replaceMetricValues(T metric, Map<String, Double> newMetricValues);
}
