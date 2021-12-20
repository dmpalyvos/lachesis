package io.palyvos.scheduler.metric;

import java.util.Map;
import java.util.function.BiFunction;
import org.apache.commons.lang3.Validate;

public interface MetricProvider<T extends Metric> extends Runnable {

  /**
   * Register a metric to be fetched by this {@link MetricProvider}. Necessary to call that before
   * {@link #compute(Metric)} or {@link #get(Metric)}.
   *
   * @param metric
   */
  void register(T metric);

  /**
   * Fetch the value of the requested metric and key.
   *
   * @param metric The metric to get the value for.
   * @param key    The entity key to get the value for (e.g., operator, task, etc).
   * @return The value as a double or {@code null} if no value exists.
   */
  default Double get(T metric, String key) {
    Map<String, Double> metricValues = get(metric);
    Validate.validState(metricValues != null, "No values for metric %s!", metric);
    return metricValues.get(key);
  }

  /**
   * Get the values of the requested metrics for all entities.
   *
   * @param metric The metric to get the values for.
   * @return A {@link Map} where the keys are the entities and the values the metric values.
   */
  Map<String, Double> get(T metric);

  /**
   * Compute or fetch the requested metric.
   */
  void compute(T metric);

  /**
   * @return {@code true} if this provider can fetch/compute the requested metric
   */
  boolean canProvide(Metric metric);

  /**
   * Helper function to convert between general (e.g., {@link SchedulerMetric} and SPE-specific
   * metric types.
   */
  T toProvidedMetric(Metric metric);

  /**
   * Reset the metric values.
   */
  void reset();

  /**
   * Class of the metrics compatible with this provider.
   * @return
   */
  Class<T> metricClass();

  void mergeMetricValues(T metric, Map<String, Double> newMetricValues,
      BiFunction<Double, Double, Double> mergeFunction);

  void replaceMetricValues(T metric, Map<String, Double> newMetricValues);
}
