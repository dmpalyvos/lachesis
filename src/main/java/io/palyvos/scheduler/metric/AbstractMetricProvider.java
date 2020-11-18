package io.palyvos.scheduler.metric;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class AbstractMetricProvider<T extends Metric<T>> implements MetricProvider<T> {
  //TODO: Remove class instance checks in get() when interface becomes stable

  private static final Logger LOG = LogManager.getLogger(AbstractMetricProvider.class);

  protected final Set<T> updatedMetrics = new HashSet<>();
  private final Map<T, Map<String, Double>> allMetricValues = new HashMap<>();
  protected final Set<T> registeredMetrics = new HashSet<>();
  private final Class<T> metricClass;
  protected final Map<? extends Metric, T> mapping;

  protected static final <T extends Metric<T>> Map<SchedulerMetric, T> mappingFor(
      T...values) {
    Map<SchedulerMetric, T> mapping = new HashMap<>();
    for (T metric : values) {
      try {
        mapping.put(BasicSchedulerMetric.valueOf(metric.toString()), metric);
      } catch (Exception e) {
        LOG.trace("Metric {} does not correspond to any instance of {}",
            metric,
            metric.getClass().getSimpleName());
      }
    }
    return mapping;
  }

  public AbstractMetricProvider(Map<? extends Metric, T> mapping, Class<T> metricClass) {
    Validate.notNull(mapping, "mapping");
    Validate.notNull(metricClass, "metricClass");
    this.mapping = mapping;
    this.metricClass = metricClass;
  }

  @Override
  public final void register(T metric) {
    Validate.notNull(metric, "metric");
    Validate.isInstanceOf(metricClass, metric,
        "Provider %s cannot compute metric %s", this, metric);
    registeredMetrics.add(metric);
    LOG.trace("Registered metric {} with provider {}", metric, this);
    registerDependencies(metric);
  }

  protected void registerDependencies(T metric) {
    metric.dependencies().forEach(m -> register(m));
  }

  @Override
  public final Map<String, Double> get(T metric) {
    Validate.isInstanceOf(metricClass, metric,
        "Provider %s cannot compute metric %s", this, metric);
    Validate.isTrue(registeredMetrics.contains(metric),
        "Metric %s requested from %s but has not been registered", metric, this);
    return allMetricValues.get(metric);
  }

  @Override
  public final void run() {
    reset();
    for (T metric : registeredMetrics) {
      compute(metric);
    }
  }

  public final void compute(T metric) {
    Validate.isInstanceOf(metricClass, metric,
        "Provider %s cannot compute metric %s", this, metric);
    if (updatedMetrics.contains(metric)) {
      return;
    }
    LOG.debug("Computing metric {} using {}", metric, this);
    updatedMetrics.add(metric);
    metric.dependencies().forEach(m -> compute(m));
    doCompute(metric);
  }

  protected abstract void doCompute(T metric);

  @Override
  public final boolean canProvide(Metric metric) {
    return mapping.containsKey(metric);
  }

  @Override
  public final T toProvidedMetric(Metric metric) {
    Validate.notNull(metric, "metric");
    T providedMetric = mapping.get(metric);
    Validate.isTrue(providedMetric != null, "No provided metric for %s", metric);
    return providedMetric;
  }

  @Override
  public Class<T> metricClass() {
    return metricClass;
  }

  @Override
  public void reset() {
    updatedMetrics.clear();
  }

  @Override
  public final void mergeMetricValues(T metric, Map<String, Double> newMetricValues,
      BiFunction<Double, Double, Double> mergeFunction) {
    Validate.isInstanceOf(metricClass, metric,
        "Provider %s cannot compute metric %s", this, metric);
    if (newMetricValues.isEmpty()) {
      LOG.warn("Empty metric values for {}", metric);
    }
    final Map<String, Double> metricValues = allMetricValues
        .computeIfAbsent(metric, k -> new HashMap<>());
    if (allMetricValues == null) {
      allMetricValues.put(metric, newMetricValues);
      return;
    }
    for (String key : newMetricValues.keySet()) {
      Double oldValue = metricValues.get(key);
      Double newValue = newMetricValues.get(key);
      metricValues.put(key, mergeFunction.apply(oldValue, newValue));
    }
  }

  @Override
  public final void replaceMetricValues(T metric, Map<String, Double> newMetricValues) {
    Validate.notNull(newMetricValues, "newMetricValues");
    Validate.isInstanceOf(metricClass, metric,
        "Provider %s cannot compute metric %s", this, metric);
    if (newMetricValues.isEmpty()) {
      LOG.warn("Empty metric values for {}", metric);
    }
    allMetricValues.put(metric, newMetricValues);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

  public static final BiFunction<Double, Double, Double> SUM = (a, b) -> {
    double ad = a != null ? a : 0;
    double bd = b != null ? b : 0;
    return ad + bd;
  };
  public static final BiFunction<Double, Double, Double> AVERAGE = (a, b) -> {
    double ad = a != null ? a : 0;
    double bd = b != null ? b : 0;
    return (ad + bd) / 2;
  };
}
