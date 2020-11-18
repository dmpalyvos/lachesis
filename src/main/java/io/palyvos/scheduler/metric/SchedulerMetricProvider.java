package io.palyvos.scheduler.metric;

import io.palyvos.scheduler.task.TaskGraphTraverser;
import io.palyvos.scheduler.task.TaskIndex;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SchedulerMetricProvider implements MetricProvider<SchedulerMetric> {

  private static final Logger LOG = LogManager.getLogger(SchedulerMetricProvider.class);

  private final Map<SchedulerMetric, ProviderEntry> registry = new HashMap<>();
  private final List<MetricProvider<?>> providers;
  private TaskIndex taskIndex;
  private TaskGraphTraverser taskGraphTraverser;

  public SchedulerMetricProvider(MetricProvider<?>... providers) {
    Validate.notEmpty(providers, "At least one provider required!");
    List<MetricProvider<?>> providerList = new ArrayList<>(Arrays.asList(providers));
    providerList.add(new CompositeMetricProvider(this));
    this.providers = Collections.unmodifiableList(providerList);
  }

  public SchedulerMetricProvider setTaskIndex(TaskIndex taskIndex) {
    Validate.notNull(taskIndex, "taskIndex");
    this.taskIndex = taskIndex;
    this.taskGraphTraverser = new TaskGraphTraverser(taskIndex.tasks());
    return this;
  }

  @Override
  public void register(SchedulerMetric metric) {
    Validate.notNull(metric, "metric");
    LOG.trace("Attempting to register metric {}", metric);
    if (registry.containsKey(metric)) {
      LOG.trace("Already registered to {}", registry.get(metric));
      return;
    }
    //FIXME: Ignore if metric registered already!
    for (MetricProvider provider : providers) {
      if (provider.canProvide(metric)) {
        LOG.info("Registering metric {} with provider {}", metric, provider.getClass().getSimpleName());
        Metric providedMetric = provider.toProvidedMetric(metric);
        provider.register(providedMetric);
        registry.put(metric, new ProviderEntry(provider, providedMetric));
        return;
      }
    }
    throw new IllegalStateException(String.format("No provider can provide %s", metric));
  }

  @Override
  public Map<String, Double> get(SchedulerMetric metric) {
    ProviderEntry providerEntry = providerEntry(metric);
    LOG.trace("Returning metric {} from delegate {}", metric, providerEntry);
    Map<String, Double> data = providerEntry.getMetric();
    Validate.validState(data != null, "No metric data for %s", metric);
    return data;
  }

  @Override
  public void run() {
    Validate.validState(taskIndex != null, "Task Index not defined!");
    LOG.trace("Metric Registry: {}", registry);
    reset();
    for (SchedulerMetric metric : registry.keySet()) {
      compute(metric);
    }
  }

  public void compute(SchedulerMetric metric) {
    metric.dependencies().forEach(m -> compute(m));
    doCompute(metric);
  }

  public TaskIndex taskIndex() {
    return taskIndex;
  }

  public TaskGraphTraverser taskGraphTraverser() {
    return taskGraphTraverser;
  }

  protected void doCompute(SchedulerMetric metric) {
    providerEntry(metric).computeMetric();
  }

  @Override
  public boolean canProvide(Metric metric) {
    return registry.containsKey(metric);
  }

  @Override
  public SchedulerMetric toProvidedMetric(Metric metric) {
    Validate.isInstanceOf(BasicSchedulerMetric.class, metric);
    return (SchedulerMetric) metric;
  }

  private ProviderEntry providerEntry(SchedulerMetric metric) {
    ProviderEntry providerEntry = registry.get(metric);
    Validate.validState(providerEntry != null, "Metric %s has not been registered!", metric);
    return providerEntry;
  }

  @Override
  public Class<SchedulerMetric> metricClass() {
    return SchedulerMetric.class;
  }

  @Override
  public void mergeMetricValues(SchedulerMetric metric, Map<String, Double> newMetricValues,
      BiFunction<Double, Double, Double> mergeFunction) {
    providerEntry(metric).mergeValues(newMetricValues, mergeFunction);
  }

  @Override
  public void replaceMetricValues(SchedulerMetric metric, Map<String, Double> newMetricValues) {
    providerEntry(metric).replaceValues(newMetricValues);
  }

  @Override
  public void reset() {
    for (MetricProvider provider : providers) {
      provider.reset();
    }
  }

  private static class ProviderEntry {

    final MetricProvider provider;
    final Metric metric;

    public <T extends Metric<T>> ProviderEntry(MetricProvider<T> provider, Metric<T> metric) {
      Validate.notNull(provider, "provider");
      Validate.notNull(metric, "metric");
      Validate.isInstanceOf(provider.metricClass(), metric,
          "Invalid metric class %s for provider %s!", metric.getClass().getName(), provider);
      this.provider = provider;
      this.metric = metric;
    }

    void computeMetric() {
      provider.compute(metric);
    }

    Map<String, Double> getMetric() {
      return provider.get(metric);
    }

    @Override
    public String toString() {
      return new ToStringBuilder(this, ToStringStyle.SIMPLE_STYLE)
          .append("provider", provider)
          .append("metric", metric)
          .toString();
    }

    public void mergeValues(Map<String, Double> newMetricValues,
        BiFunction<Double, Double, Double> mergeFunction) {
      provider.mergeMetricValues(metric, newMetricValues, mergeFunction);
    }

    public void replaceValues(Map<String, Double> newMetricValues) {
      provider.replaceMetricValues(metric, newMetricValues);
    }
  }

  public void prettyPrint(SchedulerMetric metric) {
    Map<String, Double> metricValues = get(metric);
    System.out.format("----------- %s -----------\n", metric);
    for (String key : metricValues.keySet()) {
      System.out.format("%s:\t%.6f\n", key, metricValues.get(key));
    }
    System.out.println();
  }

}
