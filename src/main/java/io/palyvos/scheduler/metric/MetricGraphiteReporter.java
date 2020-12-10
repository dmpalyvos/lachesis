package io.palyvos.scheduler.metric;

import io.palyvos.scheduler.metric.graphite.SimpleGraphiteReporter;
import io.palyvos.scheduler.util.SchedulerContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MetricGraphiteReporter<T extends Metric> {

  public static final String METRICS_GRAPHITE_PREFIX = "metrics";
  private final SimpleGraphiteReporter reporter;
  private final MetricProvider<T> provider;
  private final T metric;
  private static final Logger LOG = LogManager.getLogger();

  public static <T extends Metric> Collection<MetricGraphiteReporter<T>> reportersFor(
      String graphiteHost, int graphitePort, MetricProvider<T> provider, T... metrics) {
    final List<MetricGraphiteReporter<T>> reporters = new ArrayList<>();
    Validate.notNull(provider, "provider");
    Validate.notEmpty(metrics, "No metric provided");
    Validate.notBlank(graphiteHost, "no graphite graphiteHost provided");
    for (T metric : metrics) {
      reporters.add(new MetricGraphiteReporter<>(graphiteHost, graphitePort, metric, provider));
      provider.register(metric);
    }
    return reporters;
  }

  public MetricGraphiteReporter(String graphiteHost, int graphitePort, T metric,
      MetricProvider<T> provider) {
    this.reporter = new SimpleGraphiteReporter(graphiteHost, graphitePort);
    this.provider = provider;
    this.metric = metric;
  }

  public void report() {
    final long now = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    reporter.open();
    for (Map.Entry<String, Double> metricEntry : provider.get(metric).entrySet()) {
      try {
        reporter.report(now, graphiteKey(metric, metricEntry.getKey()), metricEntry.getValue());
      } catch (IOException e) {
        LOG.warn("Failed to report value for metric {} to graphite", metric);
      }
    }
    reporter.close();
  }

  private String graphiteKey(T metric, String key) {
    return SchedulerContext.SCHEDULER_NAME + "."
        + METRICS_GRAPHITE_PREFIX + "."
        + SimpleGraphiteReporter.cleanGraphiteId(metric.toString()) + "."
        + SimpleGraphiteReporter.cleanGraphiteId(key);
  }
}
