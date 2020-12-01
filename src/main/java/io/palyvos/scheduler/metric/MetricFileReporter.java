package io.palyvos.scheduler.metric;

import io.palyvos.scheduler.util.SchedulerContext;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.Validate;

public class MetricFileReporter<T extends Metric> {

  private final PrintWriter out;
  private final MetricProvider<T> provider;
  private final T metric;

  public static <T extends Metric> Collection<MetricFileReporter<T>> reportersFor(MetricProvider<T> provider, T...metrics) {
    final List<MetricFileReporter<T>> reporters = new ArrayList<>();
    Validate.notNull(provider, "provider");
    Validate.notEmpty(metrics, "No metric provided");
    for (T metric : metrics) {
      reporters.add(new MetricFileReporter<>(metric, provider));
    }
    return reporters;
  }

  public MetricFileReporter(T metric, MetricProvider<T> provider) {
    final String outputFile = outputFile(metric);
    try {
      FileWriter outFile = new FileWriter(outputFile);
      out = new PrintWriter(outFile, SchedulerContext.STATISTICS_AUTO_FLUSH);
    } catch (IOException e) {
      throw new IllegalArgumentException(
          String.format("Failed to open file %s for writing: %s", outputFile, e.getMessage()), e);
    }
    this.provider = provider;
    this.metric = metric;
  }

  private String outputFile(T metric) {
    return SchedulerContext.STATISTICS_FOLDER + File.separator + "metric." + metric + ".csv";
  }

  public void report() {
    final long timestamp = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    for (Map.Entry<String, Double> metricEntry : provider.get(metric).entrySet()) {
      out.format("%d,%s,%f\n", timestamp, metricEntry.getKey(), metricEntry.getValue());
    }
  }
}
