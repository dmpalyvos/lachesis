package io.palyvos.scheduler.metric.graphite;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.Validate;

/**
 * Periodically reports the average value for every requested key to graphite using a {@link
 * SimpleGraphiteReporter} delegate.
 */
public class PeriodicGraphiteReporter {

  private final String prefix;
  private final long periodSeconds;
  private final SimpleGraphiteReporter delegate;
  private final Map<String, AverageValueReport> reports = new HashMap<>();
  private long previousTimestampSeconds = -1;

  public PeriodicGraphiteReporter(String prefix, long periodSeconds,
      SimpleGraphiteReporter delegate) {
    Validate.notEmpty(prefix, "empty prefix");
    Validate.isTrue(periodSeconds >= 0, "negative period");
    Validate.notNull(delegate, "delegate");
    this.prefix = prefix;
    this.periodSeconds = periodSeconds;
    this.delegate = delegate;
  }

  public void report(String key, double value) throws IOException {
    AverageValueReport report = reports.computeIfAbsent(key, k -> new AverageValueReport());
    report.add(value);
    final long timestampSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    if (timestampSeconds > previousTimestampSeconds + periodSeconds) {
      previousTimestampSeconds = timestampSeconds;
      delegate.open();
      for (Entry<String, AverageValueReport> entry : reports.entrySet()) {
        String k = entry.getKey();
        AverageValueReport r = entry.getValue();
        String fullKey = prefix + "." + k;
        delegate.report(timestampSeconds, fullKey, r.getAndReset());
      }
      delegate.close();
    }
  }

  private static class AverageValueReport {

    private double sum = 0;
    private int count = 0;

    void add(double value) {
      sum += value;
      count += 1;
    }

    double get() {
      return sum / count;
    }

    double getAndReset() {
      double result = get();
      reset();
      return result;
    }

    void reset() {
      sum = 0;
      count = 0;
    }

  }
}
