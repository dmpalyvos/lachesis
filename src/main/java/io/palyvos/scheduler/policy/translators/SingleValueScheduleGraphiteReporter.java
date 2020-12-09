package io.palyvos.scheduler.policy.translators;

import io.palyvos.scheduler.metric.graphite.SimpleGraphiteReporter;
import io.palyvos.scheduler.util.SchedulerContext;
import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SingleValueScheduleGraphiteReporter {
  private static final Logger LOG = LogManager.getLogger();

  public static final String GRAPHITE_PREFIX = "schedule.thread";
  private final SimpleGraphiteReporter reporter;

  public SingleValueScheduleGraphiteReporter(String host, int port) {
    this.reporter = new SimpleGraphiteReporter(host, port);
  }

  public void open() {
    reporter.open();
  }

  public void close() {
    reporter.close();
  }

  public void add(long timestamp, String thread, Long externalPriority, Double internalPriority) {
    String convertedThread = graphiteCompatibleThreadName(thread);
    try {
      if (externalPriority != null) {
        reporter.report(timestamp, graphiteKey("external", convertedThread),
            externalPriority);
      }
      if (internalPriority != null) {
        reporter.report(timestamp, graphiteKey("internal", convertedThread),
            internalPriority);
      }
    } catch (IOException e) {
      LOG.warn("Failed to report statistics to graphite: {}", e.getMessage());
    }
  }


  private String graphiteKey(String type, String entity) {
    return new StringBuffer(SchedulerContext.SCHEDULER_NAME).append(".")
        .append(GRAPHITE_PREFIX).append(".")
        .append(type).append(".")
        .append(entity).toString();
  }

  private String graphiteCompatibleThreadName(String thread) {
    String cleanedName = SimpleGraphiteReporter.cleanGraphiteId(thread);
    return SchedulerContext.THREAD_NAME_GRAPHITE_CONVERTER.apply(cleanedName);
  }

}
