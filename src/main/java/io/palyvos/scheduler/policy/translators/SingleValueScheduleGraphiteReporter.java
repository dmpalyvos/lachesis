package io.palyvos.scheduler.policy.translators;

import io.palyvos.scheduler.metric.graphite.SimpleGraphiteReporter;
import io.palyvos.scheduler.util.SchedulerContext;
import java.io.IOException;

public class SingleValueScheduleGraphiteReporter {

  public static final String SCHEDULE_GRAPHITE_PREFIX = "schedule";
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
      reporter.report(timestamp, graphiteKey(convertedThread, "external"), externalPriority != null ? externalPriority : 0);
      reporter.report(timestamp, graphiteKey(convertedThread, "internal"), internalPriority != null ? internalPriority : 0);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private String graphiteKey(String thread, String priorityType) {
    return String.format("%s.%s.%s.%s",
            SchedulerContext.SCHEDULER_NAME, SCHEDULE_GRAPHITE_PREFIX, thread, priorityType);
  }

  private String graphiteCompatibleThreadName(String thread) {
    String cleanedName = SimpleGraphiteReporter.cleanGraphiteId(thread);
    return SchedulerContext.THREAD_NAME_GRAPHITE_CONVERTER.apply(cleanedName);
  }

}
