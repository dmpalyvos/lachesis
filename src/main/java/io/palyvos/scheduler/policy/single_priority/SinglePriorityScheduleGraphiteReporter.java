package io.palyvos.scheduler.policy.single_priority;

import io.palyvos.scheduler.metric.graphite.SimpleGraphiteReporter;
import io.palyvos.scheduler.util.SchedulerContext;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SinglePriorityScheduleGraphiteReporter {
  private static final Logger LOG = LogManager.getLogger();

  public static final String GRAPHITE_PREFIX = "schedule.thread";
  private final SimpleGraphiteReporter reporter;
  private final String localIp;

  public SinglePriorityScheduleGraphiteReporter(String host, int port) {
    this.reporter = new SimpleGraphiteReporter(host, port);
    try {
      this.localIp = Inet4Address.getLocalHost().getHostAddress().replace(".", "-");
    } catch (UnknownHostException e) {
      throw new IllegalStateException(e);
    }
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
    return SchedulerContext.SCHEDULER_NAME + "."
        + localIp + "."
        + GRAPHITE_PREFIX + "."
        + type + "."
        + entity;
  }

  private String graphiteCompatibleThreadName(String thread) {
    String cleanedName = SimpleGraphiteReporter.cleanGraphiteId(thread);
    return SchedulerContext.THREAD_NAME_GRAPHITE_CONVERTER.apply(cleanedName);
  }

}
