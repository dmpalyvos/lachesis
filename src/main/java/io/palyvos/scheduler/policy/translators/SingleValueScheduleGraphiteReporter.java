package io.palyvos.scheduler.policy.translators;

import io.palyvos.scheduler.metric.graphite.SimpleGraphiteReporter;
import io.palyvos.scheduler.util.SchedulerContext;
import java.io.IOException;

public class SingleValueScheduleGraphiteReporter {

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

  public void add(long timestamp, String thread, long externalPriority, double internalPriority) {
    //FIXME: Dirty patch to produce a graphite-compatible key. This needs to be SPE-specific!
    String convertedThread = graphiteCompatibleThreadName(thread);
    try {
      reporter.report(timestamp, String.format("lachesis.%s.external", convertedThread),
          externalPriority);
      reporter.report(timestamp, String.format("lachesis.%s.internal", convertedThread),
          internalPriority);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private String graphiteCompatibleThreadName(String thread) {
    String cleanedName = thread.replaceAll("[^A-Za-z0-9\\-]", "");
    return SchedulerContext.THREAD_NAME_GRAPHITE_CONVERTER.apply(cleanedName);
  }
}
