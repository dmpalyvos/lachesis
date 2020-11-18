package io.palyvos.scheduler.policy.translators;

import io.palyvos.scheduler.util.SchedulerContext;
import io.palyvos.scheduler.util.SimpleGraphiteReporter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.SocketException;

public class SingleValueScheduleGraphiteReporter {

  private final SimpleGraphiteReporter reporter;

  public SingleValueScheduleGraphiteReporter(String host, int port) {
    try {
      this.reporter = new SimpleGraphiteReporter(host, port);
    } catch (SocketException e) {
      throw new IllegalStateException(e);
    }
  }

  public void add(long timestamp, String thread, long externalPriority, double internalPriority) {
    try {
      reporter.report(timestamp, String.format("lachesis.%s.external", thread), externalPriority);
      reporter.report(timestamp, String.format("lachesis.%s.internal", thread), internalPriority);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
