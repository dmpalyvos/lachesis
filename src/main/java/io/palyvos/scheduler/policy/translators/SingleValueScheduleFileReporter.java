package io.palyvos.scheduler.policy.translators;

import io.palyvos.scheduler.util.SchedulerContext;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class SingleValueScheduleFileReporter {

  private static final String STATISTICS_FILE = "schedule.csv";
  private final PrintWriter out;

  public SingleValueScheduleFileReporter() {
    final String outputFile = SchedulerContext.STATISTICS_FOLDER + File.separator + STATISTICS_FILE;
    try {
      FileWriter outFile = new FileWriter(outputFile);
      out = new PrintWriter(outFile, SchedulerContext.AUTO_FLUSH);
    } catch (IOException e) {
      throw new IllegalArgumentException(
          String.format("Failed to open file %s for writing: %s", outputFile, e.getMessage()), e);
    }
  }

  public void add(long timestamp, String thread, long externalPriority, double internalPriority) {
    out.format("%d,%s,%d,%f\n", timestamp, thread, externalPriority, internalPriority);
  }
}
