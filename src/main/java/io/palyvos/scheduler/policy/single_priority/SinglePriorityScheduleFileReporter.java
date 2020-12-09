package io.palyvos.scheduler.policy.single_priority;

import io.palyvos.scheduler.util.SchedulerContext;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class SinglePriorityScheduleFileReporter {

  private static final String INTERNAL_PRIORITIES_FILENAME = "schedule-internal.csv";
  private static final String EXTERNAL_PRIORITIES_FILENAME = "schedule-external.csv";
  private final PrintWriter internalOut;
  private final PrintWriter externalOut;

  public SinglePriorityScheduleFileReporter() {
    final String internalPrioritiesFile =
        SchedulerContext.STATISTICS_FOLDER + File.separator + INTERNAL_PRIORITIES_FILENAME;
    final String externalPrioritiesFile =
        SchedulerContext.STATISTICS_FOLDER + File.separator + EXTERNAL_PRIORITIES_FILENAME;
    try {
      internalOut = new PrintWriter(new FileWriter(internalPrioritiesFile),
          SchedulerContext.STATISTICS_AUTO_FLUSH);
      externalOut = new PrintWriter(new FileWriter(externalPrioritiesFile),
          SchedulerContext.STATISTICS_AUTO_FLUSH);
    } catch (IOException e) {
      throw new IllegalArgumentException(
          String.format("Failed to open file for writing: %s", e.getMessage()), e);
    }
  }

  public void add(long timestamp, String thread, Long externalPriority, Double internalPriority) {
    internalOut.format("%s,%d,%f\n", thread, timestamp, internalPriority != null ? internalPriority : 0);
    externalOut.format("%s,%d,%d\n", thread, timestamp, externalPriority != null ? externalPriority : 0);
  }
}
