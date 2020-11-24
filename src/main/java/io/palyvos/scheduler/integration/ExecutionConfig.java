package io.palyvos.scheduler.integration;

import com.beust.jcommander.Parameter;
import io.palyvos.scheduler.adapters.SpeAdapter;
import io.palyvos.scheduler.adapters.storm.StormConstants;
import io.palyvos.scheduler.policy.ConcreteSchedulingPolicy;
import io.palyvos.scheduler.util.ConcreteSchedulingPolicyConverter;
import io.palyvos.scheduler.util.JcmdCommand;
import io.palyvos.scheduler.util.Log4jLevelConverter;
import java.util.List;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class ExecutionConfig {

  private static final Logger LOG = LogManager.getLogger();

  List<Integer> pids;

  @Parameter(names = "--log", converter = Log4jLevelConverter.class, description = "Logging level (e.g., DEBUG, INFO, etc)")
  Level log = Level.DEBUG;

  @Parameter(names = "--queryGraph", required = true, description = "Path to the query graph yaml file")
  String queryGraphPath;

  @Parameter(names = "--period", description = "(Minimum) scheduling period, in seconds")
  long period = 1;

  @Parameter(names = "--window", description = "Time-window (seconds) to consider for recent metrics")
  int window = 10;

  @Parameter(names = "--policy", description = "Scheduling policy to apply, either random, constant:{PRIORITY_VALUE}, or metric:{METRIC_NAME}", converter = ConcreteSchedulingPolicyConverter.class)
  ConcreteSchedulingPolicy policy;

  @Parameter(names = "--maxPriority", description = "Maximum translated priority value")
  int maxPriority = -20;

  @Parameter(names = "--minPriority", description = "Minimum translated priority value")
  int minPriority = 10;

  @Parameter(names = "--logarithmic", description = "Take the logarithm of the priorities before converting to nice values")
  boolean logarithmic = false;

  @Parameter(names = "--statisticsFolder", description = "Path to store the scheduler statistics")
  String statisticsFolder = ".";

  @Parameter(names = "--help", help = true)
  boolean help = false;

  @Parameter(names = "--worker", description = "Pattern of the worker thread (e.g., class name)")
  String workerPattern = StormConstants.STORM_WORKER_CLASS;

  void retrievePids(Class<?> mainClass) throws InterruptedException {
    final int tries = 20;
    for (int i = 0; i < tries; i++) {
      try {
        LOG.info("Trying to retrieve worker PID...");
        // Ignore PID of current command because it also contains workerPattern as an argument
        pids = new JcmdCommand().pidsFor(workerPattern, mainClass.getName());
        LOG.info("Success!");
        return;
      } catch (Exception exception) {
        Thread.sleep(5000);
      }
    }
    throw new IllegalStateException("Failed to retrieve worker PID(s)!");
  }

  static void tryUpdateTasks(SpeAdapter adapter) throws InterruptedException {
    final int tries = 20;
    for (int i = 0; i < tries; i++) {
      try {
        LOG.info("Trying to fetch storm tasks...");
        adapter.updateTasks();
        LOG.info("Success!");
        return;
      } catch (Exception exception) {
        Thread.sleep(5000);
      }
    }
    throw new IllegalStateException("Failed to retrieve storm tasks!");
  }

}
