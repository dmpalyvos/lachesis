package io.palyvos.scheduler.integration;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import io.palyvos.scheduler.adapters.SpeAdapter;
import io.palyvos.scheduler.adapters.liebre.LiebreAdapter;
import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.policy.CGroupNoopPolicy;
import io.palyvos.scheduler.policy.CGroupSchedulingPolicy;
import io.palyvos.scheduler.policy.ConcreteSchedulingPolicy;
import io.palyvos.scheduler.policy.translators.concrete.ConcretePolicyTranslator;
import io.palyvos.scheduler.integration.converter.CGroupPolicyConverter;
import io.palyvos.scheduler.integration.converter.ConcreteSchedulingPolicyConverter;
import io.palyvos.scheduler.util.command.JcmdCommand;
import io.palyvos.scheduler.integration.converter.Log4jLevelConverter;
import io.palyvos.scheduler.util.SchedulerContext;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

class ExecutionConfig {

  private static final Logger LOG = LogManager.getLogger();

  List<Integer> pids;

  @Parameter(names = "--log", converter = Log4jLevelConverter.class, description = "Logging level (e.g., DEBUG, INFO, etc)")
  Level log = Level.INFO;

  @Parameter(names = "--queryGraph", description = "Path to the query graph yaml file")
  String queryGraphPath;

  @Parameter(names = "--period", description = "(Minimum) scheduling period, in seconds")
  long period = 1;

  @Parameter(names = "--cgroupPeriod", description = "(Minimum) cgroup period, in seconds")
  long cgroupPeriod = 1;

  @Parameter(names = "--window", description = "Time-window (seconds) to consider for recent metrics")
  int window = 10;

  @Parameter(names = "--smoothingFactor", description = "Alpha for exponential smoothing, between [0, 1]. Lower alpha -> smoother priorities.")
  double smoothingFactor = 1;

  @Parameter(names = "--policy", description =
      "Scheduling policy to apply, either random[:true], constant:{PRIORITY_VALUE}[:true], or metric:{METRIC_NAME}[:true]. "
          + "The optional true argument controls scheduling of helper threads", converter = ConcreteSchedulingPolicyConverter.class, required = true)
  ConcreteSchedulingPolicy policy;

  @Parameter(names = "--cgroupPolicy", converter = CGroupPolicyConverter.class)
  CGroupSchedulingPolicy cgroupPolicy = new CGroupNoopPolicy();

  @Parameter(names = "--maxPriority", description = "Maximum translated priority value")
  int maxPriority = -20;

  @Parameter(names = "--minPriority", description = "Minimum translated priority value")
  int minPriority = 0;

  @Parameter(names = "--logarithmic", description = "Take the logarithm of the priorities before converting to nice values")
  boolean logarithmic = false;

  @Parameter(names = "--statisticsFolder", description = "Path to store the scheduler statistics")
  String statisticsFolder = ".";

  @Parameter(names = "--help", help = true)
  boolean help = false;

  @Parameter(names = "--worker", description = "Pattern of the worker thread (e.g., class name)", required = true)
  String workerPattern;


  private long lastCgroupPolicyRun;
  private long lastPolicyRun;

  public static ExecutionConfig init(String[] args, Class<?> mainClass)
      throws InterruptedException {
    ExecutionConfig config = new ExecutionConfig();
    JCommander jCommander = JCommander.newBuilder().addObject(config).build();
    jCommander.parse(args);
    if (config.help) {
      jCommander.usage();
      System.exit(0);
    }
    Configurator.setRootLevel(config.log);
    config.retrievePids(mainClass);

    SchedulerContext.initSpeProcessInfo(config.pids.get(0));
    SchedulerContext.switchToSpeProcessContext();
    SchedulerContext.METRIC_RECENT_PERIOD_SECONDS = config.window;
    SchedulerContext.STATISTICS_FOLDER = config.statisticsFolder;
    return config;
  }

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
        LOG.info("Trying to fetch tasks...");
        adapter.updateTasks();
        Validate.validState(!adapter.tasks().isEmpty(), "No tasks found!");
        LOG.info("Success!");
        return;
      } catch (Exception exception) {
        exception.printStackTrace();
        Thread.sleep(5000);
      }
    }
    throw new IllegalStateException("Failed to retrieve storm tasks!");
  }

  void sleep() throws InterruptedException {
    Thread.sleep(TimeUnit.SECONDS.toMillis(Math.min(period, cgroupPeriod)));
  }


  void schedule(ExecutionConfig config, LiebreAdapter adapter,
      SchedulerMetricProvider metricProvider, ConcretePolicyTranslator translator) {
    final long now = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    if (lastPolicyRun + period < now) {
      policy.apply(adapter.taskIndex().tasks(), translator, metricProvider);
      lastPolicyRun = now;
    }
    if (lastCgroupPolicyRun + cgroupPeriod < now) {
      cgroupPolicy.apply(adapter.tasks(), metricProvider);
      lastCgroupPolicyRun = now;
    }
  }

}
