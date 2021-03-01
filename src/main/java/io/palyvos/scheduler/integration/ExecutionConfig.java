package io.palyvos.scheduler.integration;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import io.palyvos.scheduler.adapters.SpeAdapter;
import io.palyvos.scheduler.integration.converter.CGroupPolicyConverter;
import io.palyvos.scheduler.integration.converter.CGroupTranslatorConverter;
import io.palyvos.scheduler.integration.converter.Log4jLevelConverter;
import io.palyvos.scheduler.integration.converter.SinglePriorityPolicyConverter;
import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.policy.cgroup.CGroupPolicy;
import io.palyvos.scheduler.policy.cgroup.CGroupTranslator;
import io.palyvos.scheduler.policy.cgroup.CpuSharesCGroupTranslator;
import io.palyvos.scheduler.policy.cgroup.NoopCGroupPolicy;
import io.palyvos.scheduler.policy.normalizers.DecisionNormalizer;
import io.palyvos.scheduler.policy.normalizers.ExponentialSmoothingDecisionNormalizer;
import io.palyvos.scheduler.policy.normalizers.IdentityDecisionNormalizer;
import io.palyvos.scheduler.policy.normalizers.LogDecisionNormalizer;
import io.palyvos.scheduler.policy.normalizers.MinMaxDecisionNormalizer;
import io.palyvos.scheduler.policy.single_priority.ConstantSinglePriorityPolicy;
import io.palyvos.scheduler.policy.single_priority.DelegatingMultiSpeSinglePriorityPolicy;
import io.palyvos.scheduler.policy.single_priority.NiceSinglePriorityTranslator;
import io.palyvos.scheduler.policy.single_priority.NoopSinglePriorityPolicy;
import io.palyvos.scheduler.policy.single_priority.SinglePriorityPolicy;
import io.palyvos.scheduler.policy.single_priority.SinglePriorityTranslator;
import io.palyvos.scheduler.util.SchedulerContext;
import io.palyvos.scheduler.util.command.JcmdCommand;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.Validate;
import org.apache.commons.math3.util.ArithmeticUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

class ExecutionConfig {

  private static final Logger LOG = LogManager.getLogger();
  final static int GRAPHITE_RECEIVE_PORT = 80;
  private static final int RETRY_INTERVAL_MILLIS = 5000;
  private static final int MAX_RETRIES = 20;
  private static final long MAX_RETRY_TIME_SECONDS = 75;

  final List<Integer> pids = new ArrayList<>();

  @Parameter(names = "--log", converter = Log4jLevelConverter.class, description = "Logging level (e.g., DEBUG, INFO, etc)")
  Level log = Level.INFO;

  @Parameter(names = "--queryGraph", description = "Path to the query graph yaml file")
  List<String> queryGraphPath = new ArrayList<>();

  @Parameter(names = "--period", description = "(Minimum) scheduling period, in seconds")
  long period = 1;

  @Parameter(names = "--cgroupPeriod", description = "(Minimum) cgroup period, in seconds")
  long cgroupPeriod = 1;

  @Parameter(names = "--window", description = "Time-window (seconds) to consider for recent metrics")
  int window = 10;

  @Parameter(names = "--smoothingFactor", description = "Alpha for exponential smoothing, between [0, 1]. Lower alpha -> smoother priorities.")
  double smoothingFactor = 1;

  @Parameter(names = "--policy", description =
      "Scheduling policy to apply, either random[:true], constant:{PRIORITY_VALUE}[:true], or metric:{METRIC_NAME}[:true] or none. "
          + "The optional true argument controls scheduling of helper threads", converter = SinglePriorityPolicyConverter.class, required = true)
  SinglePriorityPolicy policy = new NoopSinglePriorityPolicy();

  @Parameter(names = "--cgroupPolicy", converter = CGroupPolicyConverter.class)
  CGroupPolicy cgroupPolicy = new NoopCGroupPolicy();

  @Parameter(names = "--cgroupTranslator", converter = CGroupTranslatorConverter.class)
  CGroupTranslator cGroupTranslator = new CpuSharesCGroupTranslator();

  @Parameter(names = "--maxPriority", description = "Maximum translated priority value")
  int maxPriority = -20;

  @Parameter(names = "--minPriority", description = "Minimum translated priority value")
  int minPriority = 0;

  @Parameter(names = "--logarithmic", description = "Take the logarithm of the priorities before converting to nice values")
  boolean logarithmic = false;

  @Parameter(names = "--statisticsFolder", description = "Path to store the scheduler statistics")
  String statisticsFolder = ".";

  @Parameter(names = "--statisticsHost", description = "Path to store the scheduler statistics", required = true)
  String statisticsHost;

  @Parameter(names = "--help", help = true)
  boolean help = false;

  @Parameter(names = "--worker", description = "Part of the command of the worker thread (e.g., class name). Argument can be repeated for multiple worker patterns.", required = true)
  List<String> workerPatterns = new ArrayList<>();

  private long lastCgroupPolicyRun;
  private long lastPolicyRun;
  private long sleepTime = -1;

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

    for (String workerPattern : config.workerPatterns) {
      config.retrievePids(workerPattern, mainClass);
    }

    SchedulerContext.initSpeProcessInfo(config.pids.get(0));
    SchedulerContext.switchToSpeProcessContext();
    SchedulerContext.METRIC_RECENT_PERIOD_SECONDS = config.window;
    SchedulerContext.STATISTICS_FOLDER = config.statisticsFolder;
    return config;
  }

  void retrievePids(String workerPattern, Class<?> mainClass) throws InterruptedException {
    LOG.info("Trying to retrieve worker PID for '{}'...", workerPattern);
    for (int i = 0; i < MAX_RETRIES; i++) {
      try {
        // Ignore PID of current command because it also contains workerPattern as an argument
        List<Integer> workerPids = new JcmdCommand().pidsFor(workerPattern, mainClass.getName());
        pids.addAll(workerPids);
        LOG.info("Success retrieving PID(s) for '{}': {}", workerPattern, workerPids);
        return;
      } catch (Exception exception) {
        Thread.sleep(RETRY_INTERVAL_MILLIS);
      }
    }
    throw new IllegalStateException(
        String.format("Failed to retrieve worker PID(s): %s", workerPattern));
  }

  static void tryUpdateTasks(SpeAdapter adapter) throws InterruptedException {
    int tries = 0;
    LOG.info("Trying to fetch tasks...");
    while (true) {
      try {
        adapter.updateTasks();
        Validate.validState(!adapter.tasks().isEmpty(), "No tasks found!");
        LOG.info("Success!");
        return;
      } catch (Exception exception) {
        if (tries++ >= MAX_RETRIES) {
          LOG.error("Failed to retrieve SPE tasks!");
          throw exception;
        }
        Thread.sleep(RETRY_INTERVAL_MILLIS);
      }
    }
  }

  void sleep() throws InterruptedException {
    if (sleepTime < 0) {
      sleepTime = ArithmeticUtils.gcd(period, cgroupPeriod);
    }
    Thread.sleep(TimeUnit.SECONDS.toMillis(sleepTime));
  }


  void scheduleMulti(DelegatingMultiSpeSinglePriorityPolicy policy,
      List<SpeAdapter> adapters,
      List<SchedulerMetricProvider> metricProviders, SinglePriorityTranslator translator,
      List<Double> scalingFactors) {
    final long now = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    boolean timeToRunPolicy = isTimeToRunPolicy(now);
    if (!timeToRunPolicy) {
      return;
    }
    policy.reset();
    metricProviders.forEach(metricProvider -> metricProvider.run());
    for (int i = 0; i < adapters.size(); i++) {
      SpeAdapter adapter = adapters.get(i);
      SchedulerMetricProvider metricProvider = metricProviders.get(i);
      policy.update(adapter.taskIndex().tasks(), metricProvider, scalingFactors.get(i));
    }
    policy.apply(translator);
    onPolicyExecuted(now);
  }

  void schedule(SpeAdapter adapter,
      SchedulerMetricProvider metricProvider, SinglePriorityTranslator translator) {
    final long now = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    boolean timeToRunPolicy = isTimeToRunPolicy(now);
    boolean timeToRunCGroupPolicy = isTimeToRunCGroupPolicy(now);
    if (timeToRunPolicy || timeToRunCGroupPolicy) {
      metricProvider.run();
    }
    if (timeToRunPolicy) {
      policy.apply(adapter.taskIndex().tasks(), translator, metricProvider);
      onPolicyExecuted(now);
    }
    if (timeToRunCGroupPolicy) {
      cgroupPolicy.apply(adapter.tasks(), cGroupTranslator, metricProvider);
      onCGroupPolicyExecuted(now);
    }
  }

  private void onCGroupPolicyExecuted(long now) {
    if (lastCgroupPolicyRun <= 0) {
      LOG.info("Started cgroup scheduling");
    }
    lastCgroupPolicyRun = now;
  }

  private void onPolicyExecuted(long now) {
    if (lastPolicyRun <= 0) {
      LOG.info("Started scheduling");
    }
    lastPolicyRun = now;
  }

  boolean isTimeToRunCGroupPolicy(long now) {
    return lastCgroupPolicyRun + cgroupPeriod < now;
  }

  boolean isTimeToRunPolicy(long now) {
    return lastPolicyRun + period < now;
  }

  long maxRetries() {
    return MAX_RETRY_TIME_SECONDS / period;
  }


  SinglePriorityTranslator newSinglePriorityTranslator() {
    if (policy instanceof ConstantSinglePriorityPolicy) {
      return new NiceSinglePriorityTranslator(new IdentityDecisionNormalizer());
    }
    DecisionNormalizer normalizer = new MinMaxDecisionNormalizer(minPriority,
        maxPriority);
    if (logarithmic) {
      normalizer = new LogDecisionNormalizer(normalizer);
    }
    normalizer = new ExponentialSmoothingDecisionNormalizer(normalizer, smoothingFactor);
    SinglePriorityTranslator translator = new NiceSinglePriorityTranslator(normalizer);
    return translator;
  }

}
