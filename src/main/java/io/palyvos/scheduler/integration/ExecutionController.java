package io.palyvos.scheduler.integration;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import io.palyvos.scheduler.adapters.SpeAdapter;
import io.palyvos.scheduler.integration.converter.CGroupPolicyConverter;
import io.palyvos.scheduler.integration.converter.Log4jLevelConverter;
import io.palyvos.scheduler.integration.converter.SinglePriorityPolicyConverter;
import io.palyvos.scheduler.metric.BasicSchedulerMetric;
import io.palyvos.scheduler.metric.MetricGraphiteReporter;
import io.palyvos.scheduler.metric.SchedulerMetric;
import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.policy.cgroup.CGroupPolicy;
import io.palyvos.scheduler.policy.cgroup.CGroupTranslator;
import io.palyvos.scheduler.policy.cgroup.CpuQuotaCGroupTranslator;
import io.palyvos.scheduler.policy.cgroup.CpuSharesCGroupTranslator;
import io.palyvos.scheduler.policy.cgroup.NoopCGroupPolicy;
import io.palyvos.scheduler.policy.normalizers.DecisionNormalizer;
import io.palyvos.scheduler.policy.normalizers.IdentityDecisionNormalizer;
import io.palyvos.scheduler.policy.normalizers.LogDecisionNormalizer;
import io.palyvos.scheduler.policy.normalizers.MinMaxDecisionNormalizer;
import io.palyvos.scheduler.policy.normalizers.NiceDecisionNormalizer;
import io.palyvos.scheduler.policy.single_priority.ConstantSinglePriorityPolicy;
import io.palyvos.scheduler.policy.single_priority.NiceSinglePriorityTranslator;
import io.palyvos.scheduler.policy.single_priority.NoopSinglePriorityPolicy;
import io.palyvos.scheduler.policy.single_priority.RandomSinglePriorityPolicy;
import io.palyvos.scheduler.policy.single_priority.RealTimeSinglePriorityTranslator;
import io.palyvos.scheduler.policy.single_priority.SinglePriorityPolicy;
import io.palyvos.scheduler.policy.single_priority.SinglePriorityTranslator;
import io.palyvos.scheduler.util.SchedulerContext;
import io.palyvos.scheduler.util.command.JcmdCommand;
import io.palyvos.scheduler.util.command.RealTimeThreadCommand.RealTimeSchedulingAlgorithm;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.Validate;
import org.apache.commons.math3.util.ArithmeticUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.util.Strings;

class ExecutionController {

  private static final Logger LOG = LogManager.getLogger();
  final static int GRAPHITE_RECEIVE_PORT = 80;
  public static final int RETRY_INTERVAL_MILLIS = 5000;
  public static final int MAX_RETRIES = 20;
  public static final long MAX_SCHEDULE_RETRY_TIME = 75;

  private Collection<MetricGraphiteReporter<SchedulerMetric>> extraMetricReporters;

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

  @Parameter(names = "--distributed", description = "Leader hostname (in case of distributed execution)")
  String distributed;

  @Parameter(names = "--policy", description =
      "Scheduling policy to apply, either random[:true], constant:{PRIORITY_VALUE}[:true], or metric:{METRIC_NAME}[:true] or none. "
          + "The optional true argument controls scheduling of helper threads", converter = SinglePriorityPolicyConverter.class, required = true)
  SinglePriorityPolicy policy = new NoopSinglePriorityPolicy();

  @Parameter(names = "--translator")
  String translator = NiceSinglePriorityTranslator.NAME;

  @Parameter(names = "--cgroupPolicy", converter = CGroupPolicyConverter.class)
  CGroupPolicy cgroupPolicy = new NoopCGroupPolicy();

  @Parameter(names = "--cgroupTranslator")
  String cGroupTranslator = CpuSharesCGroupTranslator.NAME;

  @Parameter(names = "--maxPriority", description = "Maximum translated priority value")
  Integer maxPriority;

  @Parameter(names = "--minPriority", description = "Minimum translated priority value")
  Integer minPriority;

  @Parameter(names = "--maxCGPriority", description = "Maximum translated priority value")
  Integer maxCGPriority;

  @Parameter(names = "--minCGPriority", description = "Minimum translated priority value")
  Integer minCGPriority;

  @Parameter(names = "--logarithmic", description = "Take the logarithm of the priorities before converting to OS priorities (only for cgroups)!")
  boolean logarithmic = false;

  @Parameter(names = "--statisticsFolder", description = "Path to store the scheduler statistics")
  String statisticsFolder = ".";

  @Parameter(names = "--statisticsHost", description = "Path to store the scheduler statistics", required = true)
  String statisticsHost;

  @Parameter(names = "--ncores", description = "Maximum #cores to use (for quota translator)")
  int ncores = 4;

  @Parameter(names = "--cfsPeriod", description = "CFS Period in us (for quota translator)")
  long cfsPeriod = 1000000;

  @Parameter(names = "--help", help = true)
  boolean help = false;

  @Parameter(names = "--worker", description = "Part of the command of the worker thread (e.g., class name). Argument can be repeated for multiple worker patterns.", required = true)
  List<String> workerPatterns = new ArrayList<>();

  @Parameter(names = "--extraMetric", description = "Metrics that are not necessarily used for scheduling but are reported to graphite (e.g., for debugging)")
  List<BasicSchedulerMetric> extraMetric = new ArrayList<>();

  private long lastCgroupPolicyRun;
  private long lastPolicyRun;
  private long sleepTime = -1;

  /**
   * Create a new {@link ExecutionController} from user-provided CLI arguments, retrieving the PIDs
   * of the running SPE workers and initializing the {@link SchedulerContext}.
   *
   * @param args      The CLI arguments
   * @param mainClass The class that contains the main function of the program. Necessary to omit
   *                  current program from automated PID retrieval
   * @return A new instance of this class
   * @throws InterruptedException In case there is an interrupt during PID retrieval.
   */
  public static ExecutionController init(String[] args, Class<?> mainClass)
      throws InterruptedException {
    ExecutionController controller = newInstanceFromArgs(args);
    Configurator.setRootLevel(controller.log);

    for (String workerPattern : controller.workerPatterns) {
      controller.retrievePids(workerPattern, mainClass);
    }
    LOG.info("Policy: {}", controller.policy.getClass().getSimpleName());
    LOG.info("CGroup Policy: {}", controller.cgroupPolicy.getClass().getSimpleName());
    initSchedulerContext(controller);
    return controller;
  }

  private static void initSchedulerContext(ExecutionController controller) {
    SchedulerContext.initSpeProcessInfo(controller.pids.get(0));
    SchedulerContext.switchToSpeProcessContext();
    SchedulerContext.METRIC_RECENT_PERIOD_SECONDS = controller.window;
    SchedulerContext.STATISTICS_FOLDER = controller.statisticsFolder;
    SchedulerContext.IS_DISTRIBUTED = !Strings.isBlank(controller.distributed);
    SchedulerContext.GRAPHITE_STATS_HOST = controller.statisticsHost;
  }

  private static ExecutionController newInstanceFromArgs(String[] args) {
    ExecutionController config = new ExecutionController();
    JCommander jCommander = JCommander.newBuilder().addObject(config).build();
    jCommander.parse(args);
    if (config.help) {
      jCommander.usage();
      System.exit(0);
    }
    return config;
  }

  private void retrievePids(String workerPattern, Class<?> mainClass) throws InterruptedException {
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

  /**
   * Try to load the update the state of the SPE, retrying for {@link #MAX_RETRIES} to handle cases
   * the query is still being deployed. Waits for {@link #RETRY_INTERVAL_MILLIS} between retries.
   *
   * @param adapter The {@link SpeAdapter} whose tasks need to be loaded.
   * @throws InterruptedException In case there is an interrupt during retrying.
   */
  public static void tryUpdateTasks(SpeAdapter adapter) throws InterruptedException {
    int tries = 0;
    LOG.info("Trying to fetch tasks...");
    while (true) {
      try {
        adapter.updateState();
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

  /**
   * Wait until it is time to run the next single-priority or cgroup schedule.
   *
   * @throws InterruptedException In case there is an interrupt while sleeping.
   */
  public void sleep() throws InterruptedException {
    if (sleepTime < 0) {
      // Initialize sleepTime lazily
      sleepTime = ArithmeticUtils.gcd(period, cgroupPeriod);
    }
    Thread.sleep(TimeUnit.SECONDS.toMillis(sleepTime));
  }


  /**
   * Schedule multiple SPEs at the same time.
   *
   * @param adapters         The {@link SpeAdapter}s, one for each SPE.
   * @param metricProviders  The {@link io.palyvos.scheduler.metric.MetricProvider}s, one for each
   *                         SPE.
   * @param translator       The translator used to apply the {@link SinglePriorityPolicy}.
   * @param cGroupTranslator The translator used to apply the {@link CGroupPolicy}.
   */
  public void scheduleMulti(List<SpeAdapter> adapters,
      List<SchedulerMetricProvider> metricProviders, SinglePriorityTranslator translator,
      CGroupTranslator cGroupTranslator) {
    final long now = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    boolean timeToRunPolicy = isTimeToRunPolicy(now);
    boolean timeToRunCGroupPolicy = isTimeToRunCGroupPolicy(now);
    if (timeToRunPolicy || timeToRunCGroupPolicy) {
      metricProviders.forEach(metricProvider -> metricProvider.run());
      reportExtraMetrics();
    }
    if (timeToRunPolicy) {
      for (int i = 0; i < adapters.size(); i++) {
        SpeAdapter adapter = adapters.get(i);
        SchedulerMetricProvider metricProvider = metricProviders.get(i);
        policy.apply(adapter.taskIndex().tasks(), adapter.runtimeInfo(), translator,
            metricProvider);
      }
      onPolicyExecuted(now);
    }
    if (timeToRunCGroupPolicy) {
      for (int i = 0; i < adapters.size(); i++) {
        SpeAdapter adapter = adapters.get(i);
        SchedulerMetricProvider metricProvider = metricProviders.get(i);
        cgroupPolicy.apply(adapter.taskIndex().tasks(), adapter.runtimeInfo(), cGroupTranslator,
            metricProvider);
      }
      onCGroupPolicyExecuted(now);
    }
  }

  /**
   * Initialize extra metrics that are computed and reported to graphite, but not used for
   * scheduling.
   *
   * @param metricProvider The provider used to retrieve the metrics.
   */
  public void initExtraMetrics(SchedulerMetricProvider metricProvider) {
    if (!extraMetric.isEmpty()) {
      LOG.info("Reporting extra metrics: {}", extraMetric);
      extraMetricReporters = MetricGraphiteReporter
          .reportersFor(SchedulerContext.GRAPHITE_STATS_HOST, SchedulerContext.GRAPHITE_STATS_PORT,
              metricProvider, extraMetric.toArray(new BasicSchedulerMetric[0]));
    }
  }

  private void reportExtraMetrics() {
    if (extraMetricReporters != null) {
      extraMetricReporters.stream().forEach(reporter -> reporter.report());
    }
  }

  /**
   * Schedule one SPE
   *
   * @param adapter          The {@link SpeAdapter} for the SPE.
   * @param metricProvider   The {@link io.palyvos.scheduler.metric.MetricProvider} handling the
   *                         metrics.
   * @param translator       The translator responsible for the {@link SinglePriorityPolicy}.
   * @param cGroupTranslator The translator responsible for the {@link CGroupPolicy}.
   */
  public void schedule(SpeAdapter adapter,
      SchedulerMetricProvider metricProvider, SinglePriorityTranslator translator,
      CGroupTranslator cGroupTranslator) {
    final long now = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    boolean timeToRunPolicy = isTimeToRunPolicy(now);
    boolean timeToRunCGroupPolicy = isTimeToRunCGroupPolicy(now);
    if (timeToRunPolicy || timeToRunCGroupPolicy) {
      metricProvider.run();
      reportExtraMetrics();
    }
    if (timeToRunPolicy) {
      policy.apply(adapter.taskIndex().tasks(), adapter.runtimeInfo(), translator, metricProvider);
      onPolicyExecuted(now);
    }
    if (timeToRunCGroupPolicy) {
      cgroupPolicy.apply(adapter.taskIndex().tasks(), adapter.runtimeInfo(), cGroupTranslator,
          metricProvider);
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

  private boolean isTimeToRunCGroupPolicy(long now) {
    return lastCgroupPolicyRun + cgroupPeriod < now;
  }

  private boolean isTimeToRunPolicy(long now) {
    return lastPolicyRun + period < now;
  }

  /**
   * Maximum number of times scheduling is allowed to fail, based on the allowed {@link
   * #MAX_SCHEDULE_RETRY_TIME} and the scheduling {@link #period}.
   *
   * @return The maximum number of retries.
   */
  public long maxRetries() {
    return MAX_SCHEDULE_RETRY_TIME / period;
  }

  /**
   * Create the appropriate {@link DecisionNormalizer} for the chosen {@link SinglePriorityPolicy}.
   *
   * @param realTime {@code true} if the policy uses real-time threads
   * @return A new instance of the appropriate normalizer for the policy.
   */
  public DecisionNormalizer newSinglePriorityNormalizer(boolean realTime) {
    if ((policy instanceof ConstantSinglePriorityPolicy)
        || (policy instanceof NoopSinglePriorityPolicy)) {
      Validate
          .isTrue(minPriority == null && maxPriority == null, "Cannot define priority range for %s",
              policy.getClass().getSimpleName());
      LOG.info("Using {}", IdentityDecisionNormalizer.class.getSimpleName());
      return new IdentityDecisionNormalizer();
    } else {
      Validate.isTrue(minPriority != null && maxPriority != null,
          "Single-priority translators require min and max priorities!");
      if (realTime || (policy instanceof RandomSinglePriorityPolicy)) {
        LOG.info("Forcing enabled");
        LOG.info("Using {} [{}, {}]", MinMaxDecisionNormalizer.class.getSimpleName(),
            minPriority, maxPriority);
        return new MinMaxDecisionNormalizer(maxPriority, minPriority, true);
      }
      LOG.info("Forcing disabled");
      LOG.info("Using {} [{}, {}]", NiceDecisionNormalizer.class.getSimpleName(),
          minPriority, maxPriority);
      return new NiceDecisionNormalizer(minPriority, maxPriority);
    }
  }

  /**
   * Create a new translator for the chosen {@link SinglePriorityPolicy}, based on the user
   * preferences passed as CLI args.
   *
   * @return A new instance of the translator.
   */
  public SinglePriorityTranslator newSinglePriorityTranslator() {
    LOG.info("Creating single-priority translator");
    String translatorName = translator.trim().toUpperCase();
    if (NiceSinglePriorityTranslator.NAME.equals(translatorName)) {
      LOG.info("Using nice translator");
      return new NiceSinglePriorityTranslator(newSinglePriorityNormalizer(false));
    } else if (RealTimeSinglePriorityTranslator.NAME.equals(translatorName)) {
      LOG.info("Using real-time translator (RR)");
      return new RealTimeSinglePriorityTranslator(newSinglePriorityNormalizer(true),
          RealTimeSchedulingAlgorithm.ROUND_ROBIN);
    }
    throw new IllegalArgumentException(
        String.format("Unknown single-priority translator requested: %s", translator));
  }

  /**
   * Create a new {@link DecisionNormalizer} for the chosen {@link CGroupPolicy}, possibly wrapped
   * with a {@link LogDecisionNormalizer} in case the user has requested logarithmic scaling. If
   * either of the priorit arguments is {@code null}, return an {@link IdentityDecisionNormalizer}
   * instead.
   *
   * @param minPrio The minimum priority of the normalizer.
   * @param maxPrio The maximum priority of the normalizer.
   * @return A new instance of the normalizer.
   */
  public DecisionNormalizer newCGroupNormalizer(Integer minPrio, Integer maxPrio) {
    DecisionNormalizer normalizer;
    if (minPrio != null && maxPrio != null) {
      normalizer = new MinMaxDecisionNormalizer(minPrio, maxPrio, false);
      LOG.info("Using {} [{}, {}]", normalizer.getClass().getSimpleName(), minPrio,
          maxPrio);
    } else {
      normalizer = new IdentityDecisionNormalizer();
      LOG.info("Using {}", normalizer.getClass().getSimpleName());
    }
    if (logarithmic) {
      LOG.info("Using logarithmic scaling");
      return new LogDecisionNormalizer(normalizer);
    }
    return normalizer;
  }

  /**
   * Create a new translator for the {@link CGroupPolicy} based on the user preferences defined in
   * the CLI arguments.
   *
   * @return A new instance of the desired translator.
   */
  public CGroupTranslator newCGroupTranslator() {
    LOG.info("Creating cgroup translator");
    String translatorName = cGroupTranslator.trim().toUpperCase();
    if (CpuQuotaCGroupTranslator.NAME.equals(translatorName)) {
      LOG.info("Using {}", CpuQuotaCGroupTranslator.class.getSimpleName());
      return new CpuQuotaCGroupTranslator(ncores, cfsPeriod,
          newCGroupNormalizer(minCGPriority, maxCGPriority));
    }
    if (CpuSharesCGroupTranslator.NAME.equals(translatorName)) {
      LOG.info("Using {}", CpuSharesCGroupTranslator.class.getSimpleName());
      return new CpuSharesCGroupTranslator(newCGroupNormalizer(minCGPriority, maxCGPriority));
    }
    throw new IllegalArgumentException(
        String.format("Unknown cgroup translator requested: %s", cGroupTranslator));
  }

}
