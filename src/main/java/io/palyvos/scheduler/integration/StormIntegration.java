package io.palyvos.scheduler.integration;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import io.palyvos.scheduler.adapters.linux.LinuxAdapter;
import io.palyvos.scheduler.adapters.linux.LinuxMetricProvider;
import io.palyvos.scheduler.adapters.storm.StormAdapter;
import io.palyvos.scheduler.adapters.storm.StormGraphiteMetricProvider;
import io.palyvos.scheduler.metric.BasicSchedulerMetric;
import io.palyvos.scheduler.metric.SchedulerMetric;
import io.palyvos.scheduler.metric.MetricFileReporter;
import io.palyvos.scheduler.metric.MetricProvider;
import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.policy.translators.concrete.ConcretePolicyTranslator;
import io.palyvos.scheduler.policy.translators.concrete.NicePolicyTranslator;
import io.palyvos.scheduler.policy.translators.concrete.normalizers.DecisionNormalizer;
import io.palyvos.scheduler.policy.translators.concrete.normalizers.LogDecisionNormalizer;
import io.palyvos.scheduler.policy.translators.concrete.normalizers.MinMaxDecisionNormalizer;
import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.task.Subtask;
import io.palyvos.scheduler.util.JcmdCommand;
import io.palyvos.scheduler.util.Log4jLevelConverter;
import io.palyvos.scheduler.util.SchedulerContext;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

public class StormIntegration {

  private static final Logger LOG = LogManager.getLogger();
  private static long counter = 0;

  public static void main(String[] args) throws InterruptedException {

    Config config = new Config();
    JCommander jCommander = JCommander.newBuilder().addObject(config).build();
    jCommander.parse(args);
    if (config.help) {
      jCommander.usage();
      return;
    }
    Configurator.setRootLevel(config.log);
    retrievePid(config);

    SchedulerContext.initSpeProcessInfo(config.pid);
    SchedulerContext.switchToSpeProcessContext();
    SchedulerContext.METRIC_RECENT_PERIOD_SECONDS = config.window;
    SchedulerContext.STATISTICS_FOLDER = config.statisticsFolder;

    StormAdapter adapter = new StormAdapter(config.pid, "localhost", 8080, new LinuxAdapter(),
        config.queryGraphPath);
    tryUpdateTasks(adapter);
    SchedulerMetricProvider metricProvider = new SchedulerMetricProvider(
        new StormGraphiteMetricProvider("129.16.20.158", 80),
        new LinuxMetricProvider(config.pid));
    DecisionNormalizer normalizer = new MinMaxDecisionNormalizer(config.minPriority, config.maxPriority);
    if (config.logarithmic) {
      normalizer = new LogDecisionNormalizer(normalizer);
    }
    ConcretePolicyTranslator translator = new NicePolicyTranslator(normalizer);
    metricProvider.setTaskIndex(adapter.taskIndex());

    metricProvider.register(BasicSchedulerMetric.TASK_QUEUE_SIZE_FROM_SUBTASK_DATA);
    metricProvider.register(BasicSchedulerMetric.SUBTASK_GLOBAL_RATE);

    // Registered only for viz purposes, would be auto-registered otherwise
    metricProvider.register(BasicSchedulerMetric.SUBTASK_SELECTIVITY);
    metricProvider.register(BasicSchedulerMetric.SUBTASK_COST);

    final Collection<MetricFileReporter<SchedulerMetric>> reporters = MetricFileReporter
        .reportersFor(metricProvider,
            BasicSchedulerMetric.SUBTASK_TUPLES_IN_RECENT,
            BasicSchedulerMetric.SUBTASK_TUPLES_OUT_RECENT,
            BasicSchedulerMetric.SUBTASK_SELECTIVITY,
            BasicSchedulerMetric.SUBTASK_COST,
            BasicSchedulerMetric.TASK_QUEUE_SIZE_FROM_SUBTASK_DATA,
            BasicSchedulerMetric.SUBTASK_GLOBAL_RATE);

    while (true) {
      long start = System.currentTimeMillis();
      metricProvider.run();
      schedule(metricProvider, config.metric, translator, adapter);
      reporters.forEach(reporter -> reporter.report());
      LOG.info("Scheduling took {} ms", System.currentTimeMillis() - start);
      Thread.sleep(TimeUnit.SECONDS.toMillis(config.period));
    }
  }

  private static void tryUpdateTasks(StormAdapter adapter) throws InterruptedException {
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

  private static void retrievePid(Config config) throws InterruptedException {
    final int tries = 20;
    for (int i = 0; i < tries; i++) {
      try {
        LOG.info("Trying to retrieve storm worker PID...");
        config.pid = new JcmdCommand().pidFor(StormAdapter.STORM_WORKER_CLASS);
        LOG.info("Success!");
        return;
      } catch (Exception exception) {
        Thread.sleep(5000);
      }
    }
    throw new IllegalStateException("Failed to retrieve storm worker PID!");
  }

  private static void schedule(MetricProvider collector, SchedulerMetric metric,
      ConcretePolicyTranslator translator,
      StormAdapter adapter) {
    Map<ExternalThread, Double> schedule = new HashMap<>();
    for (Subtask subtask : adapter.taskIndex().subtasks()) {
      try {
        double value = collector.get(metric, subtask.id());
        schedule.put(subtask.thread(), value);
      } catch (Exception e) {
        LOG.error("Failed to get metric {} for task {}: {}\n", metric, subtask, e.getMessage());
        throw new RuntimeException(e);
      }
    }
    translator.applyPolicy(schedule);
  }

  static class Config {

    private int pid = -1;

    @Parameter(names = "--log", converter = Log4jLevelConverter.class, description = "Logging level (e.g., DEBUG, INFO, etc)")
    private Level log = Level.DEBUG;

    @Parameter(names = "--queryGraph", required = true, description = "Path to the query graph yaml file")
    private String queryGraphPath;

    @Parameter(names = "--period", description = "(Minimum) scheduling period, in seconds")
    private long period = 1;

    @Parameter(names = "--window", description = "Time-window (seconds) to consider for recent metrics")
    private int window = 30;

    @Parameter(names = "--metric", description = "Metric to use for scheduling")
    private BasicSchedulerMetric metric = BasicSchedulerMetric.SUBTASK_GLOBAL_RATE;

    @Parameter(names = "--maxPriority", description = "Maximum translated priority value")
    private int maxPriority = -20;

    @Parameter(names = "--minPriority", description = "Minimum translated priority value")
    private int minPriority = 10;

    @Parameter(names = "--logarithmic", description = "Take the logarithm of the priorities before converting to nice values")
    private boolean logarithmic = false;

    @Parameter(names = "--statisticsFolder", description = "Path to store the scheduler statistics")
    private String statisticsFolder = ".";

    @Parameter(names = "--help", help = true)
    private boolean help = false;

  }
}
