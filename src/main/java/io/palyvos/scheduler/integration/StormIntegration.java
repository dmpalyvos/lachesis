package io.palyvos.scheduler.integration;

import io.palyvos.scheduler.adapters.linux.LinuxAdapter;
import io.palyvos.scheduler.adapters.linux.LinuxMetricProvider;
import io.palyvos.scheduler.adapters.storm.StormAdapter;
import io.palyvos.scheduler.adapters.storm.StormGraphiteMetricProvider;
import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.policy.single_priority.SinglePriorityMetricTranslator;
import io.palyvos.scheduler.policy.single_priority.NiceSinglePriorityMetricTranslator;
import io.palyvos.scheduler.policy.normalizers.DecisionNormalizer;
import io.palyvos.scheduler.policy.normalizers.LogDecisionNormalizer;
import io.palyvos.scheduler.policy.normalizers.MinMaxDecisionNormalizer;
import io.palyvos.scheduler.util.SchedulerContext;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class StormIntegration {

  private static final Logger LOG = LogManager.getLogger();

  public static void main(String[] args) throws InterruptedException {

    ExecutionConfig config = ExecutionConfig.init(args, StormIntegration.class);
    SchedulerContext.THREAD_NAME_GRAPHITE_CONVERTER = StormAdapter.THREAD_NAME_GRAPHITE_CONVERTER;
    SchedulerContext.GRAPHITE_STATS_HOST = config.statisticsHost;

    StormAdapter adapter = new StormAdapter(config.pids, new LinuxAdapter(), config.queryGraphPath);
    config.tryUpdateTasks(adapter);
    SchedulerMetricProvider metricProvider = new SchedulerMetricProvider(
        new StormGraphiteMetricProvider(config.statisticsHost, 80),
        new LinuxMetricProvider(config.pids));
    DecisionNormalizer normalizer = new MinMaxDecisionNormalizer(config.minPriority, config.maxPriority);
    if (config.logarithmic) {
      normalizer = new LogDecisionNormalizer(normalizer);
    }
    SinglePriorityMetricTranslator translator = new NiceSinglePriorityMetricTranslator(normalizer);
    metricProvider.setTaskIndex(adapter.taskIndex());

    config.policy.init(translator, metricProvider);
    int retries = 0;
    while (true) {
      try {
        long start = System.currentTimeMillis();
        metricProvider.run();
        config.policy.apply(adapter.taskIndex().tasks(), translator, metricProvider);
        LOG.debug("Scheduling took {} ms", System.currentTimeMillis() - start);
      }
      catch (Exception e) {
        LOG.error("Failed to schedule: {}", e.getMessage());
        Thread.sleep(5000);
        if (retries++ > ExecutionConfig.MAX_SCHEDULE_RETRIES) {
          throw e;
        }
      }
      Thread.sleep(TimeUnit.SECONDS.toMillis(config.period));
    }
  }



}
