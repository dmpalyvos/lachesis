package io.palyvos.scheduler.integration;

import io.palyvos.scheduler.adapters.linux.LinuxAdapter;
import io.palyvos.scheduler.adapters.linux.LinuxMetricProvider;
import io.palyvos.scheduler.adapters.storm.StormAdapter;
import io.palyvos.scheduler.adapters.storm.StormGraphiteMetricProvider;
import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.policy.normalizers.DecisionNormalizer;
import io.palyvos.scheduler.policy.normalizers.LogDecisionNormalizer;
import io.palyvos.scheduler.policy.normalizers.MinMaxDecisionNormalizer;
import io.palyvos.scheduler.policy.single_priority.NiceSinglePriorityMetricTranslator;
import io.palyvos.scheduler.policy.single_priority.SinglePriorityMetricTranslator;
import io.palyvos.scheduler.util.SchedulerContext;
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
    config.cgroupPolicy.init(adapter.tasks(), metricProvider);
    int retries = 0;
    while (true) {
      long start = System.currentTimeMillis();
      try {
        config.schedule(adapter, metricProvider, translator);
      }
      catch (Exception e) {
        if (retries++ > config.maxRetries()) {
          throw e;
        }
      }
      LOG.debug("Scheduling took {} ms", System.currentTimeMillis() - start);
      config.sleep();
    }
  }



}
