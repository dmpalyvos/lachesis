package io.palyvos.scheduler.integration;

import io.palyvos.scheduler.adapters.flink.FlinkAdapter;
import io.palyvos.scheduler.adapters.flink.FlinkGraphiteMetricProvider;
import io.palyvos.scheduler.adapters.linux.LinuxAdapter;
import io.palyvos.scheduler.adapters.linux.LinuxMetricProvider;
import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.policy.normalizers.DecisionNormalizer;
import io.palyvos.scheduler.policy.normalizers.ExponentialSmoothingDecisionNormalizer;
import io.palyvos.scheduler.policy.normalizers.LogDecisionNormalizer;
import io.palyvos.scheduler.policy.normalizers.MinMaxDecisionNormalizer;
import io.palyvos.scheduler.policy.single_priority.NiceSinglePriorityMetricTranslator;
import io.palyvos.scheduler.policy.single_priority.SinglePriorityMetricTranslator;
import io.palyvos.scheduler.util.SchedulerContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FlinkIntegration {

  private static final Logger LOG = LogManager.getLogger();

  public static void main(String[] args) throws InterruptedException {

    ExecutionConfig config = ExecutionConfig.init(args, FlinkIntegration.class);
    SchedulerContext.THREAD_NAME_GRAPHITE_CONVERTER = FlinkAdapter.THREAD_NAME_GRAPHITE_CONVERTER;
    SchedulerContext.GRAPHITE_STATS_HOST = config.statisticsHost;

    FlinkAdapter adapter = new FlinkAdapter(config.pids, "localhost",
        FlinkAdapter.DEFAULT_FLINK_PORT, new LinuxAdapter());
    config.tryUpdateTasks(adapter);
    SchedulerMetricProvider metricProvider = new SchedulerMetricProvider(
        new FlinkGraphiteMetricProvider(config.statisticsHost,
            ExecutionConfig.GRAPHITE_RECEIVE_PORT, adapter.tasks()),
        new LinuxMetricProvider(config.pids));
    DecisionNormalizer normalizer = new MinMaxDecisionNormalizer(config.minPriority,
        config.maxPriority);
    if (config.logarithmic) {
      normalizer = new LogDecisionNormalizer(normalizer);
    }
    normalizer = new ExponentialSmoothingDecisionNormalizer(normalizer, config.smoothingFactor);
    SinglePriorityMetricTranslator translator = new NiceSinglePriorityMetricTranslator(normalizer);
    metricProvider.setTaskIndex(adapter.taskIndex());

    int retries = 0;
    config.policy.init(translator, metricProvider);
    config.cgroupPolicy.init(adapter.tasks(), metricProvider);
    while (true) {
      long start = System.currentTimeMillis();
      try {
        config.schedule(adapter, metricProvider, translator);
      } catch (Exception e) {
        if (retries++ > config.maxRetries()) {
          throw e;
        }
      }
      LOG.debug("Scheduling took {} ms", System.currentTimeMillis() - start);
      config.sleep();
    }
  }


}
