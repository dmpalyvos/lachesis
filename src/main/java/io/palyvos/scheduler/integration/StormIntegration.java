package io.palyvos.scheduler.integration;

import io.palyvos.scheduler.adapters.linux.LinuxAdapter;
import io.palyvos.scheduler.adapters.linux.LinuxMetricProvider;
import io.palyvos.scheduler.adapters.storm.StormAdapter;
import io.palyvos.scheduler.adapters.storm.StormGraphiteMetricProvider;
import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.policy.single_priority.SinglePriorityMetricTranslator;
import io.palyvos.scheduler.util.SchedulerContext;
import java.util.List;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class StormIntegration {

  private static final Logger LOG = LogManager.getLogger();

  public static void main(String[] args) throws InterruptedException {

    ExecutionConfig config = ExecutionConfig.init(args, StormIntegration.class);
    SchedulerContext.THREAD_NAME_GRAPHITE_CONVERTER = StormAdapter.THREAD_NAME_GRAPHITE_CONVERTER;
    SchedulerContext.GRAPHITE_STATS_HOST = config.statisticsHost;

    Validate.isTrue(config.queryGraphPath.size() == 1, "Only one query graph allowed!");
    StormAdapter adapter = initAdapter(config, config.pids, config.queryGraphPath.get(0));
    SchedulerMetricProvider metricProvider = initMetricProvider(config, adapter, config.pids);
    SinglePriorityMetricTranslator translator = config.newSinglePriorityTranslator();

    config.policy.init(translator, metricProvider);
    config.cgroupPolicy.init(adapter.tasks(), config.cGroupTranslator, metricProvider);
    int retries = 0;
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

  static StormAdapter initAdapter(ExecutionConfig config, List<Integer> pids, String queryGraphPath)
      throws InterruptedException {
    StormAdapter adapter = new StormAdapter(pids, new LinuxAdapter(),
        queryGraphPath);
    config.tryUpdateTasks(adapter);
    return adapter;
  }

  static SchedulerMetricProvider initMetricProvider(ExecutionConfig config,
      StormAdapter adapter, List<Integer> pids) {
    SchedulerMetricProvider metricProvider = new SchedulerMetricProvider(
        new StormGraphiteMetricProvider(config.statisticsHost,
            ExecutionConfig.GRAPHITE_RECEIVE_PORT),
        new LinuxMetricProvider(pids));
    metricProvider.setTaskIndex(adapter.taskIndex());
    return metricProvider;
  }


}
