package io.palyvos.scheduler.integration;

import io.palyvos.scheduler.adapters.flink.FlinkAdapter;
import io.palyvos.scheduler.adapters.flink.FlinkGraphiteMetricProvider;
import io.palyvos.scheduler.adapters.linux.LinuxAdapter;
import io.palyvos.scheduler.adapters.linux.LinuxMetricProvider;
import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.policy.cgroup.CGroupTranslator;
import io.palyvos.scheduler.policy.single_priority.SinglePriorityTranslator;
import io.palyvos.scheduler.util.SchedulerContext;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;

public class FlinkIntegration {

  private static final Logger LOG = LogManager.getLogger();

  public static void main(String[] args) throws InterruptedException {

    ExecutionConfig config = ExecutionConfig.init(args, FlinkIntegration.class);
    SchedulerContext.THREAD_NAME_GRAPHITE_CONVERTER = FlinkAdapter.THREAD_NAME_GRAPHITE_CONVERTER;
    SchedulerContext.GRAPHITE_STATS_HOST = config.statisticsHost;

    FlinkAdapter adapter = initAdapter(config, config.pids);
    SchedulerMetricProvider metricProvider = initMetricProvider(config, adapter, config.pids);
    SinglePriorityTranslator translator = config.newNiceTranslator();
    CGroupTranslator cGroupTranslator = config.newCGroupTranslator();

    int retries = 0;
    config.policy.init(translator, metricProvider);
    config.cgroupPolicy.init(adapter.taskIndex().tasks(), adapter.runtimeInfo(), cGroupTranslator, metricProvider
    );
    while (true) {
      long start = System.currentTimeMillis();
      try {
        config.schedule(adapter, metricProvider, translator, cGroupTranslator);
      } catch (Exception e) {
        if (retries++ > config.maxRetries()) {
          throw e;
        }
      }
      LOG.debug("Scheduling took {} ms", System.currentTimeMillis() - start);
      config.sleep();
    }
  }

  static FlinkAdapter initAdapter(ExecutionConfig config, List<Integer> pids)
      throws InterruptedException {
    String leader = Strings.isBlank(config.distributed) ? "localhost" : config.distributed;
    FlinkAdapter adapter = new FlinkAdapter(pids, leader,
        FlinkAdapter.DEFAULT_FLINK_PORT, new LinuxAdapter());
    config.tryUpdateTasks(adapter);
    return adapter;
  }

  static SchedulerMetricProvider initMetricProvider(ExecutionConfig config,
      FlinkAdapter adapter, List<Integer> pids) {
    SchedulerMetricProvider metricProvider = new SchedulerMetricProvider(
        new FlinkGraphiteMetricProvider(config.statisticsHost,
            ExecutionConfig.GRAPHITE_RECEIVE_PORT, adapter.taskIndex().tasks()),
        new LinuxMetricProvider(pids));
    metricProvider.setTaskIndex(adapter.taskIndex());
    return metricProvider;
  }


}
