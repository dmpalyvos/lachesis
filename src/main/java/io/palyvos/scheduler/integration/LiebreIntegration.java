package io.palyvos.scheduler.integration;

import io.palyvos.scheduler.adapters.liebre.LiebreAdapter;
import io.palyvos.scheduler.adapters.liebre.LiebreMetricProvider;
import io.palyvos.scheduler.adapters.linux.LinuxMetricProvider;
import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.policy.cgroup.CGroupTranslator;
import io.palyvos.scheduler.policy.single_priority.SinglePriorityTranslator;
import io.palyvos.scheduler.util.SchedulerContext;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LiebreIntegration {

  private static final Logger LOG = LogManager.getLogger(LiebreIntegration.class);

  public static void main(String[] args) throws InterruptedException {
    ExecutionController config = ExecutionController.init(args, LiebreIntegration.class);
    SchedulerContext.THREAD_NAME_GRAPHITE_CONVERTER = LiebreAdapter.THREAD_NAME_GRAPHITE_CONVERTER;

    Validate.isTrue(config.queryGraphPath.size() == 1, "Only one query graph allowed!");
    Validate.validState(config.pids.size() == 1, "Only one Liebre instance supported!");
    LiebreAdapter adapter = initAdapter(config, config.pids.get(0), config.queryGraphPath.get(0));
    SchedulerMetricProvider metricProvider = initMetricProvider(config, adapter);
    SinglePriorityTranslator translator = config.newSinglePriorityTranslator();
    CGroupTranslator cGroupTranslator = config.newCGroupTranslator();

    int retries = 0;
    config.initExtraMetrics(metricProvider);
    config.policy.init(translator, metricProvider);
    config.cgroupPolicy.init(adapter.taskIndex().tasks(), adapter.runtimeInfo(), cGroupTranslator,
        metricProvider
    );
    while (true) {
      long start = System.currentTimeMillis();
      try {
        config.schedule(adapter, metricProvider, translator, cGroupTranslator);
        retries = 0;
      } catch (Exception e) {
        if (retries++ > config.maxRetries()) {
          throw e;
        }
      }
      LOG.debug("Scheduling took {} ms", System.currentTimeMillis() - start);
      config.sleep();
    }
  }

  static LiebreAdapter initAdapter(ExecutionController config, int pid, String queryGraphPath)
      throws InterruptedException {
    LiebreAdapter adapter = new LiebreAdapter(pid, queryGraphPath);
    config.tryUpdateTasks(adapter);
    return adapter;
  }

  static SchedulerMetricProvider initMetricProvider(ExecutionController config,
      LiebreAdapter adapter) {
    SchedulerMetricProvider metricProvider = new SchedulerMetricProvider(
        new LinuxMetricProvider(config.pids.get(0)),
        new LiebreMetricProvider(config.statisticsHost, ExecutionController.GRAPHITE_RECEIVE_PORT,
            adapter.taskIndex().tasks()));
    metricProvider.setTaskIndex(adapter.taskIndex());
    return metricProvider;
  }

}
