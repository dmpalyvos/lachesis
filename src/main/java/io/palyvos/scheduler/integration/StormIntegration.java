package io.palyvos.scheduler.integration;

import io.palyvos.scheduler.adapters.linux.LinuxAdapter;
import io.palyvos.scheduler.adapters.linux.LinuxMetricProvider;
import io.palyvos.scheduler.adapters.storm.StormAdapter;
import io.palyvos.scheduler.adapters.storm.StormGraphiteMetricProvider;
import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.policy.cgroup.CGroupTranslator;
import io.palyvos.scheduler.policy.single_priority.SinglePriorityTranslator;
import io.palyvos.scheduler.util.SchedulerContext;
import java.util.List;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class StormIntegration {

  private static final Logger LOG = LogManager.getLogger();

  public static void main(String[] args) throws InterruptedException {

    ExecutionController controller = ExecutionController.init(args, StormIntegration.class);
    SchedulerContext.THREAD_NAME_GRAPHITE_CONVERTER = StormAdapter.THREAD_NAME_GRAPHITE_CONVERTER;

    Validate.isTrue(controller.queryGraphPath.size() == 1, "Only one query graph allowed!");
    StormAdapter adapter = initAdapter(controller, controller.pids,
        controller.queryGraphPath.get(0));
    SchedulerMetricProvider metricProvider = initMetricProvider(controller, adapter,
        controller.pids);
    SinglePriorityTranslator translator = controller.newSinglePriorityTranslator();
    CGroupTranslator cGroupTranslator = controller.newCGroupTranslator();

    controller.initExtraMetrics(metricProvider);
    controller.policy.init(translator, metricProvider);
    controller.cgroupPolicy
        .init(adapter.taskIndex().tasks(), adapter.runtimeInfo(), cGroupTranslator, metricProvider);
    int retries = 0;
    while (true) {
      long start = System.currentTimeMillis();
      try {
        controller.schedule(adapter, metricProvider, translator, cGroupTranslator);
        retries = 0;
      } catch (Exception e) {
        if (retries++ > controller.maxRetries()) {
          throw e;
        }
      }
      LOG.debug("Scheduling took {} ms", System.currentTimeMillis() - start);
      controller.sleep();
    }
  }


  static StormAdapter initAdapter(ExecutionController controller, List<Integer> pids,
      String queryGraphPath)
      throws InterruptedException {
    StormAdapter adapter = new StormAdapter(pids, new LinuxAdapter(), queryGraphPath);
    controller.tryUpdateTasks(adapter);
    return adapter;
  }

  static SchedulerMetricProvider initMetricProvider(ExecutionController controller,
      StormAdapter adapter, List<Integer> pids) {
    SchedulerMetricProvider metricProvider = new SchedulerMetricProvider(
        new StormGraphiteMetricProvider(controller.statisticsHost,
            ExecutionController.GRAPHITE_RECEIVE_PORT), new LinuxMetricProvider(pids));
    metricProvider.setTaskIndex(adapter.taskIndex());
    return metricProvider;
  }


}
