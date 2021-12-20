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
    ExecutionController controller = ExecutionController.init(args, LiebreIntegration.class);
    SchedulerContext.THREAD_NAME_GRAPHITE_CONVERTER = LiebreAdapter.THREAD_NAME_GRAPHITE_CONVERTER;

    Validate.isTrue(controller.queryGraphPath.size() == 1, "Only one query graph allowed!");
    Validate.validState(controller.pids.size() == 1, "Only one Liebre instance supported!");
    LiebreAdapter adapter = initAdapter(controller, controller.pids.get(0),
        controller.queryGraphPath.get(0));
    SchedulerMetricProvider metricProvider = initMetricProvider(controller, adapter);
    SinglePriorityTranslator translator = controller.newSinglePriorityTranslator();
    CGroupTranslator cGroupTranslator = controller.newCGroupTranslator();

    int retries = 0;
    controller.initExtraMetrics(metricProvider);
    controller.policy.init(translator, metricProvider);
    controller.cgroupPolicy.init(adapter.taskIndex().tasks(), adapter.runtimeInfo(),
        cGroupTranslator,
        metricProvider
    );
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

  static LiebreAdapter initAdapter(ExecutionController controller, int pid, String queryGraphPath)
      throws InterruptedException {
    LiebreAdapter adapter = new LiebreAdapter(pid, queryGraphPath);
    controller.tryUpdateTasks(adapter);
    return adapter;
  }

  static SchedulerMetricProvider initMetricProvider(ExecutionController controller,
      LiebreAdapter adapter) {
    SchedulerMetricProvider metricProvider = new SchedulerMetricProvider(
        new LinuxMetricProvider(controller.pids.get(0)),
        new LiebreMetricProvider(controller.statisticsHost,
            ExecutionController.GRAPHITE_RECEIVE_PORT,
            adapter.taskIndex().tasks()));
    metricProvider.setTaskIndex(adapter.taskIndex());
    return metricProvider;
  }

}
