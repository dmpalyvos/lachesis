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

    ExecutionController controller = ExecutionController.init(args, FlinkIntegration.class);
    SchedulerContext.THREAD_NAME_GRAPHITE_CONVERTER = FlinkAdapter.THREAD_NAME_GRAPHITE_CONVERTER;

    FlinkAdapter adapter = initAdapter(controller, controller.pids);
    SchedulerMetricProvider metricProvider = initMetricProvider(controller, adapter,
        controller.pids);
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

  static FlinkAdapter initAdapter(ExecutionController controller, List<Integer> pids)
      throws InterruptedException {
    String leader = Strings.isBlank(controller.distributed) ? "localhost" : controller.distributed;
    FlinkAdapter adapter = new FlinkAdapter(pids, leader,
        FlinkAdapter.DEFAULT_FLINK_PORT, new LinuxAdapter());
    controller.tryUpdateTasks(adapter);
    return adapter;
  }

  static SchedulerMetricProvider initMetricProvider(ExecutionController controller,
      FlinkAdapter adapter, List<Integer> pids) {
    SchedulerMetricProvider metricProvider = new SchedulerMetricProvider(
        new FlinkGraphiteMetricProvider(controller.statisticsHost,
            ExecutionController.GRAPHITE_RECEIVE_PORT, adapter.taskIndex().tasks()),
        new LinuxMetricProvider(pids));
    metricProvider.setTaskIndex(adapter.taskIndex());
    return metricProvider;
  }


}
