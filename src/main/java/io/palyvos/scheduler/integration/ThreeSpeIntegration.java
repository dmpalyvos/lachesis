package io.palyvos.scheduler.integration;

import io.palyvos.scheduler.adapters.flink.FlinkAdapter;
import io.palyvos.scheduler.adapters.liebre.LiebreAdapter;
import io.palyvos.scheduler.adapters.storm.StormAdapter;
import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.policy.cgroup.CGroupTranslator;
import io.palyvos.scheduler.policy.cgroup.OneCGroupPolicy;
import io.palyvos.scheduler.policy.cgroup.SpeCGroupPolicy;
import io.palyvos.scheduler.policy.single_priority.SinglePriorityTranslator;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ThreeSpeIntegration {

  private static final Logger LOG = LogManager.getLogger();

  public static void main(String[] args) throws InterruptedException {

    LOG.info("Usage: --worker <storm_worker> --worker <flink_worker> --worker <liebre_worker>");
    Thread.sleep(2000);

    ExecutionController controller = ExecutionController.init(args, ThreeSpeIntegration.class);

    Validate.isTrue(controller.queryGraphPath.size() == 2,
        "2 query graph paths expected: storm, liebre (in that order)");
    Validate.validState(controller.pids.size() == 3,
        "Expected 3 pids (storm, flink, liebre) but got %d", controller.pids.size());
    Validate.isTrue((controller.cgroupPolicy instanceof OneCGroupPolicy)
            || (controller.cgroupPolicy instanceof SpeCGroupPolicy),
        "OneCGroupPolicy|SpeCGroupPolicy is hardcoded for this experiment, please define it in the config");

    final List<Integer> stormPids = controller.pids.subList(0, 1);
    final List<Integer> flinkPids = controller.pids.subList(1, 2);
    final List<Integer> liebrePids = controller.pids.subList(2, 3);

    StormAdapter stormAdapter = StormIntegration
        .initAdapter(controller, stormPids, controller.queryGraphPath.get(0));
    SchedulerMetricProvider stormMetricProvider = StormIntegration
        .initMetricProvider(controller, stormAdapter, stormPids);

    FlinkAdapter flinkAdapter = FlinkIntegration.initAdapter(controller, flinkPids);
    SchedulerMetricProvider flinkMetricProvider = FlinkIntegration
        .initMetricProvider(controller, flinkAdapter, flinkPids);

    LiebreAdapter liebreAdapter = LiebreIntegration
        .initAdapter(controller, liebrePids.get(0), controller.queryGraphPath.get(1));
    SchedulerMetricProvider liebreMetricProvider = LiebreIntegration
        .initMetricProvider(controller, liebreAdapter);

    SinglePriorityTranslator translator = controller.newSinglePriorityTranslator();
    CGroupTranslator cGroupTranslator = controller.newCGroupTranslator();

    // Apply cgroup for both SPEs
    controller.cgroupPolicy.init(null, null, cGroupTranslator, null);
    if (controller.cgroupPolicy instanceof OneCGroupPolicy) {
      // Reconfigure policy to prevent repeated calls
      controller.cgroupPolicy = new OneCGroupPolicy("one", 2);
    }

    controller.initExtraMetrics(flinkMetricProvider);
    controller.initExtraMetrics(stormMetricProvider);
    controller.initExtraMetrics(liebreMetricProvider);
    controller.policy.init(translator, flinkMetricProvider);
    controller.policy.init(translator, stormMetricProvider);
    controller.policy.init(translator, liebreMetricProvider);
    int retries = 0;
    // Wait a bit until the queries have really started processing data
    Thread.sleep(30000);
    while (true) {
      long start = System.currentTimeMillis();
      try {
        controller.scheduleMulti(Arrays.asList(flinkAdapter, stormAdapter, liebreAdapter),
            Arrays.asList(flinkMetricProvider, stormMetricProvider, liebreMetricProvider),
            translator, cGroupTranslator);
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

}
