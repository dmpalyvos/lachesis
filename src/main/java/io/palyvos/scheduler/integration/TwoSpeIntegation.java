package io.palyvos.scheduler.integration;

import io.palyvos.scheduler.adapters.flink.FlinkAdapter;
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

public class TwoSpeIntegation {

  private static final Logger LOG = LogManager.getLogger();

  public static void main(String[] args) throws InterruptedException {

    LOG.info("Usage: --worker <storm_worker> --worker <flink_worker>");
    Thread.sleep(2000);

    ExecutionController controller = ExecutionController.init(args, TwoSpeIntegation.class);

    Validate.isTrue(controller.queryGraphPath.size() == 1,
        "Only one query graph allowed (storm)!");
    Validate.validState(controller.pids.size() == 2,
        "Expected 2 pids but got %d", controller.pids.size());
    Validate.isTrue((controller.cgroupPolicy instanceof OneCGroupPolicy)
            || (controller.cgroupPolicy instanceof SpeCGroupPolicy),
        "OneCGroupPolicy|SpeCGroupPolicy is hardcoded for this experiment, please define it in the config");

    final List<Integer> stormPids = controller.pids.subList(0, 1);
    final List<Integer> flinkPids = controller.pids.subList(1, 2);

    StormAdapter stormAdapter = StormIntegration
        .initAdapter(controller, stormPids, controller.queryGraphPath.get(0));
    SchedulerMetricProvider stormMetricProvider = StormIntegration
        .initMetricProvider(controller, stormAdapter, stormPids);

    FlinkAdapter flinkAdapter = FlinkIntegration.initAdapter(controller, flinkPids);
    SchedulerMetricProvider flinkMetricProvider = FlinkIntegration
        .initMetricProvider(controller, flinkAdapter, flinkPids);

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
    controller.policy.init(translator, flinkMetricProvider);
    controller.policy.init(translator, stormMetricProvider);
    int retries = 0;
    while (true) {
      long start = System.currentTimeMillis();
      try {
        controller.scheduleMulti(Arrays.asList(flinkAdapter, stormAdapter),
            Arrays.asList(flinkMetricProvider, stormMetricProvider), translator, cGroupTranslator);
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
