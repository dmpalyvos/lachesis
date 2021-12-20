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

    ExecutionController config = ExecutionController.init(args, ThreeSpeIntegration.class);

    Validate.isTrue(config.queryGraphPath.size() == 2,
        "2 query graph paths expected: storm, liebre (in that order)");
    Validate.validState(config.pids.size() == 3,
        "Expected 3 pids (storm, flink, liebre) but got %d", config.pids.size());
    Validate.isTrue((config.cgroupPolicy instanceof OneCGroupPolicy)
            || (config.cgroupPolicy instanceof SpeCGroupPolicy),
        "OneCGroupPolicy|SpeCGroupPolicy is hardcoded for this experiment, please define it in the config");

    final List<Integer> stormPids = config.pids.subList(0, 1);
    final List<Integer> flinkPids = config.pids.subList(1, 2);
    final List<Integer> liebrePids = config.pids.subList(2, 3);

    StormAdapter stormAdapter = StormIntegration
        .initAdapter(config, stormPids, config.queryGraphPath.get(0));
    SchedulerMetricProvider stormMetricProvider = StormIntegration
        .initMetricProvider(config, stormAdapter, stormPids);

    FlinkAdapter flinkAdapter = FlinkIntegration.initAdapter(config, flinkPids);
    SchedulerMetricProvider flinkMetricProvider = FlinkIntegration
        .initMetricProvider(config, flinkAdapter, flinkPids);

    LiebreAdapter liebreAdapter = LiebreIntegration
        .initAdapter(config, liebrePids.get(0), config.queryGraphPath.get(1));
    SchedulerMetricProvider liebreMetricProvider = LiebreIntegration
        .initMetricProvider(config, liebreAdapter);

    SinglePriorityTranslator translator = config.newSinglePriorityTranslator();
    CGroupTranslator cGroupTranslator = config.newCGroupTranslator();

    // Apply cgroup for both SPEs
    config.cgroupPolicy.init(null, null, cGroupTranslator, null);
    if (config.cgroupPolicy instanceof OneCGroupPolicy) {
      // Reconfigure policy to prevent repeated calls
      config.cgroupPolicy = new OneCGroupPolicy("one", 2);
    }

    config.initExtraMetrics(flinkMetricProvider);
    config.initExtraMetrics(stormMetricProvider);
    config.initExtraMetrics(liebreMetricProvider);
    config.policy.init(translator, flinkMetricProvider);
    config.policy.init(translator, stormMetricProvider);
    config.policy.init(translator, liebreMetricProvider);
    int retries = 0;
    // Wait a bit until the queries have really started processing data
    Thread.sleep(30000);
    while (true) {
      long start = System.currentTimeMillis();
      try {
        config.scheduleMulti(Arrays.asList(flinkAdapter, stormAdapter, liebreAdapter),
            Arrays.asList(flinkMetricProvider, stormMetricProvider, liebreMetricProvider),
            translator, cGroupTranslator);
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

}
