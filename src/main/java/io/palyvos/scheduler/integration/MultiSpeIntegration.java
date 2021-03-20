package io.palyvos.scheduler.integration;

import io.palyvos.scheduler.adapters.SpeRuntimeInfo;
import io.palyvos.scheduler.adapters.flink.FlinkAdapter;
import io.palyvos.scheduler.adapters.storm.StormAdapter;
import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.policy.cgroup.CGroupPolicy;
import io.palyvos.scheduler.policy.cgroup.CGroupTranslator;
import io.palyvos.scheduler.policy.cgroup.OneCGroupPolicy;
import io.palyvos.scheduler.policy.cgroup.SpeCGroupPolicy;
import io.palyvos.scheduler.policy.single_priority.DelegatingMultiSpeSinglePriorityPolicy;
import io.palyvos.scheduler.policy.single_priority.SinglePriorityTranslator;
import io.palyvos.scheduler.util.SchedulerContext;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MultiSpeIntegration {

  private static final Logger LOG = LogManager.getLogger();

  public static void main(String[] args) throws InterruptedException {

    LOG.info("Usage: --worker <storm_worker> --worker <flink_worker>");
    Thread.sleep(2000);

    ExecutionController config = ExecutionController.init(args, MultiSpeIntegration.class);
    SchedulerContext.GRAPHITE_STATS_HOST = config.statisticsHost;

    Validate.isTrue(config.queryGraphPath.size() == 1,
        "Only one query graph allowed (storm)!");
    Validate.validState(config.pids.size() == 2,
        "Expected 2 pids but got %d", config.pids.size());
    Validate.isTrue((config.cgroupPolicy instanceof OneCGroupPolicy) || (config.cgroupPolicy instanceof SpeCGroupPolicy),
        "OneCGroupPolicy|SpeCGroupPolicy is hardcoded for this experiment, please define it in the config");

    final List<Integer> stormPids = config.pids.subList(0, 1);
    final List<Integer> flinkPids = config.pids.subList(1, 2);

    StormAdapter stormAdapter = StormIntegration
        .initAdapter(config, stormPids, config.queryGraphPath.get(0));
    SchedulerMetricProvider stormMetricProvider = StormIntegration
        .initMetricProvider(config, stormAdapter, stormPids);

    FlinkAdapter flinkAdapter = FlinkIntegration.initAdapter(config, flinkPids);
    SchedulerMetricProvider flinkMetricProvider = FlinkIntegration
        .initMetricProvider(config, flinkAdapter, flinkPids);

    SinglePriorityTranslator translator = config.newSinglePriorityTranslator();
    CGroupTranslator cGroupTranslator = config.newCGroupTranslator();

    DelegatingMultiSpeSinglePriorityPolicy multiPolicy = new DelegatingMultiSpeSinglePriorityPolicy(
        config.policy);

    // Apply cgroup for both SPEs
    config.cgroupPolicy.init(null, null, cGroupTranslator, null);
    if (config.cgroupPolicy instanceof OneCGroupPolicy) {
      // Reconfigure policy to prevent repeated calls
      config.cgroupPolicy = new OneCGroupPolicy("one", 2);
    }


    multiPolicy.init(translator, Arrays.asList(stormMetricProvider, flinkMetricProvider));

    int retries = 0;
    while (true) {
      long start = System.currentTimeMillis();
      try {
        config.scheduleMulti(multiPolicy, Arrays.asList(flinkAdapter, stormAdapter),
            Arrays.asList(flinkMetricProvider, stormMetricProvider), translator,
            cGroupTranslator, Arrays.asList(1.0, 5.0));
      } catch (Exception e) {
        if (retries++ > config.maxRetries()) {
          throw e;
        }
      }
      LOG.debug("Scheduling took {} ms", System.currentTimeMillis() - start);
      config.sleep();
    }
  }

  private static void applyOneCGroupPolicy(StormAdapter stormAdapter, FlinkAdapter flinkAdapter,
      CGroupTranslator cGroupTranslator) {
    CGroupPolicy cgroupPolicy = new OneCGroupPolicy();
    cgroupPolicy.init(null, null, cGroupTranslator, null);
    cgroupPolicy.apply(null,
        SpeRuntimeInfo.combination(stormAdapter.runtimeInfo(), flinkAdapter.runtimeInfo()),
        cGroupTranslator, null);
  }


}
