package io.palyvos.scheduler.integration;

import io.palyvos.scheduler.adapters.flink.FlinkAdapter;
import io.palyvos.scheduler.adapters.storm.StormAdapter;
import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.policy.single_priority.MultiSpePolicyTranslator;
import io.palyvos.scheduler.policy.single_priority.SinglePriorityMetricTranslator;
import io.palyvos.scheduler.util.SchedulerContext;
import java.util.List;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MultiSpeIntegration {

  private static final Logger LOG = LogManager.getLogger();

  public static void main(String[] args) throws InterruptedException {

    LOG.info("Usage: --worker <storm_worker> --worker <flink_worker>");
    Thread.sleep(2000);

    ExecutionConfig config = ExecutionConfig.init(args, MultiSpeIntegration.class);
    SchedulerContext.GRAPHITE_STATS_HOST = config.statisticsHost;

    Validate.isTrue(config.queryGraphPath.size() == 1,
        "Only one query graph allowed (storm)!");
    Validate.validState(config.pids.size() == 2,
        "Expected 2 pids but got %d", config.pids.size());

    final List<Integer> stormPids = config.pids.subList(0, 1);
    final List<Integer> flinkPids = config.pids.subList(1, 2);

    StormAdapter stormAdapter = StormIntegration
        .initAdapter(config, stormPids, config.queryGraphPath.get(0));
    SchedulerMetricProvider stormMetricProvider = StormIntegration
        .initMetricProvider(config, stormAdapter, stormPids);

    FlinkAdapter flinkAdapter = FlinkIntegration.initAdapter(config, flinkPids);
    SchedulerMetricProvider flinkMetricProvider = FlinkIntegration
        .initMetricProvider(config, flinkAdapter, flinkPids);

    SinglePriorityMetricTranslator translator = config.newSinglePriorityTranslator();
    MultiSpePolicyTranslator multiSpePolicyTranslator = new MultiSpePolicyTranslator(translator);

    initPolicies(config, stormAdapter, stormMetricProvider, flinkAdapter, flinkMetricProvider,
        translator);
    int retries = 0;
    while (true) {
      long start = System.currentTimeMillis();
      try {
        config.schedule(flinkAdapter, flinkMetricProvider, multiSpePolicyTranslator);
        config.schedule(stormAdapter, stormMetricProvider, multiSpePolicyTranslator);
        multiSpePolicyTranslator.run();
      } catch (Exception e) {
        if (retries++ > config.maxRetries()) {
          throw e;
        }
      }
      LOG.debug("Scheduling took {} ms", System.currentTimeMillis() - start);
      config.sleep();
    }
  }

  private static void initPolicies(ExecutionConfig config, StormAdapter stormAdapter,
      SchedulerMetricProvider stormMetricProvider, FlinkAdapter flinkAdapter,
      SchedulerMetricProvider flinkMetricProvider, SinglePriorityMetricTranslator translator) {
    config.policy.init(translator, flinkMetricProvider);
    config.policy.init(translator, stormMetricProvider);
    config.cgroupPolicy.init(flinkAdapter.tasks(), config.cGroupTranslator, flinkMetricProvider);
    config.cgroupPolicy.init(stormAdapter.tasks(), config.cGroupTranslator, stormMetricProvider);
  }

}
