package io.palyvos.scheduler.integration;

import io.palyvos.scheduler.adapters.liebre.LiebreAdapter;
import io.palyvos.scheduler.adapters.liebre.LiebreMetricProvider;
import io.palyvos.scheduler.adapters.linux.LinuxMetricProvider;
import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.policy.translators.concrete.ConcretePolicyTranslator;
import io.palyvos.scheduler.policy.translators.concrete.NicePolicyTranslator;
import io.palyvos.scheduler.policy.translators.concrete.normalizers.DecisionNormalizer;
import io.palyvos.scheduler.policy.translators.concrete.normalizers.LogDecisionNormalizer;
import io.palyvos.scheduler.policy.translators.concrete.normalizers.MinMaxDecisionNormalizer;
import io.palyvos.scheduler.util.SchedulerContext;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LiebreIntegration {

  private static final Logger LOG = LogManager.getLogger(LiebreIntegration.class);

  public static void main(String[] args) throws InterruptedException {
    ExecutionConfig config = ExecutionConfig.init(args, LiebreIntegration.class);
    SchedulerContext.THREAD_NAME_GRAPHITE_CONVERTER = LiebreAdapter.THREAD_NAME_GRAPHITE_CONVERTER;

    Validate.validState(config.pids.size() == 1, "Only one Liebre instance supported!");
    LiebreAdapter adapter = new LiebreAdapter(config.pids.get(0), config.queryGraphPath);
    config.tryUpdateTasks(adapter);
    SchedulerMetricProvider metricProvider = new SchedulerMetricProvider(
        new LinuxMetricProvider(config.pids.get(0)),
        new LiebreMetricProvider("129.16.20.158", 80, adapter.tasks()));
    metricProvider.setTaskIndex(adapter.taskIndex());
    DecisionNormalizer normalizer = new MinMaxDecisionNormalizer(config.minPriority,
        config.maxPriority);
    if (config.logarithmic) {
      normalizer = new LogDecisionNormalizer(normalizer);
    }
    ConcretePolicyTranslator translator = new NicePolicyTranslator(normalizer);

    config.policy.init(translator, metricProvider);
    config.cgroupPolicy.init(adapter.tasks(), metricProvider);
    while (true) {
      long start = System.currentTimeMillis();
      metricProvider.run();
      try {
        config.schedule(config, adapter, metricProvider, translator);
      }
      catch (Exception e) {
        LOG.error("Failed to schedule", e);
      }
      LOG.debug("Scheduling took {} ms", System.currentTimeMillis() - start);
      config.sleep();
    }
  }


}
