package io.palyvos.scheduler.integration;

import io.palyvos.scheduler.adapters.flink.FlinkAdapter;
import io.palyvos.scheduler.adapters.flink.FlinkGraphiteMetricProvider;
import io.palyvos.scheduler.adapters.linux.LinuxAdapter;
import io.palyvos.scheduler.adapters.linux.LinuxMetricProvider;
import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.policy.single_priority.SinglePriorityMetricTranslator;
import io.palyvos.scheduler.policy.single_priority.NiceSinglePriorityMetricTranslator;
import io.palyvos.scheduler.policy.normalizers.DecisionNormalizer;
import io.palyvos.scheduler.policy.normalizers.ExponentialSmoothingDecisionNormalizer;
import io.palyvos.scheduler.policy.normalizers.LogDecisionNormalizer;
import io.palyvos.scheduler.policy.normalizers.MinMaxDecisionNormalizer;
import io.palyvos.scheduler.util.SchedulerContext;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FlinkIntegration {

  private static final Logger LOG = LogManager.getLogger();
  public static final String GRAPHITE_HOST = "129.16.20.158";
  public static final int GRAPHITE_READ_PORT = 80;
  public static final int GRAPHITE_WRITE_PORT = 2003;

  public static void main(String[] args) throws InterruptedException {

    ExecutionConfig config = ExecutionConfig.init(args, FlinkIntegration.class);
    SchedulerContext.THREAD_NAME_GRAPHITE_CONVERTER = FlinkAdapter.THREAD_NAME_GRAPHITE_CONVERTER;

    FlinkAdapter adapter = new FlinkAdapter(config.pids, "localhost", 8081, new LinuxAdapter());
    config.tryUpdateTasks(adapter);
    SchedulerMetricProvider metricProvider = new SchedulerMetricProvider(
        new FlinkGraphiteMetricProvider(GRAPHITE_HOST, GRAPHITE_READ_PORT, adapter.tasks()),
        new LinuxMetricProvider(config.pids));
    DecisionNormalizer normalizer = new MinMaxDecisionNormalizer(config.minPriority,
        config.maxPriority);
    if (config.logarithmic) {
      normalizer = new LogDecisionNormalizer(normalizer);
    }
    normalizer = new ExponentialSmoothingDecisionNormalizer(normalizer, config.smoothingFactor);
    SinglePriorityMetricTranslator translator = new NiceSinglePriorityMetricTranslator(normalizer);
    metricProvider.setTaskIndex(adapter.taskIndex());
//    Collection<MetricGraphiteReporter<SchedulerMetric>> reporters = MetricGraphiteReporter
//        .reportersFor(GRAPHITE_HOST, GRAPHITE_WRITE_PORT, metricProvider,
//            BasicSchedulerMetric.TASK_QUEUE_SIZE_FROM_SUBTASK_DATA,
//            BasicSchedulerMetric.SUBTASK_SELECTIVITY, BasicSchedulerMetric.SUBTASK_COST,
//            BasicSchedulerMetric.SUBTASK_GLOBAL_SELECTIVITY, BasicSchedulerMetric.SUBTASK_GLOBAL_AVERAGE_COST);
    config.policy.init(translator, metricProvider);
    while (true) {
      long start = System.currentTimeMillis();
      metricProvider.run();
//      for (MetricGraphiteReporter<?> reporter : reporters) {
//        reporter.report();
//      }
      config.policy.apply(adapter.taskIndex().tasks(), translator, metricProvider);
      LOG.debug("Scheduling took {} ms", System.currentTimeMillis() - start);
      Thread.sleep(TimeUnit.SECONDS.toMillis(config.period));
    }
  }


}
