package io.palyvos.scheduler.integration;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import io.palyvos.scheduler.adapters.liebre.LiebreAdapter;
import io.palyvos.scheduler.adapters.liebre.LiebreMetricProvider;
import io.palyvos.scheduler.adapters.linux.LinuxMetricProvider;
import io.palyvos.scheduler.metric.SchedulerMetric;
import io.palyvos.scheduler.metric.MetricProvider;
import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.policy.translators.concrete.ConcretePolicyTranslator;
import io.palyvos.scheduler.policy.translators.concrete.NicePolicyTranslator;
import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.task.Subtask;
import io.palyvos.scheduler.util.SchedulerContext;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LiebreNicePolicy {

  private static final Logger LOG = LogManager.getLogger(LiebreNicePolicy.class);

  public static void main(String[] args) throws InterruptedException, IOException {
    Thread.sleep(15000);
    Config config = new Config();
    JCommander.newBuilder().addObject(config).build().parse(args);
    SchedulerContext.initSpeProcessInfo(config.pid);
    SchedulerContext.switchToSpeProcessContext();
    LiebreAdapter liebreAdapter = new LiebreAdapter(config.pid, config.queryDAG);
    liebreAdapter.updateTasks();
    ConcretePolicyTranslator translator = NicePolicyTranslator
        .withLinearNormalizer(config.maxPriorityNiceValue, config.minPriorityNiceValue);
    SchedulerMetricProvider metricsCollector = new SchedulerMetricProvider(
        new LinuxMetricProvider(config.pid),
        new LiebreMetricProvider("129.16.20.158", 80, "liebre.OS2"));
    metricsCollector.setTaskIndex(liebreAdapter.taskIndex());
    metricsCollector.register(config.metric);

    while (true) {
      metricsCollector.run();
      schedule(metricsCollector, config.metric, translator, liebreAdapter);
      Thread.sleep(TimeUnit.SECONDS.toMillis(config.period));
    }
  }

  private static void schedule(MetricProvider collector, SchedulerMetric metric,
      ConcretePolicyTranslator translator,
      LiebreAdapter liebreAdapter) {
    Map<ExternalThread, Double> schedule = new HashMap<>();
    for (Subtask subtask : liebreAdapter.taskIndex().subtasks()) {
      try {
        double value = collector.get(metric, subtask.id());
        schedule.put(subtask.thread(), value);
      } catch (Exception e) {
        LOG.error("Failed to get metric {} for task {}: {}\n", metric, subtask, e.getMessage());
        throw new RuntimeException(e);
      }
    }
    translator.applyPolicy(schedule);
  }

  static class Config {

    @Parameter(names = "--metric")
    private SchedulerMetric metric;

    @Parameter(names = "--pid", required = true)
    private int pid;

    @Parameter(names = "--min")
    private int minPriorityNiceValue = -20;

    @Parameter(names = "--max")
    private int maxPriorityNiceValue = 10;

    @Parameter(names = "--period")
    private long period = 1;

    @Parameter(names = "--queryDAG", required = true)
    private String queryDAG;
  }

}
