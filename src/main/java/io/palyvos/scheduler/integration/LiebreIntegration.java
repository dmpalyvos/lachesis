package io.palyvos.scheduler.integration;

import com.beust.jcommander.JCommander;
import io.palyvos.scheduler.adapters.liebre.LiebreAdapter;
import io.palyvos.scheduler.adapters.liebre.LiebreMetricProvider;
import io.palyvos.scheduler.adapters.linux.LinuxMetricProvider;
import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.policy.translators.concrete.ConcretePolicyTranslator;
import io.palyvos.scheduler.policy.translators.concrete.NicePolicyTranslator;
import io.palyvos.scheduler.policy.translators.concrete.normalizers.DecisionNormalizer;
import io.palyvos.scheduler.policy.translators.concrete.normalizers.LogDecisionNormalizer;
import io.palyvos.scheduler.policy.translators.concrete.normalizers.MinMaxDecisionNormalizer;
import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.task.Subtask;
import io.palyvos.scheduler.util.SchedulerContext;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

public class LiebreIntegration {

  private static final Logger LOG = LogManager.getLogger(LiebreIntegration.class);
  public static final long DEFAULT_NICE_VALUE = 0;

  public static void main(String[] args) throws InterruptedException, IOException {
    ExecutionConfig config = new ExecutionConfig();
    JCommander jCommander = JCommander.newBuilder().addObject(config).build();
    jCommander.parse(args);
    if (config.help) {
      jCommander.usage();
      return;
    }
    Configurator.setRootLevel(config.log);
    config.retrievePids(LiebreIntegration.class);

    SchedulerContext.initSpeProcessInfo(config.pids.get(0));
    SchedulerContext.switchToSpeProcessContext();
    SchedulerContext.METRIC_RECENT_PERIOD_SECONDS = config.window;
    SchedulerContext.STATISTICS_FOLDER = config.statisticsFolder;
    SchedulerContext.THREAD_NAME_GRAPHITE_CONVERTER = LiebreAdapter.THREAD_NAME_GRAPHITE_CONVERTER;

    Validate.validState(config.pids.size() == 1, "Only one Liebre instance supported!");
    LiebreAdapter adapter = new LiebreAdapter(config.pids.get(0), config.queryGraphPath);
    config.tryUpdateTasks(adapter);
    SchedulerMetricProvider metricProvider = new SchedulerMetricProvider(
        new LinuxMetricProvider(config.pids.get(0)),
        new LiebreMetricProvider("129.16.20.158", 80, adapter.tasks()));
    metricProvider.setTaskIndex(adapter.taskIndex());
    DecisionNormalizer normalizer = new MinMaxDecisionNormalizer(config.minPriority, config.maxPriority);
    if (config.logarithmic) {
      normalizer = new LogDecisionNormalizer(normalizer);
    }
    ConcretePolicyTranslator translator = new NicePolicyTranslator(normalizer);

//    final Thread hook = new Thread(() -> {
//      LOG.warn("Shutting down JVM...");
//      translator.applyDirect(defaultNiceSchedule(adapter.taskIndex().subtasks()));
//    });
//    Runtime.getRuntime().addShutdownHook(hook);

    config.policy.init(translator, metricProvider);
    while (true) {
      long start = System.currentTimeMillis();
      metricProvider.run();
      config.policy.apply(adapter.taskIndex().subtasks(), translator, metricProvider);
      LOG.debug("Scheduling took {} ms", System.currentTimeMillis() - start);
      Thread.sleep(TimeUnit.SECONDS.toMillis(config.period));
    }
  }

  private static Map<ExternalThread, Long> defaultNiceSchedule(Collection<Subtask> subtasks) {
    final Map<ExternalThread, Long> schedule = new HashMap<>();
    subtasks.forEach(subtask -> schedule.put(subtask.thread(), DEFAULT_NICE_VALUE));
    return schedule;
  }


}
