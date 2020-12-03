package io.palyvos.scheduler.integration;

import com.beust.jcommander.JCommander;
import io.palyvos.scheduler.adapters.SpeAdapter;
import io.palyvos.scheduler.adapters.liebre.LiebreAdapter;
import io.palyvos.scheduler.adapters.linux.LinuxAdapter;
import io.palyvos.scheduler.adapters.linux.LinuxMetricProvider;
import io.palyvos.scheduler.adapters.storm.StormAdapter;
import io.palyvos.scheduler.adapters.storm.StormGraphiteMetricProvider;
import io.palyvos.scheduler.metric.BasicSchedulerMetric;
import io.palyvos.scheduler.metric.SchedulerMetric;
import io.palyvos.scheduler.metric.MetricFileReporter;
import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.policy.translators.concrete.ConcretePolicyTranslator;
import io.palyvos.scheduler.policy.translators.concrete.NicePolicyTranslator;
import io.palyvos.scheduler.policy.translators.concrete.normalizers.DecisionNormalizer;
import io.palyvos.scheduler.policy.translators.concrete.normalizers.LogDecisionNormalizer;
import io.palyvos.scheduler.policy.translators.concrete.normalizers.MinMaxDecisionNormalizer;
import io.palyvos.scheduler.util.JcmdCommand;
import io.palyvos.scheduler.util.SchedulerContext;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

public class StormIntegration {

  private static final Logger LOG = LogManager.getLogger();

  public static void main(String[] args) throws InterruptedException {

    ExecutionConfig config = new ExecutionConfig();
    JCommander jCommander = JCommander.newBuilder().addObject(config).build();
    jCommander.parse(args);
    if (config.help) {
      jCommander.usage();
      return;
    }
    Configurator.setRootLevel(config.log);
    config.retrievePids(StormIntegration.class);

    SchedulerContext.initSpeProcessInfo(config.pids.get(0));
    SchedulerContext.switchToSpeProcessContext();
    SchedulerContext.METRIC_RECENT_PERIOD_SECONDS = config.window;
    SchedulerContext.STATISTICS_FOLDER = config.statisticsFolder;
    SchedulerContext.THREAD_NAME_GRAPHITE_CONVERTER = StormAdapter.THREAD_NAME_GRAPHITE_CONVERTER;

    StormAdapter adapter = new StormAdapter(config.pids, new LinuxAdapter(), config.queryGraphPath);
    config.tryUpdateTasks(adapter);
    SchedulerMetricProvider metricProvider = new SchedulerMetricProvider(
        new StormGraphiteMetricProvider("129.16.20.158", 80),
        new LinuxMetricProvider(config.pids));
    DecisionNormalizer normalizer = new MinMaxDecisionNormalizer(config.minPriority, config.maxPriority);
    if (config.logarithmic) {
      normalizer = new LogDecisionNormalizer(normalizer);
    }
    ConcretePolicyTranslator translator = new NicePolicyTranslator(normalizer);
    metricProvider.setTaskIndex(adapter.taskIndex());

    config.policy.init(translator, metricProvider);
    while (true) {
      long start = System.currentTimeMillis();
      metricProvider.run();
      config.policy.apply(adapter.taskIndex().tasks(), translator, metricProvider);
      LOG.debug("Scheduling took {} ms", System.currentTimeMillis() - start);
      Thread.sleep(TimeUnit.SECONDS.toMillis(config.period));
    }
  }



}
