package io.palyvos.scheduler.policy.single_priority;

import io.palyvos.scheduler.policy.normalizers.DecisionNormalizer;
import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.util.SchedulerContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class AbstractSinglePriorityMetricTranslator implements
    SinglePriorityMetricTranslator {

  private static final Logger LOG = LogManager.getLogger();
  public static final int TRANSLATOR_THREADS = 4;
  protected final DecisionNormalizer normalizer;
  private final Map<ExternalThread, Long> lastSchedule = new HashMap<>();
  private final SinglePriorityScheduleFileReporter reporter = new SinglePriorityScheduleFileReporter();
  private final SinglePriorityScheduleGraphiteReporter graphiteReporter = new SinglePriorityScheduleGraphiteReporter(
      SchedulerContext.GRAPHITE_STATS_HOST, SchedulerContext.GRAPHITE_STATS_PORT);

  public AbstractSinglePriorityMetricTranslator(DecisionNormalizer normalizer) {
    Validate.notNull(normalizer, "normalizer");
    this.normalizer = normalizer;
  }

  @Override
  public void apply(Map<ExternalThread, Double> schedule) {
    Validate.notEmpty(schedule, "No scheduling decisions found!");
    final long start = System.currentTimeMillis();
    if (!normalizer.isValid(schedule)) {
      LOG.warn("Invalid schedule detected. Skipping scheduling round...");
      return;
    }
    final Map<ExternalThread, Long> normalizedSchedule = normalizer.normalize(schedule);
    reportStatistics(schedule, normalizedSchedule);
    final int updates = applyDirect(normalizedSchedule);
    LOG.debug("{} finished applying policy: {} priority updates ({} ms)",
        getClass().getSimpleName(),
        updates, System.currentTimeMillis() - start);

  }

  protected final int applyDirect(Map<ExternalThread, Long> normalizedSchedule) {
    final List<Future<?>> futures = new ArrayList<>();
    SchedulerContext.switchToRootContext();
    final ExecutorService executor = Executors.newFixedThreadPool(TRANSLATOR_THREADS);
    for (ExternalThread thread : normalizedSchedule.keySet()) {
      long priority = normalizedSchedule.get(thread);
      Long previousPriority = lastSchedule.put(thread, priority);
      if (previousPriority == null || previousPriority != priority) {
        futures.add(apply(thread, priority, executor));
      }
    }
    wait(futures);
    executor.shutdown();
    SchedulerContext.switchToSpeProcessContext();
    return futures.size();
  }

  private void reportStatistics(Map<ExternalThread, Double> schedule,
      Map<ExternalThread, Long> normalizedSchedule) {
    long now = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    graphiteReporter.open();
    normalizedSchedule.entrySet().stream().forEach(threadPriorityEntry ->
    {
      reporter.add(now,
          threadPriorityEntry.getKey().name(),
          threadPriorityEntry.getValue(), schedule.get(threadPriorityEntry.getKey()));
      graphiteReporter.add(now,
          threadPriorityEntry.getKey().name(),
          threadPriorityEntry.getValue(), schedule.get(threadPriorityEntry.getKey()));
    });
    graphiteReporter.close();
  }

  protected abstract Future<?> apply(ExternalThread thread, long priority,
      ExecutorService executor);

  private void wait(List<Future<?>> futures) {
    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (InterruptedException e) {
        LOG.error("Interrupted while waiting for policy to be applied");
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        LOG.error("Policy application failed!");
        throw new RuntimeException(e);
      }
    }
  }
}
