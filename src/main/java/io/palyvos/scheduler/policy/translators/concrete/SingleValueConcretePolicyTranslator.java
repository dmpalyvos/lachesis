package io.palyvos.scheduler.policy.translators.concrete;

import io.palyvos.scheduler.policy.translators.SingleValueScheduleFileReporter;
import io.palyvos.scheduler.policy.translators.SingleValueScheduleGraphiteReporter;
import io.palyvos.scheduler.policy.translators.concrete.normalizers.DecisionNormalizer;
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

public abstract class SingleValueConcretePolicyTranslator implements ConcretePolicyTranslator {

  private static final Logger LOG = LogManager.getLogger();
  public static final int TRANSLATOR_THREADS = 4;
  protected final DecisionNormalizer normalizer;
  private final Map<ExternalThread, Long> lastSchedule = new HashMap<>();
  private final SingleValueScheduleFileReporter reporter = new SingleValueScheduleFileReporter();
  private final SingleValueScheduleGraphiteReporter graphiteReporter = new SingleValueScheduleGraphiteReporter(
      "129.16.20.158", 2003);

  public SingleValueConcretePolicyTranslator(DecisionNormalizer normalizer) {
    Validate.notNull(normalizer, "normalizer");
    this.normalizer = normalizer;
  }

  @Override
  public void applyPolicy(Map<ExternalThread, Double> schedule) {
    Validate.notEmpty(schedule, "No scheduling decisions found!");
    final long start = System.currentTimeMillis();
    if (!normalizer.isValid(schedule)) {
      LOG.warn("Invalid schedule detected. Skipping scheduling round...");
      return;
    }
    final Map<ExternalThread, Long> normalizedSchedule = normalizer.normalize(schedule);
    reportStatistics(schedule, normalizedSchedule);
    final int updates = applyDirect(normalizedSchedule);
    LOG.info("{} finished applying policy: {} priority updates ({} ms)", getClass().getSimpleName(),
        updates, System.currentTimeMillis() - start);

  }

  @Override
  public int applyDirect(Map<ExternalThread, Long> normalizedSchedule) {
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
    normalizedSchedule.entrySet().stream().forEach(threadPriorityEntry ->
    {
      reporter.add(now,
          threadPriorityEntry.getKey().name(),
          threadPriorityEntry.getValue(), schedule.get(threadPriorityEntry.getKey()));
      graphiteReporter.add(now,
          threadPriorityEntry.getKey().name(),
          threadPriorityEntry.getValue(), schedule.get(threadPriorityEntry.getKey()));
    });

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
