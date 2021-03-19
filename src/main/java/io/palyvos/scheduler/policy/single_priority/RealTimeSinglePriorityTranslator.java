package io.palyvos.scheduler.policy.single_priority;

import io.palyvos.scheduler.policy.normalizers.DecisionNormalizer;
import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.util.command.RealTimeThreadCommand;
import io.palyvos.scheduler.util.command.RealTimeThreadCommand.RealTimeSchedulingAlgorithm;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RealTimeSinglePriorityTranslator extends AbstractSinglePriorityTranslator {

  public static String NAME = "REAL-TIME";
  private static final Logger LOG = LogManager.getLogger(RealTimeSinglePriorityTranslator.class);
  private final RealTimeSchedulingAlgorithm algorithm;

  public RealTimeSinglePriorityTranslator(
      DecisionNormalizer normalizer, RealTimeSchedulingAlgorithm algorithm) {
    super(normalizer);
    this.algorithm = algorithm;
  }

  protected final Future<?> apply(ExternalThread thread, long priority, ExecutorService executor) {
    LOG.trace("chrt {} ({}) => {}", thread.name(), thread.pid(), priority);
    return executor.submit((Runnable) new RealTimeThreadCommand(thread.pid(), priority, algorithm));
  }

}
