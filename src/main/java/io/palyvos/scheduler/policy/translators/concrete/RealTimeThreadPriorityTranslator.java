package io.palyvos.scheduler.policy.translators.concrete;

import io.palyvos.scheduler.policy.translators.concrete.normalizers.DecisionNormalizer;
import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.util.command.RealTimeThreadCommand;
import io.palyvos.scheduler.util.command.RealTimeThreadCommand.RealTimeSchedulingAlgorithm;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RealTimeThreadPriorityTranslator extends SingleValueConcretePolicyTranslator {

  private static final Logger LOG = LogManager.getLogger(RealTimeThreadPriorityTranslator.class);
  private final RealTimeSchedulingAlgorithm algorithm;

  public RealTimeThreadPriorityTranslator(
      DecisionNormalizer normalizer, RealTimeSchedulingAlgorithm algorithm) {
    super(normalizer);
    this.algorithm = algorithm;
  }

  protected final Future<?> apply(ExternalThread thread, long priority, ExecutorService executor) {
    LOG.trace("chrt {} ({}) => {}", thread.name(), thread.pid(), priority);
    return executor.submit((Runnable) new RealTimeThreadCommand(thread.pid(), priority, algorithm));
  }

}
