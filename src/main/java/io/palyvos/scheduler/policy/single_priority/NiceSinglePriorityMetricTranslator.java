package io.palyvos.scheduler.policy.single_priority;

import io.palyvos.scheduler.policy.normalizers.DecisionNormalizer;
import io.palyvos.scheduler.policy.normalizers.MinMaxDecisionNormalizer;
import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.util.command.ReniceCommand;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NiceSinglePriorityMetricTranslator extends AbstractSinglePriorityMetricTranslator {

  private static final Logger LOG = LogManager.getLogger(NiceSinglePriorityMetricTranslator.class);

  public static NiceSinglePriorityMetricTranslator withLinearNormalizer(int minPriorityNiceValue,
      int maxPriorityNiceValue) {
    Validate.isTrue(maxPriorityNiceValue < minPriorityNiceValue,
        "nice priorities go from high to low but maxPriorityNiceValue >= minPriorityNiceValue: %d, %d",
        maxPriorityNiceValue, minPriorityNiceValue);
    return new NiceSinglePriorityMetricTranslator(new MinMaxDecisionNormalizer(minPriorityNiceValue,
        maxPriorityNiceValue));
  }

  public NiceSinglePriorityMetricTranslator(DecisionNormalizer normalizer) {
    super(normalizer);
  }

  @Override
  protected final Future<?> apply(ExternalThread thread, long priority, ExecutorService executor) {
    LOG.trace("renice {} ({}) => {}", thread.name(), thread.pid(), priority);
    return executor.submit((Runnable) new ReniceCommand(thread.pid(), priority));
  }

}
