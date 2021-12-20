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

/**
 * Translator that uses the linux {@code nice} mechanism to apply {@link SinglePriorityPolicy}.
 */
public class NiceSinglePriorityTranslator extends AbstractSinglePriorityTranslator {

  public static final String NAME = "NICE";
  private static final Logger LOG = LogManager.getLogger(NiceSinglePriorityTranslator.class);

  public static NiceSinglePriorityTranslator withLinearNormalizer(int minPriorityNiceValue,
      int maxPriorityNiceValue) {
    Validate.isTrue(maxPriorityNiceValue < minPriorityNiceValue,
        "nice priorities go from high to low but maxPriorityNiceValue >= minPriorityNiceValue: %d, %d",
        maxPriorityNiceValue, minPriorityNiceValue);
    return new NiceSinglePriorityTranslator(new MinMaxDecisionNormalizer(minPriorityNiceValue,
        maxPriorityNiceValue));
  }

  public NiceSinglePriorityTranslator(DecisionNormalizer normalizer) {
    super(normalizer);
  }

  @Override
  protected final Future<?> apply(ExternalThread thread, long priority, ExecutorService executor) {
    LOG.trace("renice {} ({}) => {}", thread.name(), thread.pid(), priority);
    return executor.submit((Runnable) new ReniceCommand(thread.pid(), priority));
  }

}
