package io.palyvos.scheduler.policy.translators.concrete;

import io.palyvos.scheduler.policy.translators.concrete.normalizers.DecisionNormalizer;
import io.palyvos.scheduler.policy.translators.concrete.normalizers.MinMaxDecisionNormalizer;
import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.util.ReniceCommand;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NicePolicyTranslator extends SingleValueConcretePolicyTranslator {

  private static final Logger LOG = LogManager.getLogger(NicePolicyTranslator.class);

  public static NicePolicyTranslator withLinearNormalizer(int minPriorityNiceValue, int maxPriorityNiceValue) {
    Validate.isTrue(maxPriorityNiceValue < minPriorityNiceValue, "nice priorities go from high to low but maxPriorityNiceValue >= minPriorityNiceValue: %d, %d",
        maxPriorityNiceValue, minPriorityNiceValue);
    return new NicePolicyTranslator(new MinMaxDecisionNormalizer(minPriorityNiceValue,
        maxPriorityNiceValue));
  }

  public NicePolicyTranslator(DecisionNormalizer normalizer) {
    super(normalizer);
  }

  @Override
  protected final Future<?> apply(ExternalThread thread, long priority, ExecutorService executor) {
    LOG.trace("renice {} ({}) => {}", thread.name(), thread.pid(), priority);
    return executor.submit((Runnable) new ReniceCommand(thread.pid(), priority));
  }

}
