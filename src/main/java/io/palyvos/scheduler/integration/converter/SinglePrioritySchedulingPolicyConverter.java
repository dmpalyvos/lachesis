package io.palyvos.scheduler.integration.converter;

import com.beust.jcommander.IStringConverter;
import io.palyvos.scheduler.metric.BasicSchedulerMetric;
import io.palyvos.scheduler.policy.single_priority.NoopSinglePrioritySchedulingPolicy;
import io.palyvos.scheduler.policy.single_priority.ConstantSinglePrioritySchedulingPolicy;
import io.palyvos.scheduler.policy.single_priority.MetricSinglePrioritySchedulingPolicy;
import io.palyvos.scheduler.policy.single_priority.RandomSinglePrioritySchedulingPolicy;
import io.palyvos.scheduler.policy.single_priority.SinglePrioritySchedulingPolicy;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SinglePrioritySchedulingPolicyConverter implements IStringConverter<SinglePrioritySchedulingPolicy> {

  private static final String NO_POLICY = "none";
  private final Pattern METRIC_POLICY_PATTERN = Pattern.compile("metric:(\\w+):?(true|false)?");
  private final Pattern CONSTANT_POLICY_MATCHER = Pattern.compile("constant:(-?\\d+):?(true|false)?");
  private final Pattern RANDOM_POLICY_MATCHER = Pattern.compile("random:?(true|false)?");

  @Override
  public SinglePrioritySchedulingPolicy convert(String argument) {
    if (argument.trim() == NO_POLICY) {
      return new NoopSinglePrioritySchedulingPolicy();
    }
    final Matcher metricMatcher = METRIC_POLICY_PATTERN.matcher(argument);
    if (metricMatcher.matches()) {
      final boolean scheduleHelpers = Boolean.valueOf(metricMatcher.group(2));
      return new MetricSinglePrioritySchedulingPolicy(BasicSchedulerMetric.valueOf(metricMatcher.group(1)),
          scheduleHelpers);
    }
    final Matcher constantMatcher = CONSTANT_POLICY_MATCHER.matcher(argument);
    if (constantMatcher.matches()) {
      final boolean scheduleHelpers = Boolean.valueOf(metricMatcher.group(2));
      return new ConstantSinglePrioritySchedulingPolicy(Long.valueOf(constantMatcher.group(1)),
          scheduleHelpers);
    }
    final Matcher randomMatcher = RANDOM_POLICY_MATCHER.matcher(argument);
    if (randomMatcher.matches()) {
      final boolean scheduleHelpers = Boolean.valueOf(metricMatcher.group(1));
      return new RandomSinglePrioritySchedulingPolicy(scheduleHelpers);
    }
    throw new IllegalArgumentException(String.format("Unknown policy requested: %s", argument));
  }
}
