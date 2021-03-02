package io.palyvos.scheduler.integration.converter;

import com.beust.jcommander.IStringConverter;
import io.palyvos.scheduler.metric.BasicSchedulerMetric;
import io.palyvos.scheduler.policy.single_priority.MetricPerQuerySinglePriorityPolicy;
import io.palyvos.scheduler.policy.single_priority.NoopSinglePriorityPolicy;
import io.palyvos.scheduler.policy.single_priority.ConstantSinglePriorityPolicy;
import io.palyvos.scheduler.policy.single_priority.MetricSinglePriorityPolicy;
import io.palyvos.scheduler.policy.single_priority.RandomSinglePriorityPolicy;
import io.palyvos.scheduler.policy.single_priority.SinglePriorityPolicy;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SinglePriorityPolicyConverter implements IStringConverter<SinglePriorityPolicy> {

  private static final String NO_POLICY = "none";
  private final Pattern METRIC_POLICY_PATTERN = Pattern.compile("(metric|query-metric):(\\w+):?(true|false)?");
  private final Pattern CONSTANT_POLICY_MATCHER = Pattern.compile("constant:(-?\\d+):?(true|false)?");
  private final Pattern RANDOM_POLICY_MATCHER = Pattern.compile("random:?(true|false)?");

  @Override
  public SinglePriorityPolicy convert(String argument) {
    if (NO_POLICY.equals(argument.trim())) {
      return new NoopSinglePriorityPolicy();
    }
    final Matcher metricMatcher = METRIC_POLICY_PATTERN.matcher(argument);
    if (metricMatcher.matches()) {
      final boolean scheduleHelpers = Boolean.valueOf(metricMatcher.group(3));
      if ("metric".equals(metricMatcher.group(1))) {
        return new MetricSinglePriorityPolicy(BasicSchedulerMetric.valueOf(metricMatcher.group(2)),
            scheduleHelpers);
      }
      else {
        return new MetricPerQuerySinglePriorityPolicy(BasicSchedulerMetric.valueOf(metricMatcher.group(2)),
            scheduleHelpers);
      }
    }
    final Matcher constantMatcher = CONSTANT_POLICY_MATCHER.matcher(argument);
    if (constantMatcher.matches()) {
      final boolean scheduleHelpers = Boolean.valueOf(constantMatcher.group(2));
      return new ConstantSinglePriorityPolicy(Long.valueOf(constantMatcher.group(1)),
          scheduleHelpers);
    }
    final Matcher randomMatcher = RANDOM_POLICY_MATCHER.matcher(argument);
    if (randomMatcher.matches()) {
      final boolean scheduleHelpers = Boolean.valueOf(randomMatcher.group(1));
      return new RandomSinglePriorityPolicy(scheduleHelpers);
    }
    throw new IllegalArgumentException(String.format("Unknown policy requested: %s", argument));
  }
}
