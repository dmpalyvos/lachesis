package io.palyvos.scheduler.util;

import com.beust.jcommander.IStringConverter;
import io.palyvos.scheduler.metric.BasicSchedulerMetric;
import io.palyvos.scheduler.policy.ConstantConcreteSchedulingPolicy;
import io.palyvos.scheduler.policy.MetricConcreteSchedulingPolicy;
import io.palyvos.scheduler.policy.ConcreteSchedulingPolicy;
import io.palyvos.scheduler.policy.RandomConcreteSchedulingPolicy;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConcreteSchedulingPolicyConverter implements IStringConverter<ConcreteSchedulingPolicy> {

  private final Pattern METRIC_POLICY_PATTERN = Pattern.compile("metric:(\\w+)");
  private final Pattern CONSTANT_POLICY_MATCHER = Pattern.compile("constant:(-?\\d+)");

  @Override
  public ConcreteSchedulingPolicy convert(String argument) {
    final Matcher metricMatcher = METRIC_POLICY_PATTERN.matcher(argument);
    if (metricMatcher.matches()) {
      return new MetricConcreteSchedulingPolicy(BasicSchedulerMetric.valueOf(metricMatcher.group(1)));
    }
    final Matcher constantMatcher = CONSTANT_POLICY_MATCHER.matcher(argument);
    if (constantMatcher.matches()) {
      return new ConstantConcreteSchedulingPolicy(Long.valueOf(constantMatcher.group(1)));
    }
    if ("random".equals(argument)) {
      return new RandomConcreteSchedulingPolicy();
    }
    throw new IllegalArgumentException(String.format("Unknown policy requested: %s", argument));
  }
}
