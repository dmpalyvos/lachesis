package io.palyvos.scheduler.integration.converter;

import com.beust.jcommander.IStringConverter;
import io.palyvos.scheduler.metric.BasicSchedulerMetric;
import io.palyvos.scheduler.metric.SchedulerMetric;
import io.palyvos.scheduler.policy.cgroup.CGroupSchedulingPolicy;
import io.palyvos.scheduler.policy.cgroup.ClusterinCGroupSchedulingPolicy;
import io.palyvos.scheduler.policy.cgroup.FairQueryCGroupPolicy;
import io.palyvos.scheduler.policy.cgroup.MetricQueryCGroupPolicy;
import io.palyvos.scheduler.task.Query;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.Validate;

public class CGroupSchedulingPolicyConverter implements IStringConverter<CGroupSchedulingPolicy> {

  public static final BiFunction<Query, Map<String, Double>, Double> DEFAULT_QUERY_FUNCTION =
      (query, values) -> query.sources().stream().map(source -> values.get(source.id()))
          .filter(Objects::nonNull).mapToDouble(Double::doubleValue).average().orElse(0);
  public static final int DEFAULT_NGROUPS = 5;
  //policy(:metric)?
  private final Pattern METRIC_POLICY_PATTERN = Pattern.compile("(\\w+):?(\\w+)?");

  @Override
  public CGroupSchedulingPolicy convert(String argument) {
    final Matcher metricMatcher = METRIC_POLICY_PATTERN.matcher(argument);
    if (metricMatcher.matches()) {
      final String policyName = metricMatcher.group(1).toUpperCase();
      final BasicSchedulerMetric metric =
          metricMatcher.group(2) != null ? BasicSchedulerMetric.valueOf(metricMatcher.group(2))
              : null;
      return policy(policyName, metric);
    }
    throw new IllegalArgumentException(String.format("Unknown policy requested: %s", argument));
  }


  private CGroupSchedulingPolicy policy(String policyName, SchedulerMetric metric) {
    if (FairQueryCGroupPolicy.NAME.equals(policyName)) {
      return new FairQueryCGroupPolicy();
    }
    if (MetricQueryCGroupPolicy.NAME.equals(policyName)) {
      Validate.notNull(metric, "No metric specified");
      return new MetricQueryCGroupPolicy(metric, DEFAULT_QUERY_FUNCTION);
    }
    if (ClusterinCGroupSchedulingPolicy.NAME.equals(policyName)) {
      Validate.notNull(metric, "No metric specified");
      return new ClusterinCGroupSchedulingPolicy(metric, DEFAULT_NGROUPS);
    }
    throw new IllegalArgumentException(String.format("Unknown cgroup policy %s", policyName));
  }
}
