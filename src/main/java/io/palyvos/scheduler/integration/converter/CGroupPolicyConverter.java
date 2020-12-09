package io.palyvos.scheduler.integration.converter;

import com.beust.jcommander.IStringConverter;
import io.palyvos.scheduler.metric.BasicSchedulerMetric;
import io.palyvos.scheduler.policy.CGroupMetricPolicy;
import io.palyvos.scheduler.policy.CGroupNoopPolicy;
import io.palyvos.scheduler.policy.CGroupSchedulingPolicy;
import io.palyvos.scheduler.policy.translators.cgroup.CGroupAgnosticTranslator;
import io.palyvos.scheduler.policy.translators.cgroup.CGroupSchedulingFunction;
import io.palyvos.scheduler.policy.translators.cgroup.ClusteringCGroupTranslator;
import io.palyvos.scheduler.policy.translators.cgroup.CpuQuotaFunction;
import io.palyvos.scheduler.policy.translators.cgroup.CpuSharesScheduleFunction;
import io.palyvos.scheduler.policy.translators.cgroup.InterQueryCGroupTranslator;
import io.palyvos.scheduler.task.Query;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CGroupPolicyConverter implements IStringConverter<CGroupSchedulingPolicy> {

  private static final String NO_POLICY = "none";
  public static final BiFunction<Query, Map<String, Double>, Double> DEFAULT_QUERY_FUNCTION =
      (query, values) -> query.sources().stream().map(source -> values.get(source.id()))
          .filter(Objects::nonNull).mapToDouble(Double::doubleValue).average().orElse(0);
  public static final int DEFAULT_CPU_PERIOD = 100000;
  public static final int DEFAULT_NCORES = 4;
  public static final int DEFAULT_NGROUPS = 5;
  //metric:translator:parameter:metric
  private final Pattern METRIC_POLICY_PATTERN = Pattern.compile("metric:(\\w+):(\\w+):(\\w+)");

  @Override
  public CGroupSchedulingPolicy convert(String argument) {
    final Matcher metricMatcher = METRIC_POLICY_PATTERN.matcher(argument);
    if (argument.trim() == NO_POLICY) {
      return new CGroupNoopPolicy();
    }
    if (metricMatcher.matches()) {
      final CGroupAgnosticTranslator translator = translator(metricMatcher.group(1));
      final CGroupSchedulingFunction schedulingFunction = schedulingFunction(
          metricMatcher.group(2));
      final BasicSchedulerMetric metric = BasicSchedulerMetric.valueOf(metricMatcher.group(3));
      return new CGroupMetricPolicy(metric, translator, schedulingFunction);
    }
    throw new IllegalArgumentException(String.format("Unknown policy requested: %s", argument));
  }

  private CGroupAgnosticTranslator translator(String name) {
    if (ClusteringCGroupTranslator.NAME.equals(name)) {
      return new ClusteringCGroupTranslator(DEFAULT_NGROUPS);
    }
    if (InterQueryCGroupTranslator.NAME.equals(name)) {
      return new InterQueryCGroupTranslator(DEFAULT_QUERY_FUNCTION);
    }
    throw new IllegalArgumentException(String.format("Unknown cgroup translator %s", name));
  }

  private CGroupSchedulingFunction schedulingFunction(String name) {
    if (CpuSharesScheduleFunction.NAME.equals(name)) {
      return new CpuSharesScheduleFunction();
    }
    if (CpuQuotaFunction.NAME.equals(name)) {
      return new CpuQuotaFunction(DEFAULT_CPU_PERIOD, DEFAULT_NCORES);
    }
    throw new IllegalArgumentException(String.format("Unknown cgroup function %s", name));
  }
}
