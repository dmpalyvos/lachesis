package io.palyvos.scheduler.policy.cgroup;

import io.palyvos.scheduler.metric.graphite.SimpleGraphiteReporter;
import io.palyvos.scheduler.util.SchedulerContext;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CGroupScheduleGraphiteReporter {

  private static final Logger LOG = LogManager.getLogger();
  private static final String GRAPHITE_PREFIX = "schedule.cgroup";

  private final SimpleGraphiteReporter reporter;

  public CGroupScheduleGraphiteReporter(String host, int port) {
    this.reporter = new SimpleGraphiteReporter(host, port);
  }

  public void report(Map<CGroup, Double> internalMetrics,
      Map<CGroup, Collection<CGroupParameterContainer>> schedule) {
    final long now = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    reporter.open();
    for (CGroup cgroup : internalMetrics.keySet()) {
      Double internalValue = internalMetrics.get(cgroup);
      if (internalValue != null) {
        reportInternalValue(now, cgroup, internalValue);
      }
      Collection<CGroupParameterContainer> parameters = schedule.get(cgroup);
      if (parameters != null) {
        reportExternalValues(now, cgroup, parameters);
      }
    }
    reporter.close();
  }

  private void reportExternalValues(long now, CGroup cgroup,
      Collection<CGroupParameterContainer> parameters) {
    for (CGroupParameterContainer parameter : parameters) {
      try {
        final String parameterKey = parameter.key().replace(".", "_");
        final String entity = parameterKey + "." + cgroup.path();
        reporter.report(now, graphiteKey("external", entity), parameter.value());
      } catch (Exception e) {
        LOG.warn("Failed to report to graphite: {}", e.getMessage());
      }
    }
  }

  private void reportInternalValue(long now, CGroup cgroup, Double value) {
    try {
      reporter.report(now, graphiteKey("internal", cgroup.path()), value);
    } catch (Exception e) {
      LOG.warn("Failed to report to graphite: {}", e.getMessage());
    }
  }


  private String graphiteKey(String type, String entity) {
    return new StringBuilder(SchedulerContext.SCHEDULER_NAME).append(".")
                .append(GRAPHITE_PREFIX).append(".")
                .append(type).append(".")
                .append(entity).toString();
  }

}
