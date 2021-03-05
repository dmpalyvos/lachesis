package io.palyvos.scheduler.policy.cgroup;

import io.palyvos.scheduler.metric.graphite.SimpleGraphiteReporter;
import io.palyvos.scheduler.util.SchedulerContext;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CGroupScheduleGraphiteReporter {

  private static final Logger LOG = LogManager.getLogger();
  private static final String GRAPHITE_PREFIX = "schedule.cgroup";

  private final SimpleGraphiteReporter reporter;
  private final String localIp;

  public CGroupScheduleGraphiteReporter(String host, int port) {
    this.reporter = new SimpleGraphiteReporter(host, port);
    try {
      this.localIp = Inet4Address.getLocalHost().getHostAddress().replace(".", "-");
    } catch (UnknownHostException e) {
      throw new IllegalStateException(e);
    }
  }

  public void report(Map<CGroup, Double> internalMetrics,
      Map<CGroup, Collection<CGroupParameterContainer>> schedule) {
    try {
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
    } catch (Exception exception) {
      LOG.error("Failed to report to graphite: {}", exception.getMessage());
    }
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
      reporter.report(now, graphiteKey("internal", cgroup.path()), Math.round(value));
    } catch (Exception e) {
      LOG.warn("Failed to report to graphite: {}", e.getMessage());
    }
  }


  private String graphiteKey(String type, String entity) {
    return String.valueOf(SchedulerContext.SCHEDULER_NAME) + "."
        + localIp + "."
        + GRAPHITE_PREFIX + "."
        + type + "."
        + entity;
  }

}
