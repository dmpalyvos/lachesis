package io.palyvos.scheduler.adapters.linux;

import io.palyvos.scheduler.metric.AbstractMetricProvider;
import io.palyvos.scheduler.metric.Metric;
import io.palyvos.scheduler.metric.SchedulerMetric;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LinuxMetricProvider extends AbstractMetricProvider<LinuxMetric> {

  private static final Logger LOG = LogManager.getLogger(LinuxMetricProvider.class);

  protected final Collection<Integer> pids;

  public LinuxMetricProvider(Collection<Integer> pids) {
    super(mappingFor(LinuxMetric.values()), LinuxMetric.class);
    Validate.notEmpty(pids, "At least one PID required!");
    this.pids = pids;
  }

  public LinuxMetricProvider(int pid) {
    this(Arrays.asList(pid));
  }

  @Override
  protected void doCompute(LinuxMetric metric) {
    metric.compute(this);
  }

}
