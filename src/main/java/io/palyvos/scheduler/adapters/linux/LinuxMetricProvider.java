package io.palyvos.scheduler.adapters.linux;

import io.palyvos.scheduler.metric.AbstractMetricProvider;
import io.palyvos.scheduler.metric.Metric;
import io.palyvos.scheduler.metric.SchedulerMetric;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LinuxMetricProvider extends AbstractMetricProvider<LinuxMetric> {

  private static final Logger LOG = LogManager.getLogger(LinuxMetricProvider.class);

  protected final int pid;

  public LinuxMetricProvider(int pid) {
    super(mappingFor(LinuxMetric.values()), LinuxMetric.class);
    Validate.isTrue(pid > 1, "invalid pid");
    this.pid = pid;
  }

  @Override
  protected void doCompute(LinuxMetric metric) {
    metric.compute(this);
  }

}
