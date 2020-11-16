package io.palyvos.scheduler.adapters.linux;

import io.palyvos.scheduler.metric.Metric;
import io.palyvos.scheduler.util.TopCpuUtilizationCommand;
import java.util.Map;

enum LinuxMetric implements Metric<LinuxMetric> {
  THREAD_CPU_UTILIZATION() {
    @Override
    protected void compute(LinuxMetricProvider provider) {
      Map<String, Double> cpuPerThread = new TopCpuUtilizationCommand(provider.pid)
          .cpuUtilizationPerThread();
      provider.replaceMetricValues(this, cpuPerThread);
    }
  };

  protected abstract void compute(LinuxMetricProvider provider);
}
