package io.palyvos.scheduler.adapters.linux;

import io.palyvos.scheduler.metric.Metric;
import io.palyvos.scheduler.util.command.TopCpuUtilizationCommand;
import java.util.HashMap;
import java.util.Map;

enum LinuxMetric implements Metric<LinuxMetric> {
  THREAD_CPU_UTILIZATION() {
    @Override
    protected void compute(LinuxMetricProvider provider) {
      Map<String, Double> cpuPerThread = new HashMap<>();
      for (int pid : provider.pids) {
        cpuPerThread.putAll(new TopCpuUtilizationCommand(pid)
            .cpuUtilizationPerThread());
        provider.replaceMetricValues(this, cpuPerThread);
      }
    }
  };

  protected abstract void compute(LinuxMetricProvider provider);
}
