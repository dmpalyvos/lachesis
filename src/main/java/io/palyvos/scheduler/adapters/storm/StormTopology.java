package io.palyvos.scheduler.adapters.storm;

import org.apache.commons.lang3.builder.ToStringBuilder;

class StormTopology {

  public String id;
  public String name;
  public String status;
  public String uptime;
  public Long uptimeSeconds;
  public Long tasksTotal;
  public Long workersTotal;
  public Long executorsTotal;
  public Long replicationCount;
  public Long requestedMemOnHeap;
  public Long requestedMemOffHeap;
  public Long requestedTotalMem;
  public Long requestedCpu;
  public Long assignedMemOnHeap;
  public Long assignedMemOffHeap;
  public Long assignedTotalMem;
  public Long assignedCpu;

  @Override
  public String toString() {
    return new ToStringBuilder(this).append("id", id).append("name", name).append("status", status)
        .append("uptime", uptime).append("uptimeSeconds", uptimeSeconds)
        .append("tasksTotal", tasksTotal).append("workersTotal", workersTotal)
        .append("executorsTotal", executorsTotal).append("replicationCount", replicationCount)
        .append("requestedMemOnHeap", requestedMemOnHeap)
        .append("requestedMemOffHeap", requestedMemOffHeap)
        .append("requestedTotalMem", requestedTotalMem).append("requestedCpu", requestedCpu)
        .append("assignedMemOnHeap", assignedMemOnHeap)
        .append("assignedMemOffHeap", assignedMemOffHeap)
        .append("assignedTotalMem", assignedTotalMem).append("assignedCpu", assignedCpu)
        .toString();
  }
}
