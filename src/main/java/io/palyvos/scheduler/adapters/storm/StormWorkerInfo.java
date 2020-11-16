package io.palyvos.scheduler.adapters.storm;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.builder.ToStringBuilder;

class StormWorkerInfo {

  public String topologyName;
  public String topologyId;
  public String host;
  public String supervisorId;
  public String uptime;
  public long uptimeSeconds;
  public long port;
  public String workerLogLink;
  public Map<String, Integer> componentNumTasks = new HashMap<>();
  public long executorsTotal;
  public double assignedMemOnHeap;
  public double assignedCpu;
  public double assignedMemOffHeap;

  @Override
  public String toString() {
    return new ToStringBuilder(this).append("topologyName", topologyName)
        .append("topologyId", topologyId).append("host", host).append("supervisorId", supervisorId)
        .append("uptime", uptime).append("uptimeSeconds", uptimeSeconds).append("port", port)
        .append("workerLogLink", workerLogLink).append("componentNumTasks", componentNumTasks)
        .append("executorsTotal", executorsTotal).append("assignedMemOnHeap", assignedMemOnHeap)
        .append("assignedCpu", assignedCpu).append("assignedMemOffHeap", assignedMemOffHeap)
        .toString();
  }
}
