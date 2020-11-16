package io.palyvos.scheduler.adapters.storm;

import io.palyvos.scheduler.task.Subtask;
import io.palyvos.scheduler.task.Task;
import org.apache.commons.lang3.builder.ToStringBuilder;

class StormBoltInfo {

  public long executors;
  public long emitted;
  public long transferred;
  public long acked;
  public String executeLatency;
  public long tasks;
  public long executed;
  public String processLatency;
  public String boltId;
  public String lastError;
  public int errorLapsedSecs;
  public String capacity;
  public long failed;

  public Task toTask(String topologyId) {
    Task task = new Task(boltId, boltId, topologyId);
    for (int i = 0; i < executors; i++) {
      task.subtasks().add(new Subtask(boltId, boltId, i));
    }
    return task;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this).append("executors", executors).append("emitted", emitted)
        .append("transferred", transferred).append("acked", acked)
        .append("executeLatency", executeLatency).append("tasks", tasks)
        .append("executed", executed).append("processLatency", processLatency)
        .append("boltId", boltId).append("lastError", lastError)
        .append("errorLapsedSecs", errorLapsedSecs).append("capacity", capacity)
        .append("failed", failed).toString();
  }
}
