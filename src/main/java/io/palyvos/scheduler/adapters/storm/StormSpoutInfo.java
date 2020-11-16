package io.palyvos.scheduler.adapters.storm;

import io.palyvos.scheduler.task.Subtask;
import io.palyvos.scheduler.task.Task;
import org.apache.commons.lang3.builder.ToStringBuilder;

class StormSpoutInfo {

  public long executors;
  public long emitted;
  public String completeLatency;
  public long transferred;
  public long acked;
  public String spoutId;
  public long tasks;
  public String lastError;
  public int errorLapsedSecs;
  public long failed;

  @Override
  public String toString() {
    return new ToStringBuilder(this).append("executors", executors).append("emitted", emitted)
        .append("completeLatency", completeLatency).append("transferred", transferred)
        .append("acked", acked).append("spoutId", spoutId).append("tasks", tasks)
        .append("lastError", lastError).append("errorLapsedSecs", errorLapsedSecs)
        .append("failed", failed).toString();
  }

  public Task toTask(String topologyId) {
    Task task = new Task(spoutId, spoutId, topologyId);
    for (int i = 0; i < executors; i++) {
      task.subtasks().add(new Subtask(spoutId, spoutId, i));
    }
    return task;
  }
}
