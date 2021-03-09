package io.palyvos.scheduler.adapters.flink;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import io.palyvos.scheduler.task.Subtask;
import io.palyvos.scheduler.task.Task;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

class FlinkVertex {

  @SerializedName("id")
  @Expose
  public String id;

  @SerializedName("name")
  @Expose
  public String name;

  @SerializedName("parallelism")
  @Expose
  public int parallelism;

  @SerializedName("status")
  @Expose
  public String status;

  @SerializedName("start-time")
  @Expose
  public long startTime;

  @SerializedName("end-time")
  @Expose
  public long endTime;

  @SerializedName("duration")
  @Expose
  public long duration;

  @SerializedName("metrics")
  @Expose
  public FlinkJobMetrics metrics;

  public Task toTask(String jid) {
    Task task = new Task(name, id, jid, FlinkAdapter.SPE_NAME);
    for (int i = 0; i < parallelism; i++) {
      final String subtaskId = String.format("%s.%d", id, i);
      task.subtasks().add(new Subtask(name, id, i));
    }
    return task;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.NO_CLASS_NAME_STYLE)
        .append("id", id)
        .append("name", name)
        .append("parallelism", parallelism)
        .append("status", status)
        .append("startTime", startTime)
        .append("endTime", endTime)
        .append("duration", duration)
        .append("metrics", metrics)
        .toString();
  }
}
