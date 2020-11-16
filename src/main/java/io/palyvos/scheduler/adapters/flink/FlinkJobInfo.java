package io.palyvos.scheduler.adapters.flink;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

class FlinkJobInfo {

  @SerializedName("jid")
  @Expose
  public String jid;

  @SerializedName("name")
  @Expose
  public String name;

  @SerializedName("isStoppable")
  @Expose
  public boolean isStoppable;

  @SerializedName("state")
  @Expose
  public String state;

  @SerializedName("start-time")
  @Expose
  public long startTime;

  @SerializedName("end-time")
  @Expose
  public long endTime;

  @SerializedName("duration")
  @Expose
  public long duration;

  @SerializedName("now")
  @Expose
  public long now;

  @SerializedName("timestamps")
  @Expose
  public FlinkTimestamps timestamps;

  @SerializedName("vertices")
  @Expose
  public List<FlinkVertex> vertices = new ArrayList<>();

  @SerializedName("plan")
  @Expose
  public FlinkPlan plan;

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.NO_CLASS_NAME_STYLE)
        .append("jid", jid)
        .append("name", name)
        .append("isStoppable", isStoppable)
        .append("state", state)
        .append("startTime", startTime)
        .append("endTime", endTime)
        .append("duration", duration)
        .append("now", now)
        .append("timestamps", timestamps)
        .append("vertices", vertices)
        .append("plan", plan)
        .toString();
  }
}
