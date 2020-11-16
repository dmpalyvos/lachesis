package io.palyvos.scheduler.adapters.flink;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

class FlinkTimestamps {

  @SerializedName("FAILED")
  @Expose
  public long failed;

  @SerializedName("CANCELED")
  @Expose
  public long canceled;

  @SerializedName("RUNNING")
  @Expose
  public long running;

  @SerializedName("RESTARTING")
  @Expose
  public long restarting;

  @SerializedName("FAILING")
  @Expose
  public long failing;

  @SerializedName("FINISHED")
  @Expose
  public long finished;

  @SerializedName("RECONCILING")
  @Expose
  public long reconciling;

  @SerializedName("CREATED")
  @Expose
  public long created;

  @SerializedName("CANCELLING")
  @Expose
  public long cancelling;

  @SerializedName("SUSPENDED")
  @Expose
  public long suspended;


  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.NO_CLASS_NAME_STYLE)
        .append("failed", failed)
        .append("canceled", canceled)
        .append("running", running)
        .append("restarting", restarting)
        .append("failing", failing)
        .append("finished", finished)
        .append("reconciling", reconciling)
        .append("created", created)
        .append("cancelling", cancelling)
        .append("suspended", suspended)
        .toString();
  }
}

