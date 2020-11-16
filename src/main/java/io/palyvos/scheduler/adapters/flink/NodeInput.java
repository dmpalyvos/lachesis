package io.palyvos.scheduler.adapters.flink;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

class NodeInput {

  @SerializedName("num")
  @Expose
  public long num;

  @SerializedName("id")
  @Expose
  public String id;

  @SerializedName("ship_strategy")
  @Expose
  public String shipStrategy;

  @SerializedName("exchange")
  @Expose
  public String exchange;

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.NO_CLASS_NAME_STYLE)
        .append("num", num)
        .append("id", id)
        .append("shipStrategy", shipStrategy)
        .append("exchange", exchange)
        .toString();
  }

}
