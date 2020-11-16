package io.palyvos.scheduler.adapters.flink;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

class FlinkNode {

  @SerializedName("id")
  @Expose
  public String id;

  @SerializedName("parallelism")
  @Expose
  public long parallelism;

  @SerializedName("operator")
  @Expose
  public String operator;

  @SerializedName("operator_strategy")
  @Expose
  public String operatorStrategy;

  @SerializedName("description")
  @Expose
  public String description;

  @SerializedName("inputs")
  @Expose
  public List<NodeInput> inputs = new ArrayList<>();

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.NO_CLASS_NAME_STYLE)
        .append("id", id)
        .append("parallelism", parallelism)
        .append("operator", operator)
        .append("operatorStrategy", operatorStrategy)
        .append("description", description)
        .append("inputs", inputs)
        .toString();
  }
}
