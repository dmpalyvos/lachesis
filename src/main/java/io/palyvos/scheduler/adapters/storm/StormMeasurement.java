package io.palyvos.scheduler.adapters.storm;

import org.apache.commons.lang3.builder.ToStringBuilder;

class StormMeasurement {

  public String componentId;
  public String streamId;
  public String value;

  @Override
  public String toString() {
    return new ToStringBuilder(this).append("componentId", componentId).append("streamId", streamId)
        .append("value", value).toString();
  }
}
