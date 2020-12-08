package io.palyvos.scheduler.task;

import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class CGroupParameter {

  private final String key;
  private final Object value;

  public CGroupParameter(String key, Object value) {
    Validate.notBlank(key, "blank key");
    Validate.notNull(value, "value");
    this.key = key;
    this.value = value;
  }

  public String key() {
    return key;
  }

  public Object value() {
    return value;
  }

  @Override
  public String toString() {
    return String.format("%s=%s", key, value);
  }
}
