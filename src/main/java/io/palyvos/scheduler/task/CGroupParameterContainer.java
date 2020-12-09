package io.palyvos.scheduler.task;

import org.apache.commons.lang3.Validate;

public class CGroupParameterContainer {

  private final String key;
  private final Object value;

  public CGroupParameterContainer(String key, Object value) {
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
