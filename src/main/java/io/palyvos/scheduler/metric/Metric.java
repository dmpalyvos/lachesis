package io.palyvos.scheduler.metric;

import java.util.Collections;
import java.util.Set;

public interface Metric<T extends Metric<T>> {

  default boolean isConstant() {
    return false;
  }

  default Set<T> dependencies() {
    return Collections.emptySet();
  }

}
