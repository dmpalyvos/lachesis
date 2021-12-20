package io.palyvos.scheduler.metric;

import java.util.Collections;
import java.util.Set;

/**
 * Abstract representation of an external metric whose key is the implementation of this interface.
 *
 * @param <T> The type of the implementation, necessary to perform certain type checks in the
 *            scheduler internals.
 */
public interface Metric<T extends Metric<T>> {

  /**
   * @return {@code true} if the value of the metric never changes.
   */
  default boolean isConstant() {
    return false;
  }

  /**
   * @return The set of {@link Metric}s that are necessary to compute this metric.
   */
  default Set<T> dependencies() {
    return Collections.emptySet();
  }

}
