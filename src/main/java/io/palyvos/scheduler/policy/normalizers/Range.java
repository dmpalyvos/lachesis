package io.palyvos.scheduler.policy.normalizers;

import java.util.Collection;

/** Non-generic range class to prevent autoboxing */
class Range {

  final double min;
  final double max;
  final double realMin;
  final double realMax;

  static Range of(Collection<Double> values) {
    boolean first = true;
    double min = 0;
    double max = 0;
    for (double value : values) {
      if (first) {
        min = max = value;
        first = false;
        continue;
      }
      if (value > max) {
        max = value;
      }
      if (value < min) {
        min = value;
      }
    }
    return new Range(min, max);
  }

  public Range(double min, double max) {
    this.min = min;
    this.max = max;
    this.realMin = Math.min(min, max);
    this.realMax = Math.max(min, max);
  }

  public boolean contains(Range other) {
    return other.realMin >= this.realMin && other.realMax <= this.realMax;
  }
}
