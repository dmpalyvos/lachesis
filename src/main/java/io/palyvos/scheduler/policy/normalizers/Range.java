package io.palyvos.scheduler.policy.normalizers;

/** Non-generic range class to prevent autoboxing */
class Range {

  final double min;
  final double max;

  public Range(double min, double max) {
    this.min = min;
    this.max = max;
  }
}
