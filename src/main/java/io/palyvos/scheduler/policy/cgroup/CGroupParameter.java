package io.palyvos.scheduler.policy.cgroup;

import static io.palyvos.scheduler.policy.cgroup.CGroupController.CPU;
import static io.palyvos.scheduler.policy.cgroup.CGroupController.CPUSET;

import org.apache.commons.lang3.Validate;

public enum CGroupParameter {
  CPU_SHARES("cpu.shares", CPU) {
    @Override
    public CGroupParameterContainer of(Object value) {
      long valueAsLong = (long) value;
      Validate.isTrue(valueAsLong >= 2, "%s must be >= 2: %d", id, value);
      return new CGroupParameterContainer(id, value);
    }
  },
  CPU_CFS_PERIOD_US("cpu.cfs_period_us", CPU) {
    @Override
    public CGroupParameterContainer of(Object value) {
      long valueAsLong = (long) value;
      Validate.isTrue(valueAsLong >= 1000 && valueAsLong <= 1000000,
          "%s must be between 1E3 and 1E6: %d", id, value);
      return new CGroupParameterContainer(id, value);
    }
  },
  CPU_CFS_QUOTA_US("cpu.cfs_quota_us", CPU) {
    @Override
    public CGroupParameterContainer of(Object value) {
      return new CGroupParameterContainer(id, value);
    }
  },
  CPUSET_CPUS("cpuset.cpus", CPUSET) {
    @Override
    public CGroupParameterContainer of(Object value) {
      return new CGroupParameterContainer(id, value);
    }
  };


  protected final String id;
  protected final CGroupController controller;

  CGroupParameter(String id, CGroupController controller) {
    this.id = id;
    this.controller = controller;
  }

  public abstract CGroupParameterContainer of(Object value);
}
