package io.palyvos.scheduler.adapters;

import io.palyvos.scheduler.task.ExternalThread;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class SpeRuntimeInfo {

  private final String spe;
  private final List<Integer> pids;
  private final List<ExternalThread> threads;

  public SpeRuntimeInfo(List<Integer> pids, List<ExternalThread> threads, String spe) {
    Validate.notEmpty(pids, "no SPE PID given");
    Validate.notEmpty(threads, "no thread given");
    Validate.notBlank(spe, "blank SPE name");
    this.pids = Collections.unmodifiableList(pids);
    this.threads = Collections.unmodifiableList(threads);
    this.spe = spe;
  }

  public List<ExternalThread> threads() {
    return threads;
  }

  public Collection<Integer> pids() {
    return pids;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("spe", spe)
        .append("pids", pids)
        .append("threads", threads)
        .toString();
  }
}
