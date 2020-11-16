package io.palyvos.scheduler.adapters;

import io.palyvos.scheduler.task.ExternalThread;
import java.util.Collection;

public interface OsAdapter {

  Collection<ExternalThread> jvmThreads(int pid);
}
